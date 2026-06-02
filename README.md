# Infrastructure & Deployment Guidelines
## S3 Tables × Spark on EMR Serverless

---

*Representative diagram used during the production-level POC.*

![Architecture Diagram](S3%20Tables%20-%20Poc%20-%20Architecture%20Diagram.png)

---

## Table of Contents

1. [S3 Tables Bucket Setup](#1-s3-tables-bucket-setup)
2. [Ingestion Infrastructure](#2-ingestion-infrastructure)
3. [Important Factors for Ingestion Efficiency](#3-important-factors-for-ingestion-efficiency)
4. [Querying via Spark on EMR Serverless](#4-querying-via-spark-on-emr-serverless)

---

## 1. S3 Tables Bucket Setup

**Complete guide:** https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html

### Critical Steps

1. **Table Bucket Creation**
2. **Tagging** for cost & billing purposes
3. **Integration with AWS services** — for access via AWS Athena, Glue, etc. — and Lake Formation (if required, for data access controls)
   - Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integration-overview.html

### Iceberg Table Design — Recommended Configuration

The following partition scheme, sort order, and file settings were validated at 120B variants and should be applied at table creation time.

**Partition spec**

```
identity(contig) + truncate(pos_start, 5_000_000) + bucket(sample_name, 16)
```

This yields a bounded maximum of ~10,000 partitions regardless of data volume growth (the genomic coordinate space is fixed), while balancing targeted query pruning against large-scan efficiency.

**Bloom filters** — add on columns used as equality predicates in selective queries:

```
ref, alt, vep_symbol, vep_gene
```

**Target file size:** 256 MB (set via Iceberg table property `write.target-file-size-bytes`).

> These settings were the single largest driver of query performance improvement. Changing partition granularity from an over-fine initial spec to this scheme produced a 20× reduction in file count and dramatically cut Iceberg commit overhead during ingestion.

---

## 2. Ingestion Infrastructure

For our POC implementation purposes, an internal Pipelines Orchestration system within Elucidata's infrastructure was used to create, manage, execute, and monitor Python-based pipelines at scale. The system used **ECS Fargate** for compute.

### Instance Configuration

| Parameter | Value |
|---|---|
| vCPUs | 16 |
| RAM | 120 GB |
| Concurrent Instances | 17 |
| PyIceberg Workers per Instance | 40 |
| Variant Rows per Batch (approx.) | 50.4 million |

### Pipeline Architecture — 3-Stage Toolchain

Each ingestion run executes the following stages:

**Stage 1 — Read & Parse**

VCF files in S3 are read in parallel via [`cyvcf2`](https://github.com/brentp/cyvcf2) inside a `ProcessPoolExecutor`. Each worker processes a batch of ~50.4M variants. Missing fields are filled with `NULL` or synthetic placeholders to conform to the unified schema.

**Stage 2 — Transform (Schema Normalisation)**

Records are mapped to the 24-field unified schema and converted to **PyArrow** tables. Fields are type-cast to their Iceberg target types. Data is split into two streams at this stage:
- Sample-level variant data → `variants` table
- Locus-level annotation data → `annotations` table

**Stage 3 — Write via PyIceberg**

PyArrow tables are committed via **PyIceberg** (40 workers/instance, 17 instances in parallel). On S3 commit conflicts, PyIceberg's private functions are used to decouple the data-file push from the snapshot commit — only the lightweight commit step is retried, not the full S3 data upload. AWS auto-compaction runs post-ingestion.

| Table | Contents | Partitioning |
|---|---|---|
| `variants` | One row per sample × locus. All format fields + inline VEP + gnomAD. | `identity(contig)` + `truncate(pos_start, 5M)` + `bucket(sample, 16)`. Bloom filters on `ref`, `alt`, `vep_gene`, `vep_symbol`. File size 256 MB. |
| `annotations` | One row per locus × annotation source. | `identity(chrom)` + `truncate(pos, 5M)` |
| `annotations_dedup` | One row per unique `(chrom, pos, ref, alt)` — the join target. | Same as annotations. Created via Athena CTAS with `ROW_NUMBER()` deduplication. |

---

## 3. Important Factors for Ingestion Efficiency

### 3.1 Instance Configuration & OOM Risk

Instance configuration must be optimised relative to actual CPU and memory usage, which depends on the volume of data ingested per batch. If batch size exceeds allocated resources, it will result in Out-of-Memory (OOM) errors.

### 3.2 Max Instance Concurrency & S3 PUT Limits

Max instance concurrency must not exceed a safe threshold (17 in this case). Beyond this limit, the workload will hit **S3 PUT rate limits**, causing errors. The effective concurrency ceiling is:

```
Max Concurrency = PyIceberg Workers per Instance × Concurrently Running Instances
```

### 3.3 Partitioning Scheme Impact

The partitioning scheme significantly impacts ingestion time. A higher number of partitions increases the time required to split data and results in a greater number of smaller S3 PUT requests.

### 3.4 S3 Commit Conflicts

S3 Commit Conflicts are an inherent characteristic of Iceberg's commit-based write model. Auto-compaction, ingestion, and data updates can occur concurrently. **All write operations must explicitly handle this error.**

### 3.5 PyIceberg: Decoupling Data Upload from Commit

When using PyIceberg, leverage its internal private functions to decouple the **data-file push** from the **snapshot commit**. Only the lightweight commit step should be retried on conflict — not the full data upload. Retrying the full upload on every conflict significantly increases both duration and cost.

### 3.6 Annotations Table Deduplication

For the POC, annotations were extracted directly from VCF variant data, which resulted in duplicate rows in the annotations table. An **AWS Glue script** was used to deduplicate the table and create a new `annotations_dedup` table via CTAS.

> **Recommended approach going forward:** Ingest only new, unique annotation rows directly into the annotations table to avoid duplicates at source and eliminate the need for a deduplication step.

### 3.7 S3 Tables Auto-Compaction Failures

Auto-compaction may report failures in the maintenance status post-ingestion. This is expected behaviour — compaction succeeds automatically after a small number of retries and does not require manual intervention.

---

## 4. Querying via Spark on EMR Serverless

### 4.1 Spark Version

| Component | Version |
|---|---|
| Apache Spark | 3.5.6 |
| EMR Serverless | 7.13.0 |

### 4.2 Setup Overview

1. Create an **EMR Serverless Application**
2. Create an **EMR Studio** and associated **Workspace (Notebook)** — provides a Jupyter-like interface for interacting with Iceberg data and monitoring via the integrated Spark Web UI
3. **Complete guide:** https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/applications.html

### 4.3 Serverless Application Configuration

**Application Limits (soft)**

| Resource | Limit |
|---|---|
| vCPUs | 400 |
| Memory | 3,000 GB |
| Disk | 20,000 GB |

> Interactive endpoints for EMR Studio and Livvy must be enabled.

**Pre-initialised Capacity**

| Component | Count | Compute |
|---|---|---|
| Spark Drivers | 1 | 4 vCPUs, 16 GB memory |
| Spark Executors | 5 | 4 vCPUs, 16 GB memory |

### 4.4 EMR Studio Notebook Configuration (PySpark Kernel)

```python
%%configure -f
{
    "conf": {
        # ── Iceberg Integration ──────────────────────────────────────────────
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "s3tables",
        "spark.sql.catalog.s3tables": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.s3tables.glue.id": "<aws_account_id>:s3tablescatalog/<s3_table_bucket_name>",
        "spark.sql.catalog.s3tables.warehouse": "s3://<s3_table_bucket_name>/warehouse/",
        "spark.sql.catalog.s3tables.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

        # ── S3 Connection Tuning ─────────────────────────────────────────────
        "spark.sql.catalog.s3tables.s3.connection-maximum": "2500",
        "spark.hadoop.fs.s3.maxConnections": "2500",
        "spark.hadoop.fs.s3a.connection.maximum": "2500",
        "spark.hadoop.fs.s3a.threads.max": "256",
        "spark.sql.catalog.s3tables.http-client.apache.max-connections": "2500",

        # ── Spark Idle Timeouts ──────────────────────────────────────────────
        "spark.dynamicAllocation.executorIdleTimeout": "60",
        "spark.dynamicAllocation.cachedExecutorIdleTimeout": "60",
        "spark.python.worker.killOnIdleTimeout": "true",
        "spark.python.worker.idleTimeoutSeconds": "60",
        "spark.sql.execution.pyspark.udf.idleTimeoutSeconds": "60",
        "spark.sql.execution.pyspark.udf.killOnIdleTimeout": "60",
        "spark.dynamicAllocation.shuffleTracking.timeout": "600"
    }
}
```

> **Note:** Idle timeout values should be tuned iteratively based on actual usage patterns to optimise for both performance and cost efficiency.

### 4.5 Join Strategy & Broadcast Hints

Broadcast hints significantly influence the Spark query planner's join strategy. For benchmarking join queries, the `BROADCAST` hint was applied to force the planner to use broadcast strategy with the smaller table (`annotations`):

```sql
WITH base AS (
    SELECT chrom, pos, ref, alt, symbol
    FROM annotations_dedup
    WHERE impact = 'LOW'
      AND consequence LIKE '%missense%'
)
SELECT  /*+ BROADCAST(a) */
    a.symbol      AS gene_symbol,
    COUNT(*)      AS num_non_silent
FROM variants v
JOIN base a
    ON  v.contig     = a.chrom
    AND v.pos_start  = a.pos
    AND v.ref        = a.ref
    AND v.alt        = a.alt
WHERE v.filters = 'PASS'
GROUP BY a.symbol;
```

> **Fallback:** If the filtered annotations dataset grows too large to fit in a single worker's memory, switch to a **PARTITIONED join strategy**, which processes data partition-by-partition rather than loading the entire filtered table into memory.

---

