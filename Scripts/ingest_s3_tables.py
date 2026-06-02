#!/usr/bin/env python3
"""
Ingest VCF files into Iceberg tables on S3 Tables via pyiceberg + pyarrow.

Reads VCFs using cyvcf2 in parallel (ProcessPoolExecutor), builds PyArrow tables,
and writes batches to 3 Iceberg tables: variants, annotations, variants_by_sample.

Usage:
  python ingest.py \
    --paths-file /work/s3-tables/vcf_paths.txt \
    --workers 7 \
    --batch-size 500

  python ingest.py \
    --paths-file "benchmark_data:::0-9999" \
    --workers 16 \
    --batch-size 4000 \
    --tables variants annotations variants_by_sample

  # Write to specific tables only
  python ingest.py \
    --paths-file /work/s3-tables/tmp/test_paths.txt \
    --workers 7 \
    --batch-size 100 \
    --tables variants annotations variants_by_sample

Post-ingestion: deduplicate annotations table via Athena CTAS:

  CREATE TABLE variants.annotations_deduped
  WITH (
      table_type = 'ICEBERG',
      format = 'PARQUET',
      partitioning = ARRAY['chrom', 'truncate(pos, 1000000)']
  ) AS
  SELECT chrom, pos, ref, alt,
         gene, symbol, consequence, impact,
         gnomad_af, gnomad_af_popmax, pathogenecity
  FROM (
      SELECT *, ROW_NUMBER() OVER (
          PARTITION BY chrom, pos, ref, alt ORDER BY gene
      ) AS rn
      FROM variants.annotations
  )
  WHERE rn = 1

  -- Then drop old table and rename, or use annotations_deduped directly.
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import tarfile
import time
import uuid
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

import boto3
import cyvcf2
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io.pyarrow import _dataframe_to_data_files

# --- Schema: maps VCF INFO/FORMAT fields to column names ---

ANNOTATION_INFO_FIELDS = {
    "GENE": "vep_gene",
    "SYMBOL": "vep_symbol",
    "CONS": "vep_consequence",
    "IMPACT": "vep_impact",
    "GNOMAD_AF": "gnomad_af",
    "GNOMAD_AF_POPMAX": "gnomad_af_popmax",
    "PATHOGENECITY": "pathogenecity",
    "DATA_SOURCE": "data_source",
    "ORIGIN": "somalone_predicted_origin",
    "DATA_TYPE": "data_type",
}

ANNOTATION_INFO_FIELDS_SEP = {
    k: v
    for k, v in ANNOTATION_INFO_FIELDS.items()
    if (
        k != "ORIGIN"  # varies per sample, so don't put in separate variant-level table
        and k != "DATA_SOURCE"  # also varies per sample and often missing, so skip for now
        and k != "DATA_TYPE"  # also varies per sample and often missing, so skip for now
    )
}

FORMAT_FIELDS = {
    "GT": "genotype",
    "DP": "read_depth",
    "AD": "allele_depth",
    "GQ": "genotype_quality",
    "VAF": "variant_allele_frequency",
    "SOM_SCORE": "somalone_somatic_score",
}

# PyArrow schema for the variants tables
VARIANTS_ARROW_SCHEMA = pa.schema(
    [
        ("sample_name", pa.string()),
        ("contig", pa.string()),
        ("pos_start", pa.int32()),
        ("pos_end", pa.int32()),
        ("ref", pa.string()),
        ("alt", pa.string()),
        ("qual", pa.float64()),
        ("filters", pa.string()),
        ("genotype", pa.string()),
        ("read_depth", pa.int32()),
        ("allele_depth", pa.list_(pa.field("element", pa.int32(), nullable=False))),
        ("genotype_quality", pa.int32()),
        ("variant_allele_frequency", pa.float32()),
        ("somalone_somatic_score", pa.float32()),
        ("vep_gene", pa.string()),
        ("vep_symbol", pa.string()),
        ("vep_consequence", pa.string()),
        ("vep_impact", pa.string()),
        ("gnomad_af", pa.float32()),
        ("gnomad_af_popmax", pa.float32()),
        ("pathogenecity", pa.string()),
        ("data_source", pa.string()),
        ("somalone_predicted_origin", pa.string()),
        ("data_type", pa.string()),
    ]
)

ANNOTATIONS_ARROW_SCHEMA = pa.schema(
    [
        ("chrom", pa.string()),
        ("pos", pa.int32()),
        ("ref", pa.string()),
        ("alt", pa.string()),
        ("gene", pa.string()),
        ("symbol", pa.string()),
        ("consequence", pa.string()),
        ("impact", pa.string()),
        ("gnomad_af", pa.float32()),
        ("gnomad_af_popmax", pa.float32()),
        ("pathogenecity", pa.string()),
    ]
)

# Column names for the columnar parse output (must match VARIANTS_ARROW_SCHEMA order)
_VARIANT_COLUMNS = [field.name for field in VARIANTS_ARROW_SCHEMA]

TPM_ARROW_SCHEMA = pa.schema(
    [
        ("sample_id", pa.string()),
        ("gene_id", pa.string()),
        ("gene_symbol", pa.string()),
        ("expression", pa.float32()),
        ("tissue", pa.string()),
        ("dataset", pa.string()),
        ("dataset_version", pa.string()),
        ("genome_build", pa.string()),
        ("annotation", pa.string()),
        ("quant_method", pa.string()),
        ("unit", pa.string()),
    ]
)

_TPM_COLUMNS = [field.name for field in TPM_ARROW_SCHEMA]

CNV_INFO_FIELDS = {
    "IMPORTJOBID": "importjobid",
    "TECH": "technology",
    "PIPELINE": "pipeline",
    "PIPELINE_VERSION": "pipeline_version",
    "OUT_PATH": "out_path",
    "RECORD_TYPE": "record_type",
    "GENE": "gene",
}

# CN, LOG2, BAF, COPY, CALL are per-sample FORMAT fields, not INFO
CNV_FORMAT_FIELDS = {
    "CN": "cn",
    "LOG2": "log2",
    "BAF": "baf",
    "COPY": "copy",
    "CALL": "call",
}

CNV_ARROW_SCHEMA = pa.schema(
    [
        ("importjobid", pa.string()),
        ("sampleid", pa.string()),
        ("technology", pa.string()),
        ("pipeline", pa.string()),
        ("pipeline_version", pa.string()),
        ("out_path", pa.string()),
        ("record_type", pa.string()),
        ("contigname", pa.string()),
        ("start", pa.int32()),
        ("end", pa.int32()),
        ("gene", pa.string()),
        ("log2", pa.float32()),
        ("baf", pa.float32()),
        ("cn", pa.float32()),
        ("copy", pa.int32()),
        ("call", pa.string()),
    ]
)

_CNV_COLUMNS = [field.name for field in CNV_ARROW_SCHEMA]


# --- Logging ---


def setup_logging(verbose=False):
    log = logging.getLogger("iceberg_ingest")
    log.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    log.addHandler(ch)
    try:
        Path("logs").mkdir(exist_ok=True)
        fh = logging.FileHandler("logs/iceberg_ingest.log")
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(fh)
    except Exception:
        pass
    return log


# --- AWS client ---

_s3_client = None


def init_s3_client(aws_creds=None):
    """Initialize the module-level S3 client. Call once from main()."""
    global _s3_client
    if aws_creds:
        key_id, secret, region = aws_creds
        session = boto3.Session(
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            region_name=region,
        )
        _s3_client = session.client("s3")
    else:
        _s3_client = boto3.client("s3")


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


# --- VCF parsing (runs in worker processes) ---


def download_file_from_s3(path):
    """Download file path from S3 to local cache. Returns local path."""
    if path.startswith("s3://"):
        local = "tmp/download_cache/" + os.path.basename(path)
        if not os.path.exists(local):
            os.makedirs(os.path.dirname(local), exist_ok=True)
            bucket = path[5:].split("/", 1)[0]
            key = path[5:].split("/", 1)[1]
            get_s3_client().download_file(bucket, key, local)
        return local
    return path


def extract_archive(s3_path):
    """Extract .vcf.gz and .tbi files from a .zip or .tar.gz archive. Returns list of .vcf.gz paths only."""
    local_file = "tmp/download_cache/" + os.path.basename(s3_path)
    extract_dir = os.path.dirname(local_file)
    extracted = []

    # .csv.gz is a single gzipped file, not an archive — pass through as-is.
    if local_file.endswith(".csv.gz"):
        return [{"path": local_file}]

    if local_file.endswith(".tar.gz") or local_file.endswith(".tgz"):
        with tarfile.open(local_file, "r:gz") as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                extracted_path = os.path.join(extract_dir, member.name)
                if member.name.endswith(".vcf.gz") or member.name.endswith(".vcf.gz.tbi"):
                    if not os.path.exists(extracted_path):
                        tf.extract(member, extract_dir)
                    if member.name.endswith(".vcf.gz") and not member.name.endswith(".vcf.gz.tbi"):
                        extracted.append({"path": extracted_path})
    else:
        with zipfile.ZipFile(local_file, "r") as zf:
            for member in zf.namelist():
                extracted_path = os.path.join(extract_dir, member)
                if member.endswith(".vcf.gz") or member.endswith(".vcf.gz.tbi"):
                    if not os.path.exists(extracted_path):
                        zf.extract(member, extract_dir)
                    if member.endswith(".vcf.gz") and not member.endswith(".vcf.gz.tbi"):
                        extracted.append({"path": extracted_path})

    # Delete zip/tar file
    if os.path.exists(local_file):
        os.remove(local_file)

    return extracted


def parse_vcf(vcf_path):
    """Parse a single VCF file into columnar dict of lists (one list per column)."""
    local_path = download_file_from_s3(vcf_path)
    vcf = cyvcf2.VCF(local_path)
    sample_name = (
        vcf.samples[0] if vcf.samples else os.path.basename(vcf_path).replace(".vcf.gz", "")
    )

    # Pre-allocate columnar lists
    cols = {name: [] for name in _VARIANT_COLUMNS}

    for v in vcf:
        alt = v.ALT[0] if v.ALT else "."

        # Genotype: reconstruct GT string from header indices (e.g. "0/1", "0|1")
        try:
            gt = v.genotypes[0]
            sep = "|" if gt[-1] else "/"
            gt_str = sep.join("." if a < 0 else str(a) for a in gt[:-1])
        except Exception:
            gt_str = "./."

        try:
            dp = int(v.format("DP")[0][0]) if v.format("DP") is not None else None
        except Exception:
            dp = None

        try:
            ad_raw = v.format("AD")
            ad = ad_raw[0].tolist() if ad_raw is not None else None
        except Exception:
            ad = None

        try:
            gq = int(v.format("GQ")[0][0]) if v.format("GQ") is not None else None
        except Exception:
            gq = None

        try:
            vaf = float(v.format("VAF")[0][0]) if v.format("VAF") is not None else None
        except Exception:
            vaf = None

        try:
            som = float(v.format("SOM_SCORE")[0][0]) if v.format("SOM_SCORE") is not None else None
        except Exception:
            som = None

        cols["sample_name"].append(sample_name)
        cols["contig"].append(v.CHROM)
        cols["pos_start"].append(v.POS)
        cols["pos_end"].append(v.end)
        cols["ref"].append(v.REF)
        cols["alt"].append(alt)
        cols["qual"].append(v.QUAL)
        cols["filters"].append(";".join(v.FILTERS) if v.FILTERS else None)
        cols["genotype"].append(gt_str)
        cols["read_depth"].append(dp)
        cols["allele_depth"].append(ad)
        cols["genotype_quality"].append(gq)
        cols["variant_allele_frequency"].append(vaf)
        cols["somalone_somatic_score"].append(som)

        for info_field, col_name in ANNOTATION_INFO_FIELDS.items():
            cols[col_name].append(v.INFO.get(info_field, None))

    vcf.close()

    # Delete vcf file after reading
    if os.path.exists(local_path):
        os.remove(local_path)

    return cols


def stream_tpm_csv(csv_path, rows_per_chunk):
    """Stream a (possibly huge) TPM .csv.gz file as PyArrow tables of ~rows_per_chunk rows.

    Uses pyarrow's streaming CSV reader so memory usage stays bounded regardless of file size.
    Yields tables matching TPM_ARROW_SCHEMA. Deletes the local file when done.
    """
    local_path = download_file_from_s3(csv_path)
    read_options = pacsv.ReadOptions(
        use_threads=False,
        block_size=64 * 1024 * 1024,  # 64 MiB input blocks -> each yielded RecordBatch
    )
    convert_options = pacsv.ConvertOptions(
        column_types={f.name: f.type for f in TPM_ARROW_SCHEMA},
        include_columns=_TPM_COLUMNS,
        strings_can_be_null=True,
    )

    reader = pacsv.open_csv(local_path, read_options=read_options, convert_options=convert_options)
    try:
        buffer: list = []
        buffer_rows = 0
        for batch in reader:
            buffer.append(batch)
            buffer_rows += batch.num_rows
            if buffer_rows >= rows_per_chunk:
                yield pa.Table.from_batches(buffer, schema=TPM_ARROW_SCHEMA)
                buffer = []
                buffer_rows = 0
        if buffer:
            yield pa.Table.from_batches(buffer, schema=TPM_ARROW_SCHEMA)
    finally:
        reader.close()
        if os.path.exists(local_path):
            os.remove(local_path)


def merge_columnar(col_dicts):
    """Merge list of columnar dicts into a single columnar dict by extending lists."""
    merged = {name: [] for name in _VARIANT_COLUMNS}
    for d in col_dicts:
        for name in _VARIANT_COLUMNS:
            merged[name].extend(d[name])
    return merged


def columnar_to_variants_arrow(cols):
    """Convert columnar dict of lists to a PyArrow table matching VARIANTS_ARROW_SCHEMA."""
    arrays = []
    for field in VARIANTS_ARROW_SCHEMA:
        arrays.append(pa.array(cols[field.name], type=field.type))
    return pa.table(arrays, schema=VARIANTS_ARROW_SCHEMA)


def variants_to_annotations_arrow(variants_table):
    """Extract deduplicated annotations from a variants PyArrow table. Stays in Arrow."""
    annot = pa.table(
        {
            "chrom": variants_table.column("contig"),
            "pos": variants_table.column("pos_start"),
            "ref": variants_table.column("ref"),
            "alt": variants_table.column("alt"),
            "gene": variants_table.column("vep_gene"),
            "symbol": variants_table.column("vep_symbol"),
            "consequence": variants_table.column("vep_consequence"),
            "impact": variants_table.column("vep_impact"),
            "gnomad_af": variants_table.column("gnomad_af"),
            "gnomad_af_popmax": variants_table.column("gnomad_af_popmax"),
            "pathogenecity": variants_table.column("pathogenecity"),
        },
        schema=ANNOTATIONS_ARROW_SCHEMA,
    )

    # Deduplicate by (chrom, pos, ref, alt) — build composite key, keep first occurrence
    key_col = pc.binary_join_element_wise(
        pc.cast(annot.column("chrom"), pa.string()),
        pc.cast(annot.column("pos"), pa.string()),
        pc.cast(annot.column("ref"), pa.string()),
        pc.cast(annot.column("alt"), pa.string()),
        ":",
    )
    # dictionary_encode maps each unique key to an integer index;
    # indices.dictionary gives first-seen order, so we pick first occurrence per key
    encoded = pc.dictionary_encode(key_col).combine_chunks()
    seen = set()
    keep = []
    for i, idx in enumerate(encoded.indices):
        val = idx.as_py()
        if val not in seen:
            seen.add(val)
            keep.append(i)
    return annot.take(keep)


def parse_cnv_vcf(vcf_path):
    """Parse a single CNV VCF file into a columnar dict.

    Metadata fields (importjobid, technology, pipeline, etc.) come from INFO.
    CNV values (cn, log2, baf, copy, call) come from per-sample FORMAT fields.
    sampleid is taken from the VCF sample name.
    """
    local_path = download_file_from_s3(vcf_path)
    vcf = cyvcf2.VCF(local_path)
    sample_id = vcf.samples[0] if vcf.samples else None

    cols = {name: [] for name in _CNV_COLUMNS}

    for v in vcf:
        cols["sampleid"].append(sample_id)
        cols["contigname"].append(v.CHROM)
        cols["start"].append(v.POS)
        cols["end"].append(v.end)

        for info_key, col_name in CNV_INFO_FIELDS.items():
            cols[col_name].append(v.INFO.get(info_key, None))

        try:
            cols["cn"].append(float(v.format("CN")[0][0]) if v.format("CN") is not None else None)
        except Exception:
            cols["cn"].append(None)

        try:
            cols["log2"].append(
                float(v.format("LOG2")[0][0]) if v.format("LOG2") is not None else None
            )
        except Exception:
            cols["log2"].append(None)

        try:
            cols["baf"].append(
                float(v.format("BAF")[0][0]) if v.format("BAF") is not None else None
            )
        except Exception:
            cols["baf"].append(None)

        try:
            copy_raw = v.format("COPY")
            cols["copy"].append(int(copy_raw[0][0]) if copy_raw is not None else None)
        except Exception:
            cols["copy"].append(None)

        try:
            call_raw = v.format("CALL")
            val = call_raw[0] if call_raw is not None else None
            if isinstance(val, bytes):
                val = val.decode()
            cols["call"].append(val)
        except Exception:
            cols["call"].append(None)

    vcf.close()

    if os.path.exists(local_path):
        os.remove(local_path)

    return cols


def _merge_cnv_columnar(col_dicts):
    merged = {name: [] for name in _CNV_COLUMNS}
    for d in col_dicts:
        for name in _CNV_COLUMNS:
            merged[name].extend(d[name])
    return merged


def _columnar_to_cnv_arrow(cols):
    arrays = [pa.array(cols[field.name], type=field.type) for field in CNV_ARROW_SCHEMA]
    return pa.table(arrays, schema=CNV_ARROW_SCHEMA)


# --- Paths file handling ---


def read_paths_file(path):
    with open(path, "r") as f:
        lines = [json.loads(ln.strip()) for ln in f if ln.strip()]
    out = []
    for p in lines:
        if not p["path"].startswith("s3://"):
            p["path"] = str(Path(p["path"]).expanduser().resolve())
        out.append(p)
    return out


def download_benchmark_paths_file(uri=""): # uri = BENCHMARK_PATHS_FILE_S3_URI containing vcf paths on s3
    local = "benchmark_vcf_paths.txt"
    if os.path.exists(local):
        print("Using existing 'benchmark_vcf_paths.txt'..")
    else:
        get_s3_client().download_file(uri[5:].split("/", 1)[0], uri[5:].split("/", 1)[1], local)
    return local


# --- Main ingestion ---


def _write_arrow_to_iceberg(tbl, arrow_data, tbl_name, max_retries=100):
    """Write an Arrow table to an Iceberg table with retry on commit conflicts."""
    t1 = time.perf_counter()

    # Step 1: Write data files to S3 (slow, done once)
    data_files_max_retries = 10
    for attempt in range(data_files_max_retries):
        try:
            data_files = list(
                _dataframe_to_data_files(
                    table_metadata=tbl.metadata,
                    write_uuid=uuid.uuid4(),
                    df=arrow_data,
                    io=tbl.io,
                )
            )
            break
        except Exception as e:
            if attempt == data_files_max_retries - 1:
                raise
            print(f"WARNING: Failed to write data files for {tbl_name}, attempt {attempt + 1}: {e}")
            # time.sleep(min(2**attempt, 60) + random.uniform(0, 5))
            time.sleep(random.randint(2, 30))
    print(f"{tbl_name}: data files uploaded in {time.perf_counter() - t1:.1f}s")

    # Step 2: Commit metadata (fast, retry on conflict)
    for attempt in range(max_retries):
        try:
            with tbl.transaction() as tx:
                with tx.update_snapshot().fast_append() as update_snapshot:
                    for data_file in data_files:
                        update_snapshot.append_data_file(data_file)
            return time.perf_counter() - t1
        except CommitFailedException as e:
            print(
                f"WARNING: {tbl_name}: commit conflict (attempt {attempt + 1}/{max_retries}): {e}"
            )
            tbl.refresh()
            time.sleep(random.randint(2, 30))
    raise Exception(f"Exceeded {max_retries} retries committing to {tbl_name}")


def _ingest_tpm(csv_paths, catalog, rows_per_chunk, resume_chunk_no, log):
    """Stream each TPM .csv.gz file in chunks and write each chunk to variants.tpm.

    Memory stays bounded at ~rows_per_chunk rows regardless of file size.
    Files are processed sequentially; within each file, chunks are read and written serially.
    `resume_chunk_no` is a 1-based global chunk index to skip chunks before (for resuming).
    """
    tpm_table = catalog.load_table("variants.tpm")
    log.info(
        f"Writing to table: variants.tpm ({len(csv_paths)} CSV files, "
        f"~{rows_per_chunk:,} rows per write chunk)"
    )

    total_rows = 0
    global_chunk_idx = 0
    t_start = time.perf_counter()

    for file_idx, csv_path in enumerate(csv_paths):
        file_label = f"File {file_idx + 1}/{len(csv_paths)} ({csv_path})"
        t_file = time.perf_counter()
        file_rows = 0

        try:
            for chunk in stream_tpm_csv(csv_path, rows_per_chunk):
                global_chunk_idx += 1
                if global_chunk_idx < resume_chunk_no:
                    log.info(f"{file_label}: skipping chunk {global_chunk_idx} (resume)")
                    continue

                chunk_label = f"{file_label} chunk {global_chunk_idx}"
                log.info(f"{chunk_label}: {chunk.num_rows:,} rows parsed, writing...")
                elapsed = _write_arrow_to_iceberg(tpm_table, chunk, "tpm")
                log.info(f"{chunk_label}: wrote in {elapsed:.1f}s")

                file_rows += chunk.num_rows
                total_rows += chunk.num_rows
                del chunk
        except Exception:
            log.exception(
                f"{file_label} FAILED at global chunk {global_chunk_idx}. "
                f"Total rows written before failure: {total_rows}"
            )
            sys.exit(1)

        elapsed_file = time.perf_counter() - t_file
        log.info(
            f"{file_label} done in {elapsed_file:.1f}s | "
            f"File rows: {file_rows:,} | Total rows: {total_rows:,}"
        )

    elapsed = time.perf_counter() - t_start
    log.info(
        f"TPM ingestion complete: {len(csv_paths)} files, {total_rows:,} rows in {elapsed:.1f}s"
    )


def _ingest_cnv(sample_batches, catalog, batch_size, workers, resume_batch_no, log):
    """Download, extract, parse, and write CNV VCF files into variants.cnv.

    All fields (importjobid, sampleid, technology, etc.) are read directly from each VCF —
    INFO fields for metadata, FORMAT fields for CNV values, and vcf.samples[0] for sampleid.
    """
    cnv_table = catalog.load_table("variants.cnv")

    s3_paths = [s["path"] for s in sample_batches]

    log.info("CNV: downloading archives from S3...")
    with ThreadPoolExecutor(max_workers=32) as ex:
        list(ex.map(download_file_from_s3, s3_paths))

    log.info("CNV: extracting archives...")
    with ThreadPoolExecutor(max_workers=32) as ex:
        results = list(ex.map(extract_archive, s3_paths))

    vcf_paths = [vcf["path"] for batch in results for vcf in batch]
    vcf_paths.sort()

    log.info(
        f"Writing to table: variants.cnv ({len(vcf_paths)} CNV VCF files, "
        f"batch size {batch_size})"
    )

    batches = [vcf_paths[i : i + batch_size] for i in range(0, len(vcf_paths), batch_size)]
    total_rows = 0
    t_start = time.perf_counter()

    for batch_idx, batch in enumerate(batches):
        if batch_idx + 1 < resume_batch_no:
            continue

        t_batch = time.perf_counter()
        batch_label = f"Batch {batch_idx + 1}/{len(batches)}"

        try:
            log.info(f"{batch_label}: parsing {len(batch)} CNV VCFs with {workers} workers...")
            with ProcessPoolExecutor(max_workers=workers) as ex:
                col_dicts = list(ex.map(parse_cnv_vcf, batch))

            merged = _merge_cnv_columnar(col_dicts)
            del col_dicts
            batch_row_count = len(merged["contigname"])
            total_rows += batch_row_count
            log.info(f"{batch_label}: {batch_row_count} rows parsed")

            cnv_arrow = _columnar_to_cnv_arrow(merged)
            del merged

            elapsed = _write_arrow_to_iceberg(cnv_table, cnv_arrow, "cnv")
            log.info(f"{batch_label}: wrote to cnv in {elapsed:.1f}s")
            del cnv_arrow

        except Exception:
            log.exception(
                f"{batch_label} FAILED. " f"Total rows written before failure: {total_rows}"
            )
            sys.exit(1)

        elapsed_batch = time.perf_counter() - t_batch
        elapsed_total = time.perf_counter() - t_start
        samples_done = min((batch_idx + 1) * batch_size, len(vcf_paths))
        rate = samples_done / elapsed_total
        eta = (len(vcf_paths) - samples_done) / rate if rate > 0 else 0
        log.info(
            f"{batch_label} done in {elapsed_batch:.1f}s | "
            f"Total: {samples_done}/{len(vcf_paths)} samples, {total_rows} rows | "
            f"Rate: {rate:.1f} samples/s | ETA: {eta/3600:.1f}h"
        )

    elapsed = time.perf_counter() - t_start
    log.info(
        f"CNV ingestion complete: {len(vcf_paths)} files, {total_rows:,} rows in {elapsed:.1f}s"
    )


def main(args_lst: list[str] = None, aws_creds=None):
    """
    aws_creds: tuple containing (aws_access_key_id, aws_secret_access_key, aws_region)
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths-file", required=True)
    ap.add_argument(
        "--workers", type=int, default=32, help="Parallel VCF parsers (leave 1 CPU for main thread)"
    )
    ap.add_argument("--batch-size", type=int, default=100, help="Samples per write batch")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument(
        "--benchmarks-paths-file-uri",
        type=str,
        default=None,
        help="S3 URI for the benchmark VCF paths file (used when --paths-file is 'benchmark_data')",
    )
    ap.add_argument(
        "--resume-batch-no",
        type=int,
        default=1,
        help="1-based batch number to resume from; batches before this are skipped. For --tables tpm, this is the global chunk number.",
    )
    ap.add_argument(
        "--tpm-rows-per-chunk",
        type=int,
        default=50_000_000,
        help="Rows per write chunk when ingesting --tables tpm. Keeps memory bounded for huge csv.gz files.",
    )
    ap.add_argument(
        "--tables",
        nargs="+",
        default=["variants", "annotations", "variants_by_sample"],
        choices=["variants", "annotations", "variants_by_sample", "tpm", "cnv"],
        help="Which tables to write to. 'tpm' ingests .csv.gz files into variants.tpm and must be run alone. 'cnv' ingests CNV vcf.gz files into variants.cnv and must be run alone.",
    )

    # read args from parameters if passed, else cmdline
    args = ap.parse_args(args_lst) if args_lst else ap.parse_args()

    # log = setup_logging(args.verbose)
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("ingest_log")

    os.environ["PYICEBERG_MAX_WORKERS"] = str(args.workers)

    # Initialize S3 client (uses explicit creds if provided)
    init_s3_client(aws_creds)

    # Resolve paths file
    paths_file = args.paths_file
    paths_range = None
    if re.match(r".*:::\d+-\d+", paths_file):
        paths_range = list(map(int, paths_file.split(":::")[1].split("-")))
        paths_file = paths_file.split(":::")[0]

    if paths_file == "benchmark_data":
        kwargs = {"uri": args.benchmarks_paths_file_uri} if args.benchmarks_paths_file_uri else {}
        paths_file = download_benchmark_paths_file(**kwargs)

    sample_batches = read_paths_file(paths_file)
    if paths_range:
        sample_batches = sample_batches[paths_range[0] : paths_range[1] + 1]

    # Step 1: Download zip files in parallel (I/O bound → threads)
    log.info("Downloading zip files from s3..")
    s3_paths = [s["path"] for s in sample_batches]
    with ThreadPoolExecutor(max_workers=32) as ex:
        list(ex.map(download_file_from_s3, s3_paths))

    # Step 2: Extract zip files to get .vcf.gz files (I/O bound → threads)
    log.info("Extracting zip files...")
    with ThreadPoolExecutor(max_workers=32) as ex:
        results = list(ex.map(extract_archive, s3_paths))
    samples = [vcf for batch in results for vcf in batch]
    log.info(f"Extracted {len(samples)} VCF files from {len(s3_paths)} zip files")

    vcf_paths = [s["path"] for s in samples]
    vcf_paths.sort()  # SORT to help with re-runs from mid
    log.info(f"Total samples: {len(vcf_paths)}")

    # Connect to catalog
    catalog_props = {
        "type": "rest",
        "uri": "https://s3tables.ap-southeast-1.amazonaws.com/iceberg",
        "warehouse": "", # S3_TABLE_BUCKET_ARN,
        "rest.sigv4-enabled": "true",
        "rest.signing-region": "ap-southeast-1",
        "rest.signing-name": "s3tables",
    }
    if aws_creds:
        key_id, secret, region = aws_creds
        catalog_props.update(
            {
                "client.access-key-id": key_id,
                "client.secret-access-key": secret,
                "client.region": region,
            }
        )
    catalog = load_catalog("s3tables", **catalog_props)

    if "tpm" in args.tables:
        if args.tables != ["tpm"]:
            log.error("'tpm' must be the only value passed to --tables; got %s", args.tables)
            sys.exit(2)
        _ingest_tpm(
            csv_paths=vcf_paths,
            catalog=catalog,
            rows_per_chunk=args.tpm_rows_per_chunk,
            resume_chunk_no=args.resume_batch_no,
            log=log,
        )
        return

    if "cnv" in args.tables:
        if args.tables != ["cnv"]:
            log.error("'cnv' must be the only value passed to --tables; got %s", args.tables)
            sys.exit(2)
        _ingest_cnv(
            sample_batches=sample_batches,
            catalog=catalog,
            batch_size=args.batch_size,
            workers=args.workers,
            resume_batch_no=args.resume_batch_no,
            log=log,
        )
        return

    tables = {}
    if "variants" in args.tables:
        tables["variants"] = catalog.load_table("variants.variants")
    if "annotations" in args.tables:
        tables["annotations"] = catalog.load_table("variants.annotations")
    if "variants_by_sample" in args.tables:
        tables["variants_by_sample"] = catalog.load_table("variants.variants_by_sample")

    log.info(f"Writing to tables: {list(tables.keys())}")

    # # Configure PyIceberg S3 client with larger connection pool
    # for tbl in tables.values():
    #     try:
    #         fs = tbl.io.get_fs('s3')
    #         if hasattr(fs, 'config_kwargs'):
    #             fs.config_kwargs['max_pool_connections'] = 20
    #             print("Successfully updated s3 config max_pool_connections")
    #         else:
    #             print("fs doesn't have 'config_kwargs' attr.. printing all avail attrs..")
    #             print(print(dir(fs)))
    #     except Exception as e:
    #         print(f"Failed to update fs s3 config: {e}")
    #         pass  # Skip if not S3 or not available

    # Batch processing
    batches = [
        vcf_paths[i : i + args.batch_size] for i in range(0, len(vcf_paths), args.batch_size)
    ]
    total_rows = 0
    t_start = time.perf_counter()

    for batch_idx, batch in enumerate(batches):
        if batch_idx + 1 < args.resume_batch_no:
            continue

        t_batch = time.perf_counter()
        batch_label = f"Batch {batch_idx + 1}/{len(batches)}"

        try:
            # # Step 1: Download VCFs in parallel (I/O bound → threads)
            # log.info(f"{batch_label}: downloading {len(batch)} VCFs...")
            # with ThreadPoolExecutor(max_workers=args.workers) as ex:
            #     list(ex.map(download_file_from_s3, batch))

            # Step 2: Parse (and Delete) VCFs in parallel (CPU bound → processes)
            log.info(f"{batch_label}: parsing {len(batch)} VCFs with {args.workers} workers...")
            with ProcessPoolExecutor(max_workers=32) as ex:
                col_dicts = list(ex.map(parse_vcf, batch))

            # Step 3: Merge columnar results and build Arrow table
            merged = merge_columnar(col_dicts)
            del col_dicts
            batch_row_count = len(merged["sample_name"])
            total_rows += batch_row_count
            log.info(f"{batch_label}: {batch_row_count} rows parsed")

            variants_arrow = columnar_to_variants_arrow(merged)
            del merged

            # Sort by contig, pos, ref, alt for Parquet row-group locality
            t_sort = time.perf_counter()
            sort_indices = pc.sort_indices(
                variants_arrow,
                sort_keys=[
                    ("contig", "ascending"),
                    ("pos_start", "ascending"),
                    ("ref", "ascending"),
                    ("alt", "ascending"),
                ],
            )
            variants_arrow = variants_arrow.take(sort_indices)
            log.info(f"{batch_label}: sorted in {time.perf_counter() - t_sort:.1f}s")

            # Prepare annotations before parallel writes (CPU work)
            annot_arrow = None
            if "annotations" in tables:
                t_dedup = time.perf_counter()
                annot_arrow = variants_to_annotations_arrow(variants_arrow)
                log.info(
                    f"{batch_label}: deduped {len(annot_arrow)} annotations in {time.perf_counter() - t_dedup:.1f}s"
                )

            write_futures = []
            with ThreadPoolExecutor(max_workers=3) as write_ex:
                if "variants" in tables:
                    write_futures.append(
                        (
                            "variants",
                            write_ex.submit(
                                _write_arrow_to_iceberg,
                                tables["variants"],
                                variants_arrow,
                                "variants",
                            ),
                        )
                    )
                if "variants_by_sample" in tables:
                    write_futures.append(
                        (
                            "variants_by_sample",
                            write_ex.submit(
                                _write_arrow_to_iceberg,
                                tables["variants_by_sample"],
                                variants_arrow,
                                "variants_by_sample",
                            ),
                        )
                    )
                if "annotations" in tables and annot_arrow is not None:
                    write_futures.append(
                        (
                            "annotations",
                            write_ex.submit(
                                _write_arrow_to_iceberg,
                                tables["annotations"],
                                annot_arrow,
                                "annotations",
                            ),
                        )
                    )

                for tbl_name, fut in write_futures:
                    elapsed = fut.result()
                    log.info(f"{batch_label}: wrote to {tbl_name} in {elapsed:.1f}s")

            del variants_arrow, annot_arrow

        except Exception:
            log.exception(
                f"{batch_label} FAILED at sample index {batch_idx * args.batch_size}. "
                f"First sample in batch: {batch[0]}. "
                f"Last successful batch: {batch_idx - 1 if batch_idx > 0 else 'none'}. "
                f"Total rows written before failure: {total_rows}"
            )
            sys.exit(1)

        elapsed_batch = time.perf_counter() - t_batch
        elapsed_total = time.perf_counter() - t_start
        samples_done = min((batch_idx + 1) * args.batch_size, len(vcf_paths))
        rate = samples_done / elapsed_total
        eta = (len(vcf_paths) - samples_done) / rate if rate > 0 else 0

        log.info(
            f"{batch_label} done in {elapsed_batch:.1f}s | "
            f"Total: {samples_done}/{len(vcf_paths)} samples, {total_rows} rows | "
            f"Rate: {rate:.1f} samples/s | ETA: {eta/3600:.1f}h"
        )

    elapsed = time.perf_counter() - t_start
    log.info(f"Ingestion complete: {len(vcf_paths)} samples, {total_rows} rows in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
