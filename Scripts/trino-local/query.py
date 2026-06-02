import csv
import time
import trino

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="trino",
    catalog="iceberg",
    schema="variants",
)


def set_session_properties():
    cur = conn.cursor()
    cur.execute("SET SESSION join_distribution_type = 'BROADCAST'")


def explain_analyze_query(query: str) -> str:
    cur = conn.cursor()
    cur.execute(f"EXPLAIN ANALYZE {query}")
    result = cur.fetchall()
    print("\n".join(row[0] for row in result))


def benchmark_queries(queries: list[str], output_csv: str = "benchmark.csv") -> list[dict]:
    cur = conn.cursor()
    results = []

    for i, query in enumerate(queries, 1):
        print(f"\n--- Query {i} ---")
        start = time.perf_counter()
        cur.execute(query)
        rows = cur.fetchall()
        elapsed_s = time.perf_counter() - start

        stats = cur.stats or {}
        physical_input_bytes = stats.get("physicalInputBytes", 0)
        processed_bytes = stats.get("processedBytes", 0)
        physical_input_gb = round(physical_input_bytes / 1e9, 6) if physical_input_bytes else None
        processed_gb = round(processed_bytes / 1e9, 6) if processed_bytes else None
        rows_returned = len(rows)

        metrics_str = (
            f"{int(elapsed_s)} secs\n"
            f"{physical_input_gb} GB physical\n"
            f"{processed_gb} GB processed\n"
            f"{rows_returned} rows"
        )
        row = {
            "query": query.strip(),
            "metrics": metrics_str,
        }
        results.append(row)
        print(f"  Time:           {int(elapsed_s)}s")
        print(f"  Physical input: {physical_input_gb} GB")
        print(f"  Processed:      {processed_gb} GB")
        print(f"  Rows returned:  {rows_returned}")

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["query", "metrics"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults written to {output_csv}")
    return results


queries = [
    """
    WITH a AS (
        SELECT 
            symbol,
            LOWER(pathogenecity) AS pathogenecity,
            chrom, pos, ref, alt
        FROM annotations_dedup
        WHERE 
            (gnomad_af < 0.01 OR gnomad_af IS NULL)
            AND LOWER(pathogenecity) LIKE '%pathogenic%'
            AND chrom = 'chr6' AND (pos >= 25000000 AND pos < 30000000)
    ), v as (
        SELECT 
            sample_name, contig, pos_start, ref, alt
        FROM variants
        WHERE 
            genotype != '0/0'
            AND read_depth >= 10
            AND filters NOT LIKE '%LowQ%'
            AND contig = 'chr6' AND (pos_start >= 25000000 AND pos_start < 30000000)
    )
    SELECT 
        a.symbol AS gene,
        a.pathogenecity,
        COUNT(distinct v.sample_name) AS carrier_count
    FROM v INNER JOIN a
    ON v.contig = a.chrom and v.pos_start = a.pos and v.ref = a.ref and v.alt = a.alt
    GROUP BY
        a.symbol, a.pathogenecity
    ORDER BY carrier_count DESC
    """,
    """
    SELECT * FROM variants
    WHERE contig = 'chr7' AND pos_start >= 144000320 AND pos_start <= 144008793
    """,
    """
    SELECT * FROM variants
    WHERE sample_name = 'S02N4GZR_001'
    """,
    """
    SELECT * FROM variants
    WHERE
        ((contig = 'chr7' AND pos_start >= 144000320 AND pos_start <= 144008793)
        OR (contig = 'chr12' AND pos_start >= 17832 AND pos_start <= 4349861))
        AND (sample_name = 'S02N4GZR_001' OR sample_name = 'SFXM2YM3')
    """,
    """
    SELECT * FROM variants
    WHERE (sample_name = 'S02N4GZR_001' OR sample_name = 'SFXM2YM3')
        AND filters = 'PASS'
    """,
    """
    SELECT * FROM variants
    WHERE contig = 'chr7' AND pos_start >= 110215000 AND pos_start <= 110216000
        AND filters = 'AC0'
    """,
    """
    SELECT * FROM variants
    WHERE vep_symbol = 'WASH8P' AND sample_name = 'SFXM2YM3'
    """,
    """
    SELECT * FROM variants
    WHERE somalone_somatic_score >= 0.99
        AND contig = 'chr12' AND pos_start >= 1783200 AND pos_start <= 4349861
    """,
    """
    SELECT DISTINCT sample_name FROM variants
    WHERE (PATHOGENECITY = 'protective&likely_pathogenic' OR PATHOGENECITY = 'other')
        AND filters = 'PASS'
    """,
    """
    SELECT * FROM variants
    WHERE contig = 'chr15' AND pos_start >= 22225250 AND pos_start <= 22225374
        AND vep_consequence = 'transcript_ablation'
        AND read_depth >= 20
        AND variant_allele_frequency >= 0.05
    """,
    """
    SELECT sample_name, COUNT(*) AS variant_count, AVG(read_depth) AS mean_depth
    FROM variants
    GROUP BY sample_name
    ORDER BY variant_count DESC
    """,
    """
    SELECT PATHOGENECITY, count(*) FROM variants
    GROUP BY PATHOGENECITY
    ORDER BY PATHOGENECITY
    """,
    """
    SELECT
        vep_symbol,
        COUNT(*) AS vus_count
    FROM variants
    WHERE pathogenecity = 'uncertain_significance,pathogenic'
        AND vep_impact IN ('HIGH', 'MODERATE')
        AND vep_consequence != 'synonymous_variant'
        AND read_depth >= 10
        AND genotype NOT IN ('./.', '.|.')
    GROUP BY vep_symbol
    ORDER BY vus_count DESC
    """,
    """
    SELECT * FROM variants
    WHERE gnomad_af < 0.01
        AND pathogenecity IN ('pathogenic', 'uncertain_significance')
        AND genotype != '0/1'
        AND read_depth >= 10
        AND filters NOT LIKE '%LowQ%'
    """,
    """
    SELECT vep_symbol AS gene, LOWER(pathogenecity) AS pathogenicity,
        COUNT(DISTINCT sample_name) AS carrier_count
    FROM variants
    WHERE (gnomad_af < 0.01 OR gnomad_af IS NULL)
        AND LOWER(pathogenecity) LIKE '%pathogenic%'
        AND genotype != '0/0'
        AND read_depth >= 10
        AND filters NOT LIKE '%LowQ%'
    GROUP BY vep_symbol, LOWER(pathogenecity)
    ORDER BY carrier_count DESC
    """,
    """
    SELECT vep_symbol AS gene,
        COUNT(DISTINCT CASE WHEN data_source = 'ICGC-ARGO' THEN sample_name END) AS icgc_carriers,
        COUNT(DISTINCT CASE WHEN data_source LIKE '%TCGA%' THEN sample_name END) AS tcga_carriers
    FROM variants.variants
    WHERE (gnomad_af < 0.01 OR gnomad_af IS NULL)
        AND (vep_impact = 'HIGH' OR LOWER(pathogenecity) LIKE '%pathogenic%')
        AND genotype != '0/0'
        AND read_depth >= 10
        AND data_type = 'real'
        AND filters NOT LIKE '%LowQ%'
    GROUP BY vep_symbol
    ORDER BY icgc_carriers DESC
    """,
    """
    SELECT vep_symbol AS gene, contig, pos_start, data_source,
        COUNT(*) AS variant_rows,
        COUNT(DISTINCT sample_name) AS carrier_count
    FROM variants
    WHERE (
            (contig = 'chr1' AND pos_start BETWEEN 243000000 AND 244000000)
            OR vep_symbol = 'TP53'
        )
        AND genotype != '0/0'
        AND read_depth >= 10
        AND filters = 'PASS'
        AND (gnomad_af < 0.01 OR gnomad_af IS NULL)
        AND data_source IN ('ICGC-ARGO', 'TCGA-GDC')
    GROUP BY vep_symbol, contig, pos_start, data_source
    ORDER BY variant_rows DESC
    """,
    """
    WITH base AS (
        SELECT vep_symbol AS gene, sample_name,
            CASE
                WHEN LOWER(pathogenecity) LIKE '%likely%'     THEN 'likely_pathogenic'
                WHEN LOWER(pathogenecity) LIKE '%pathogenic%' THEN 'pathogenic'
                ELSE 'other'
            END AS pathogenicity_class,
            CASE
                WHEN data_source = 'ICGC-ARGO' AND vep_impact = 'HIGH' THEN 'cohort_A_high'
                WHEN data_source = 'ICGC-ARGO'                         THEN 'cohort_A'
                WHEN data_source LIKE '%TCGA%'                         THEN 'cohort_B'
                ELSE 'full_cohort'
            END AS cohort_group
        FROM variants
        WHERE (gnomad_af < 0.01 OR gnomad_af IS NULL)
            AND vep_symbol IN ('TP53', 'BRCA1', 'KMT2C')
            AND (vep_impact = 'HIGH' OR LOWER(pathogenecity) LIKE '%pathogenic%')
            AND genotype != '0/0'
            AND read_depth >= 10
            AND filters NOT LIKE '%LowQ%'
    )
    SELECT gene, pathogenicity_class, cohort_group,
        COUNT(*) AS variant_count,
        COUNT(DISTINCT sample_name) AS carrier_count
    FROM base
    GROUP BY gene, pathogenicity_class, cohort_group
    ORDER BY gene, cohort_group
    """,
]


queries_sep_annotations_table = [
    """
    SELECT v.*
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE a.symbol = 'WASH8P' AND v.sample_name = 'SFXM2YM3'
    """,
    """
    SELECT DISTINCT v.sample_name
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE v.filters = 'PASS'
        AND (a.pathogenecity = 'protective&likely_pathogenic' OR a.pathogenecity = 'other')
    """,
    """
    SELECT v.*
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE v.contig = 'chr15' AND v.pos_start >= 22225250 AND v.pos_start <= 22225374
        AND a.chrom = 'chr15' AND a.pos >= 22225250 AND a.pos <= 22225374
        AND a.consequence = 'transcript_ablation'
        AND v.read_depth >= 20 AND v.variant_allele_frequency >= 0.05
    """,
    """
    SELECT count(expression)
    FROM
        tpm T INNER JOIN variants V
        ON T.sample_id = V.sample_name and T.gene_symbol = V.vep_symbol 
    WHERE
        T.gene_symbol = 'DNAH2' and V.vep_symbol = 'DNAH2' and V.data_source = 'ICGC-ARGO'
    """,
    """
    WITH v AS (
        SELECT contig, pos_start, ref, alt, count(*) AS variant_count
        FROM variants
        GROUP BY contig, pos_start, ref, alt
    )
    SELECT a.pathogenecity, sum(v.variant_count) AS vus_count
    FROM v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    GROUP BY a.pathogenecity
    ORDER BY a.pathogenecity
    """,
    """
    WITH v AS (
        SELECT contig, pos_start, ref, alt, count(*) AS variant_count
        FROM variants
        WHERE read_depth >= 10 AND genotype NOT IN ('./.', '.|.')
        GROUP BY contig, pos_start, ref, alt
    )
    SELECT a.symbol, sum(v.variant_count) AS vus_count
    FROM v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE a.pathogenecity = 'uncertain_significance,pathogenic'
        AND a.impact IN ('HIGH', 'MODERATE')
        AND a.consequence != 'synonymous_variant'
    GROUP BY a.symbol
    ORDER BY vus_count DESC
    """,
    """
    SELECT *
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE a.gnomad_af < 0.01
        AND a.pathogenecity IN ('pathogenic', 'uncertain_significance')
        AND v.genotype != '0/1'
        AND v.read_depth >= 10
        AND v.filters NOT LIKE '%LowQ%'
    """,
    """
    SELECT a.symbol AS gene, LOWER(a.pathogenecity) AS pathogenicity,
        COUNT(DISTINCT v.sample_name) AS carrier_count
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE (a.gnomad_af < 0.01 OR a.gnomad_af IS NULL)
        AND LOWER(a.pathogenecity) LIKE '%pathogenic%'
        AND v.genotype != '0/0'
        AND v.read_depth >= 10
        AND v.filters NOT LIKE '%LowQ%'
    GROUP BY a.symbol, LOWER(a.pathogenecity)
    ORDER BY carrier_count DESC
    """,
    """
    SELECT a.symbol AS gene,
        COUNT(DISTINCT CASE WHEN v.data_source = 'ICGC-ARGO' THEN v.sample_name END) AS icgc_carriers,
        COUNT(DISTINCT CASE WHEN v.data_source = 'TCGA-GDC'  THEN v.sample_name END) AS tcga_carriers
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE (a.gnomad_af < 0.01 OR a.gnomad_af IS NULL)
        AND (a.impact = 'HIGH' OR LOWER(a.pathogenecity) LIKE '%pathogenic%')
        AND v.genotype != '0/0'
        AND v.read_depth >= 10
        AND v.filters NOT LIKE '%LowQ%'
    GROUP BY a.symbol
    ORDER BY icgc_carriers DESC
    """,
    """
    SELECT a.symbol AS gene, v.contig, v.pos_start, v.data_source,
        COUNT(*) AS variant_rows,
        COUNT(DISTINCT v.sample_name) AS carrier_count
    FROM variants v INNER JOIN annotations_dedup a
        ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
    WHERE (
            (v.contig = 'chr1' AND v.pos_start BETWEEN 243000000 AND 244000000)
            OR a.symbol = 'TP53'
        )
        AND v.genotype != '0/0'
        AND v.read_depth >= 10
        AND v.filters = 'PASS'
        AND (a.gnomad_af < 0.01 OR a.gnomad_af IS NULL)
        AND v.data_source IN ('ICGC-ARGO', 'TCGA-GDC')
    GROUP BY a.symbol, v.contig, v.pos_start, v.data_source
    ORDER BY variant_rows DESC
    """,
    """
    WITH base AS (
        SELECT a.symbol AS gene, v.sample_name,
            CASE
                WHEN LOWER(a.pathogenecity) LIKE '%likely%'     THEN 'likely_pathogenic'
                WHEN LOWER(a.pathogenecity) LIKE '%pathogenic%' THEN 'pathogenic'
                ELSE 'other'
            END AS pathogenicity_class,
            CASE
                WHEN v.data_source = 'ICGC-ARGO' AND a.impact = 'HIGH' THEN 'cohort_A_high'
                WHEN v.data_source = 'ICGC-ARGO'                        THEN 'cohort_A'
                WHEN v.data_source LIKE '%TCGA%'                        THEN 'cohort_B'
                ELSE 'full_cohort'
            END AS cohort_group
        FROM variants v INNER JOIN annotations_dedup a
            ON v.contig = a.chrom AND v.pos_start = a.pos AND v.ref = a.ref AND v.alt = a.alt
        WHERE (a.gnomad_af < 0.01 OR a.gnomad_af IS NULL)
            AND a.symbol IN ('TP53', 'BRCA1', 'KMT2C')
            AND (a.impact = 'HIGH' OR LOWER(a.pathogenecity) LIKE '%pathogenic%')
            AND v.genotype != '0/0'
            AND v.read_depth >= 10
            AND v.filters NOT LIKE '%LowQ%'
    )
    SELECT gene, pathogenicity_class, cohort_group,
        COUNT(*) AS variant_count,
        COUNT(DISTINCT sample_name) AS carrier_count
    FROM base
    GROUP BY gene, pathogenicity_class, cohort_group
    ORDER BY gene, cohort_group
    """,
]

# benchmark_queries(queries[:1], output_csv="benchmark.csv")
# benchmark_queries(queries_sep_annotations_table, output_csv="benchmark_sat.csv")

set_session_properties()

explain_analyze_query(
    """
        WITH a AS (
        SELECT 
            symbol,
            LOWER(pathogenecity) AS pathogenecity,
            chrom, pos, ref, alt
        FROM annotations_dedup
        WHERE 
            (gnomad_af < 0.01 OR gnomad_af IS NULL)
            AND LOWER(pathogenecity) LIKE '%pathogenic%'
            AND chrom = 'chr6' --AND pos between 5000000*5 AND 5000000*6
        ), v as (
            SELECT 
                sample_name,
                contig, pos_start, ref, alt
            FROM variants
            WHERE 
                genotype != '0/0'
                AND read_depth >= 10
                AND filters NOT LIKE '%LowQ%'
                AND contig = 'chr6' --AND pos_start between 5000000*5 AND 5000000*6
        )
        SELECT
            a.symbol AS gene,
            a.pathogenecity,
            COUNT(DISTINCT v.sample_name) AS carrier_count
        FROM v INNER JOIN a
        ON v.contig = a.chrom and v.pos_start = a.pos and v.ref = a.ref and v.alt = a.alt
        GROUP BY
            a.symbol, a.pathogenecity
        ORDER BY carrier_count DESC
    """
)