from __future__ import annotations

import argparse
import csv
import glob
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window


def _file_glob(*parts: str) -> str:
    """Build a Spark-readable file glob for local Windows paths.

    Spark accepts forward slashes on Windows.
    """
    p = Path(*parts)
    return str(p.resolve()).replace("\\", "/")


def _is_windows() -> bool:
    return os.name == "nt"


def _is_irrelevant_category(col: F.Column) -> F.Column:
    """Values to exclude from visualization outputs.

    User requested to drop UNKNOWN and any 'not displayed' buckets.
    """

    normalized = F.upper(F.trim(col))
    return (
        normalized.isNull()
        | (normalized == "")
        | (normalized == "UNKNOWN")
        | normalized.contains("KHÔNG HIỂN THỊ")
    )


def _read_local_tsv_rows(path_glob: str, expected_cols: int) -> list[list[str]]:
    rows: list[list[str]] = []
    for file_path in sorted(glob.glob(path_glob)):
        # Pig outputs are typically UTF-8 text; be tolerant of bad bytes.
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip("\n").rstrip("\r")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < expected_cols:
                    parts = parts + [None] * (expected_cols - len(parts))
                rows.append(parts[:expected_cols])
    return rows


def _export_small_csv(df, out_csv: Path) -> None:
    """Export a small DataFrame to a single CSV file.

    On Windows, Spark's local FS integration is frequently painful (winutils).
    These viz outputs are intentionally small, so collecting is acceptable.
    """

    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # If a previous run exported Spark-style CSV folder outputs, remove them to
    # avoid confusion (and to allow re-exports with the same base name).
    legacy_dir = out_csv.with_suffix("")
    if legacy_dir.exists() and legacy_dir.is_dir():
        shutil.rmtree(legacy_dir)

    rows = list(df.toLocalIterator())
    columns = list(df.columns)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for r in rows:
            writer.writerow([r[c] for c in columns])


def _export_df(df, out_dir: Path, name: str, fmt: str) -> None:
    """Export a DataFrame for visualization.

    - csv: single file on Windows, folder output on Linux via Spark
    - parquet: folder output
    """
    fmt = fmt.lower().strip()
    if fmt not in {"csv", "parquet"}:
        raise ValueError(f"Unsupported export format: {fmt}")

    if fmt == "csv":
        # Always export as a single CSV file (better UX for Excel/Power BI).
        _export_small_csv(df, out_dir / f"{name}.csv")
        return

    # parquet
    df.write.mode("overwrite").parquet(
        str((out_dir / name).resolve()).replace("\\", "/")
    )


def load_two_col_tsv(spark: SparkSession, path_glob: str, col1: str, col2: str):
    schema = T.StructType(
        [
            T.StructField(col1, T.StringType(), True),
            T.StructField(col2, T.LongType(), True),
        ]
    )

    # Native Spark-on-Windows often breaks on local globbing due to missing
    # Hadoop native bits (winutils/hadoop.dll). For local exploration, load
    # via Python IO and then create a DataFrame.
    if _is_windows():
        rows = _read_local_tsv_rows(path_glob, expected_cols=2)
        parsed = [(r[0], int(r[1]) if r[1] not in (None, "") else None) for r in rows]
        return spark.createDataFrame(parsed, schema=schema)

    return (
        spark.read.option("sep", "\t")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(path_glob)
    )


def load_three_col_tsv(
    spark: SparkSession, path_glob: str, col1: str, col2: str, col3: str
):
    schema = T.StructType(
        [
            T.StructField(col1, T.StringType(), True),
            T.StructField(col2, T.StringType(), True),
            T.StructField(col3, T.LongType(), True),
        ]
    )

    if _is_windows():
        rows = _read_local_tsv_rows(path_glob, expected_cols=3)
        parsed = [
            (r[0], r[1], int(r[2]) if r[2] not in (None, "") else None) for r in rows
        ]
        return spark.createDataFrame(parsed, schema=schema)

    return (
        spark.read.option("sep", "\t")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(path_glob)
    )


def load_four_col_tsv(
    spark: SparkSession, path_glob: str, c1: str, c2: str, c3: str, c4: str
):
    schema = T.StructType(
        [
            T.StructField(c1, T.StringType(), True),
            T.StructField(c2, T.StringType(), True),
            T.StructField(c3, T.StringType(), True),
            T.StructField(c4, T.LongType(), True),
        ]
    )

    if _is_windows():
        rows = _read_local_tsv_rows(path_glob, expected_cols=4)
        parsed = [
            (r[0], r[1], r[2], int(r[3]) if r[3] not in (None, "") else None) for r in rows
        ]
        return spark.createDataFrame(parsed, schema=schema)

    return (
        spark.read.option("sep", "\t")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(path_glob)
    )


def load_job_base_clean(spark: SparkSession, path_glob: str):
    # job_base_clean appears to be 10 tab-separated columns.
    # On native Windows, avoid spark.read.text() to dodge Hadoop native issues.
    if _is_windows():
        rows = _read_local_tsv_rows(path_glob, expected_cols=10)
        schema = T.StructType(
            [
                T.StructField("job_id", T.LongType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("category_location_raw", T.StringType(), True),
                T.StructField("company_raw", T.StringType(), True),
                T.StructField("location_raw", T.StringType(), True),
                T.StructField("experience_raw", T.StringType(), True),
                T.StructField("requirements_raw", T.StringType(), True),
                T.StructField("industry_raw", T.StringType(), True),
                T.StructField("education_raw", T.StringType(), True),
                T.StructField("employment_type_raw", T.StringType(), True),
            ]
        )
        parsed = []
        for r in rows:
            job_id = int(r[0]) if r[0] not in (None, "") else None
            parsed.append((job_id, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]))
        df = spark.createDataFrame(parsed, schema=schema)
        return df.withColumn("_ncols", F.lit(10)).withColumn("_raw", F.lit(None).cast("string"))

    raw = spark.read.text(path_glob).select(F.col("value").alias("_raw"))
    parts = F.split(F.col("_raw"), "\t", -1)

    cols = [
        (0, "job_id"),
        (1, "title"),
        (2, "category_location_raw"),
        (3, "company_raw"),
        (4, "location_raw"),
        (5, "experience_raw"),
        (6, "requirements_raw"),
        (7, "industry_raw"),
        (8, "education_raw"),
        (9, "employment_type_raw"),
    ]

    df = raw.select(
        *[parts.getItem(i).alias(name) for i, name in cols],
        F.size(parts).alias("_ncols"),
        F.col("_raw"),
    )

    # Cast id if possible; keep null on bad rows.
    df = df.withColumn("job_id", F.col("job_id").cast("long"))
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Explore Pig output (local folder) using PySpark"
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).parent.joinpath("output")),
        help="Path to the Pig output folder (default: ./output)",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=20,
        help="Number of rows to show for sample outputs",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export viz-ready aggregates to output/viz (CSV by default)",
    )
    parser.add_argument(
        "--export-format",
        default="csv",
        choices=["csv", "parquet"],
        help="Export format for viz outputs (default: csv)",
    )
    parser.add_argument(
        "--export-dir",
        default=None,
        help="Directory to export viz outputs (default: <output-dir>/viz)",
    )

    args = parser.parse_args()
    output_dir = Path(args.output_dir)

    # On Windows it's common for `python` on PATH to be the Microsoft Store alias,
    # which breaks Spark's ability to launch Python workers.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName("recruitment-output-explore")
        # For local analysis on a laptop/desktop.
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    export_dir = (
        Path(args.export_dir)
        if args.export_dir
        else Path(output_dir).joinpath("viz")
    )

    # 1) industry_total: (industry, count)
    industry_total_path = _file_glob(output_dir, "industry_total", "part-*")
    industry_total = load_two_col_tsv(
        spark, industry_total_path, "industry", "job_count"
    )
    print("\n== industry_total (top 20 by job_count) ==")
    industry_total.orderBy(F.desc("job_count"), F.asc("industry")).show(
        args.show, truncate=False
    )

    industry_total_known = industry_total.where(
        (~F.col("industry").isin("UNKNOWN", "TONG_TAT_CA"))
        & (~_is_irrelevant_category(F.col("industry")))
    )

    industry_total_viz = industry_total.where(
        (~F.col("industry").isin("TONG_TAT_CA")) & (~_is_irrelevant_category(F.col("industry")))
    )

    # 2) industry_by_location: (province, industry, count)
    industry_by_location_path = _file_glob(output_dir, "industry_by_location", "part-*")
    industry_by_location = load_three_col_tsv(
        spark, industry_by_location_path, "province", "industry", "job_count"
    )
    print("\n== industry_by_location (sample) ==")
    industry_by_location.show(args.show, truncate=False)

    industry_by_location_viz = industry_by_location.where(
        ~_is_irrelevant_category(F.col("industry"))
    )

    province_total = (
        industry_by_location_viz.groupBy("province")
        .agg(F.sum("job_count").alias("province_job_count"))
        .orderBy(F.desc("province_job_count"), F.asc("province"))
    )

    print("\n== Top industries by province (top 3 each) ==")
    w = F.row_number().over(
        Window.partitionBy("province").orderBy(F.desc("job_count"), F.asc("industry"))
    )
    (
        industry_by_location_viz.withColumn("rn", w)
        .where(F.col("rn") <= 3)
        .orderBy(F.asc("province"), F.asc("rn"))
        .show(200, truncate=False)
    )

    top5_industries_by_province = (
        industry_by_location_viz.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("province").orderBy(
                    F.desc("job_count"), F.asc("industry")
                )
            ),
        )
        .where(F.col("rn") <= 5)
    )

    province_industry_share = (
        industry_by_location_viz.join(province_total, on="province", how="left")
        .withColumn(
            "share",
            F.when(F.col("province_job_count") > 0, F.col("job_count") / F.col("province_job_count")).otherwise(
                F.lit(None)
            ),
        )
        .select("province", "industry", "job_count", "province_job_count", "share")
    )

    # 3) job_base_clean: 10 columns (tab-separated)
    job_base_clean_path = _file_glob(output_dir, "job_base_clean", "part-*")
    job_base_clean = load_job_base_clean(spark, job_base_clean_path)

    print("\n== job_base_clean: column count distribution ==")
    job_base_clean.groupBy("_ncols").count().orderBy("_ncols").show(50, truncate=False)

    print("\n== job_base_clean: sample rows ==")
    job_base_clean.select(
        "job_id",
        "title",
        "location_raw",
        "experience_raw",
        "industry_raw",
        "education_raw",
        "employment_type_raw",
        "_ncols",
    ).show(args.show, truncate=False)

    print("\n== job_base_clean: top locations (raw) ==")
    (
        job_base_clean.groupBy("location_raw")
        .count()
        .orderBy(F.desc("count"), F.asc("location_raw"))
        .show(args.show, truncate=False)
    )

    # 4) requirement_analysis outputs (industry_code, industry, label, count)
    exp_path = _file_glob(output_dir, "requirement_analysis", "exp_total", "part-*")
    edu_path = _file_glob(output_dir, "requirement_analysis", "edu_total", "part-*")
    type_path = _file_glob(output_dir, "requirement_analysis", "type_total", "part-*")
    skill_path = _file_glob(output_dir, "requirement_analysis", "skill_total", "part-*")

    exp_by_industry = load_four_col_tsv(
        spark, exp_path, "industry_code", "industry", "experience", "job_count"
    )
    edu_by_industry = load_four_col_tsv(
        spark, edu_path, "industry_code", "industry", "education", "job_count"
    )
    type_by_industry = load_four_col_tsv(
        spark, type_path, "industry_code", "industry", "employment_type", "job_count"
    )
    skill_by_industry = load_four_col_tsv(
        spark, skill_path, "industry_code", "industry", "skill", "job_count"
    )

    exp_total = (
        exp_by_industry.where(~_is_irrelevant_category(F.col("experience"))).groupBy("experience")
        .agg(F.sum("job_count").alias("job_count"))
        .orderBy(F.desc("job_count"), F.asc("experience"))
    )
    edu_total = (
        edu_by_industry.where(~_is_irrelevant_category(F.col("education"))).groupBy("education")
        .agg(F.sum("job_count").alias("job_count"))
        .orderBy(F.desc("job_count"), F.asc("education"))
    )
    type_total = (
        type_by_industry.where(~_is_irrelevant_category(F.col("employment_type"))).groupBy("employment_type")
        .agg(F.sum("job_count").alias("job_count"))
        .orderBy(F.desc("job_count"), F.asc("employment_type"))
    )
    skill_total = (
        skill_by_industry.where(~_is_irrelevant_category(F.col("skill"))).groupBy("skill")
        .agg(F.sum("job_count").alias("job_count"))
        .orderBy(F.desc("job_count"), F.asc("skill"))
    )

    print("\n== requirement_analysis: experience total (top 20) ==")
    exp_total.show(20, truncate=False)
    print("\n== requirement_analysis: education total (top 20) ==")
    edu_total.show(20, truncate=False)
    print("\n== requirement_analysis: employment type total (top 20) ==")
    type_total.show(20, truncate=False)
    print("\n== requirement_analysis: skill total (top 20) ==")
    skill_total.show(20, truncate=False)

    if args.export:
        export_dir.mkdir(parents=True, exist_ok=True)

        # Map/cung-cầu
        _export_df(industry_total_viz, export_dir, "industry_total", args.export_format)
        _export_df(industry_total_known, export_dir, "industry_total_known", args.export_format)
        _export_df(industry_by_location_viz, export_dir, "industry_by_location", args.export_format)
        _export_df(province_total, export_dir, "province_total", args.export_format)
        _export_df(top5_industries_by_province, export_dir, "top5_industries_by_province", args.export_format)
        _export_df(province_industry_share, export_dir, "province_industry_share", args.export_format)

        # Requirements
        _export_df(exp_total, export_dir, "requirement_experience_total", args.export_format)
        _export_df(edu_total, export_dir, "requirement_education_total", args.export_format)
        _export_df(type_total, export_dir, "requirement_employment_type_total", args.export_format)
        _export_df(skill_total.limit(500), export_dir, "requirement_skill_total_top500", args.export_format)

    spark.stop()


if __name__ == "__main__":
    main()
