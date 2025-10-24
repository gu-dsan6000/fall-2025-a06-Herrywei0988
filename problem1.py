# problem1.py
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql import functions as F

def main():
    if len(sys.argv) != 4 or sys.argv[2] != "--net-id":
        print("Usage: uv run python problem1.py <spark-master-url> --net-id <YOUR_NET_ID>")
        sys.exit(1)

    master_url = sys.argv[1]
    net_id = sys.argv[3]

    print(f"[INFO] master_url = {master_url}")
    print(f"[INFO] net_id = {net_id}")

    spark = (
        SparkSession.builder
        .appName("Problem1_Log_Level_Distribution")
        .master(master_url)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    # read all .log files recursively
    input_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/"
    logs_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .text(input_path)
    )

    total_lines = logs_df.count()
    print(f"[DEBUG] Total raw log lines read = {total_lines:,}")

    if total_lines == 0:
        print("[ERROR] No log files read from S3 — check path or permissions.")
        spark.stop()
        sys.exit(1)

    # improved regex (detect INFO/WARN/ERROR/DEBUG at word boundaries)
    parsed_df = logs_df.select(
        regexp_extract('value', r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias('log_level'),
        col('value').alias('log_entry')
    )

    clean_df = parsed_df.filter(col("log_level") != "")
    clean_count = clean_df.count()
    print(f"[DEBUG] Rows with log_level = {clean_count:,}")

    # sample preview
    print("[DEBUG] Preview:")
    clean_df.show(5, truncate=False)

    # --- Aggregations ---
    counts_df = clean_df.groupBy("log_level").count().orderBy("count", ascending=False)
    sample_df = clean_df.orderBy(F.rand()).limit(10).select("log_entry", "log_level")

    output_dir = "/home/ubuntu/spark-cluster/data/output"
    os.makedirs(output_dir, exist_ok=True)

    # force collect to driver and save as real CSV (not Spark folder)
    counts_pd = counts_df.toPandas()
    sample_pd = sample_df.toPandas()

    counts_pd.to_csv(os.path.join(output_dir, "problem1_counts.csv"), index=False)
    sample_pd.to_csv(os.path.join(output_dir, "problem1_sample.csv"), index=False)

    summary_path = os.path.join(output_dir, "problem1_summary.txt")
    total_with_level = clean_count
    unique_levels = counts_df.count()
    with open(summary_path, "w") as f:
        f.write(f"Total log lines processed: {total_lines}\n")
        f.write(f"Total lines with log levels: {total_with_level}\n")
        f.write(f"Unique log levels found: {unique_levels}\n")

    print(f"[SUCCESS] Wrote output files to: {output_dir}")
    spark.stop()
    print("[INFO] Spark session stopped.")


if __name__ == "__main__":
    main()
