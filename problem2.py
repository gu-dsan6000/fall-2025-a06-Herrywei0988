from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, min as spark_min, max as spark_max, col
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import argparse

def main():
    # --- Argument parsing ---
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("mode", help="local[*] or spark://<MASTER_IP>:7077")
    parser.add_argument("--net-id", required=True, help="Your NET ID (e.g., yw1103)")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark processing and only generate plots")
    args = parser.parse_args()

    # --- Directory setup ---
    base_dir = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/"
    out_dir = "data/output"
    os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Mode = {args.mode}")
    print(f"[INFO] Net ID = {args.net_id}")
    print(f"[INFO] Base directory = {base_dir}")
    print(f"[INFO] Output directory = {out_dir}")

    # --- Step 1: Spark job (if not skipped) ---
    if not args.skip_spark:
        spark = (
            SparkSession.builder
            .appName("Problem2_ClusterUsage")
            .master(args.mode)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
        )

        print("[INFO] Starting Spark session...")
        logs_df = spark.read.option("recursiveFileLookup", "true").text(base_dir)
        total_lines = logs_df.count()
        print(f"[DEBUG] Total lines read = {total_lines:,}")

        # --- Regex extraction ---
        pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*application_(\d+)_(\d+)"
        parsed_df = (
            logs_df
            .withColumn("timestamp", regexp_extract(col("value"), pattern, 1))
            .withColumn("cluster_id", regexp_extract(col("value"), pattern, 2))
            .withColumn("app_number", regexp_extract(col("value"), pattern, 3))
            .filter(col("cluster_id") != "")
        )

        valid_count = parsed_df.count()
        print(f"[DEBUG] Valid log entries: {valid_count:,}")

        if valid_count == 0:
            print("[ERROR] No valid log entries found — check regex or S3 path.")
            spark.stop()
            return

        # --- Compute application start/end ---
        timeline_df = (
            parsed_df.groupBy("cluster_id", "app_number")
            .agg(
                spark_min("timestamp").alias("start_time"),
                spark_max("timestamp").alias("end_time")
            )
        )

        # --- Convert to pandas and write actual CSV ---
        timeline_pd = timeline_df.toPandas()
        summary_pd = (
            timeline_df.groupBy("cluster_id")
            .agg(
                spark_min("start_time").alias("cluster_first_app"),
                spark_max("end_time").alias("cluster_last_app")
            )
            .toPandas()
        )

        timeline_out = os.path.join(out_dir, "problem2_timeline.csv")
        summary_out = os.path.join(out_dir, "problem2_cluster_summary.csv")

        timeline_pd.to_csv(timeline_out, index=False)
        summary_pd.to_csv(summary_out, index=False)

        print(f"[INFO] Wrote timeline CSV to {timeline_out}")
        print(f"[INFO] Wrote cluster summary CSV to {summary_out}")

        # --- Stats text file ---
        total_clusters = len(summary_pd)
        total_apps = len(timeline_pd)
        avg_apps = total_apps / total_clusters if total_clusters > 0 else 0

        stats_path = os.path.join(out_dir, "problem2_stats.txt")
        with open(stats_path, "w") as f:
            f.write(f"Total unique clusters: {total_clusters}\n")
            f.write(f"Total applications: {total_apps}\n")
            f.write(f"Average applications per cluster: {avg_apps:.2f}\n")
        print(f"[INFO] Wrote stats file to {stats_path}")

        spark.stop()
        print("[INFO] Spark job finished successfully.")
    else:
        print("[INFO] Skipping Spark job, using existing CSVs.")

    # --- Step 2: Visualization ---
    timeline_csv = os.path.join(out_dir, "problem2_timeline.csv")
    summary_csv = os.path.join(out_dir, "problem2_cluster_summary.csv")

    if not os.path.exists(timeline_csv) or not os.path.exists(summary_csv):
        raise FileNotFoundError("Timeline or summary CSV not found. Make sure Spark step ran first.")

    timeline_pd = pd.read_csv(timeline_csv)
    summary_pd = pd.read_csv(summary_csv)

    # --- Aggregation ---
    cluster_counts = timeline_pd.groupby("cluster_id")["app_number"].count().reset_index()
    cluster_counts.columns = ["cluster_id", "num_applications"]

    # --- Bar chart ---
    plt.figure(figsize=(6, 4))
    sns.barplot(x="cluster_id", y="num_applications", data=cluster_counts, palette="Set2")
    plt.xticks(rotation=45, ha="right")
    plt.title("Applications per Cluster")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "problem2_bar_chart.png"))
    plt.close()
    print("[INFO] Saved bar chart.")

    # --- Duration histogram (largest cluster) ---
    largest_cluster = cluster_counts.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
    subset = timeline_pd[timeline_pd["cluster_id"] == largest_cluster].copy()
    subset["start_time"] = pd.to_datetime(subset["start_time"], errors="coerce")
    subset["end_time"] = pd.to_datetime(subset["end_time"], errors="coerce")
    subset["duration"] = (subset["end_time"] - subset["start_time"]).dt.total_seconds()

    plt.figure(figsize=(6, 4))
    sns.histplot(subset["duration"].dropna(), kde=True, log_scale=True)
    plt.title(f"Job Duration Distribution (Cluster {largest_cluster})")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "problem2_density_plot.png"))
    plt.close()
    print("[INFO] Saved density plot.")

    print("[SUCCESS] All outputs generated successfully.")


if __name__ == "__main__":
    main()
