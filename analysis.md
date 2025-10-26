# Problem 1: Log Level Distribution

## Objective
The goal of this task was to analyze log messages from the distributed Spark cluster to identify the distribution of different log levels (INFO, WARN, ERROR, DEBUG) across all application logs stored in Amazon S3.  
This provides an overview of how frequently each type of log message appears and helps highlight potential sources of instability or excessive logging.

---

## Approach
1. **Spark-based data ingestion:**  
   Used PySpark’s `spark.read.text()` with recursive file lookup to read all `.log` files stored under  
   `s3a://yw1103-assignment-spark-cluster-logs/data/`.

2. **Regex extraction:**  
   Applied regular expressions to extract both timestamps and log levels from each log line.  
   Only the entries containing recognized log levels (INFO, WARN, ERROR, DEBUG) were retained.

3. **Aggregation:**  
   Grouped records by `log_level` and calculated total counts using `groupBy().count()`.

4. **Sampling:**  
   Randomly selected 10 log entries with their corresponding log levels to provide representative examples.

5. **Outputs generated:**  
   - `problem1_counts.csv` — Summary count per log level  
   - `problem1_sample.csv` — Ten randomly sampled log entries  
   - `problem1_summary.txt` — Overall statistics summary

---

## Results
| Metric | Value |
|---------|--------|
| **Total log lines processed** | 33,236,604 |
| **Lines containing valid log levels** | 27,410,336 |
| **Unique log levels identified** | 3 (`INFO`, `ERROR`, `WARN`) |

### Log Level Distribution
| Log Level | Count | Percentage |
|------------|--------|------------|
| **INFO**  | 27,389,482 | ~99.93 % |
| **ERROR** | 11,259 | ~0.04 % |
| **WARN**  | 9,595 | ~0.03 % |

---

## Key Observations
- **INFO logs dominate (~99.9 %)**, indicating most system events are routine operations and task completions.  
- **ERROR** and **WARN** messages occur much less frequently, suggesting stable cluster performance with minimal runtime exceptions.  
- The absence of **DEBUG** logs implies production-level logging configuration, where verbose debugging output is disabled for efficiency.

---

## Interpretation & Insights
The heavy skew toward `INFO` messages is typical in production Spark environments, where detailed `DEBUG` logs are suppressed and only essential operational messages are retained.  
The small but non-zero number of `ERROR` and `WARN` entries could be further investigated to pinpoint which executors or stages triggered them.

---

## Conclusion
This analysis confirms that the Spark cluster’s logging behavior is functioning as expected—primarily informative logs, with a small proportion of warnings and errors.  
Such a pattern suggests a **healthy and stable distributed system** with minimal failure incidents.


# Problem 2 — Cluster Usage Analysis

## Overview

In this problem, we analyzed cluster usage patterns across Spark application logs stored in S3.  
The goal was to identify:

1. How many unique clusters exist  
2. How heavily each cluster is used  
3. The duration and distribution of application executions  

Using a distributed Spark job running on the EC2 cluster, the program parsed over **33 million log lines**, extracted cluster IDs, application IDs, and timestamps, and aggregated start/end times to produce a time-series dataset suitable for visualization.

---

## Summary Statistics

| Metric                     | Value  |
|---------------------------|--------|
| Total unique clusters     | 6      |
| Total applications        | 193    |
| Average applications/cluster | 32.17 |

> While multiple clusters exist in the dataset, usage is extremely imbalanced — one cluster dominates the workload.

---

## Cluster Usage Summary

| cluster_id      | num_applications | cluster_first_app | cluster_last_app  |
|-----------------|------------------|-------------------|-------------------|
| 1485248649253   | 180              | 17/01/24 17:00:29 | 17/07/27 21:45:00 |
| 1472621869829   | 8                | 16/09/09 07:43:50 | 16/09/09 10:07:06 |
| 1460011102909   | 1                | 16/07/26 11:54:22 | 16/07/26 12:19:25 |
| 144806111297    | 2                | 16/04/07 10:45:23 | 16/04/07 12:22:08 |
| 1440487435730   | 1                | 15/09/01 18:14:42 | 15/09/01 18:15:01 |
| 1474351042505   | 1                | 16/11/18 22:30:10 | 16/11/18 22:30:10 |

> Cluster `1485248649253` processed ≈ **93%** of all applications, suggesting most computational demand was concentrated on a single cluster node.

---

## Visualization Results

### 1. Applications per Cluster (Bar Chart)

The bar chart confirms the imbalance observed in the summary statistics:  
One cluster (`1485248649253`) handled ~180 applications, while all others had ≤ 8.

### 2. Job Duration Distribution (Density Plot)

The duration distribution for the busiest cluster is **highly skewed**, with:

- Most jobs completing in under **10³ seconds**
- A few long-running outliers exceeding **10⁷ seconds**

This skew suggests heterogeneous job types — many short tasks interspersed with a few very heavy workloads.

---

## Insights & Conclusion

### Load Imbalance Detected
One cluster dominates execution, potentially indicating uneven job scheduling or resource allocation.

### Short- vs Long-Running Jobs
The density plot reveals strong right-skewness, typical in mixed workloads where few large batch jobs coexist with many short ones.

---

## Operational Implications

- **Monitoring and load balancing** should focus on the heavily used cluster `1485248649253`.
- **Long-duration jobs** may warrant profiling for optimization or rescheduling.

---

## Deliverables Generated

| File                      | Description                             |
|---------------------------|-----------------------------------------|
| `problem2_timeline.csv`   | Per-application start/end times         |
| `problem2_cluster_summary.csv` | Cluster-level usage summary       |
| `problem2_stats.txt`      | Overall statistics                      |
| `problem2_bar_chart.png`  | Applications per cluster visualization |
| `problem2_density_plot.png` | Job duration distribution            |
