#!/usr/bin/env python3
"""
Enhanced Log Analytics with Spark - Following Course Examples
Big Data Course Project - Log Analytics with HDFS Integration
"""

import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import time

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.hdfs_operations import HDFSManager


def create_spark_session():
    """Create Spark Session with HDFS configuration (course pattern)"""

    print("=== Creating Spark Session ===")

    # Create Spark Session (exact course pattern with HDFS config)
    spark = pyspark.sql.SparkSession.builder \
        .appName("LogAnalyticsHDFS") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    # Set Hadoop configuration for HDFS access
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://localhost:9000")

    print("Spark session created with HDFS integration!")
    print(f"Spark version: {spark.version}")
    print(f"Spark UI: http://localhost:4040/")

    return spark


def load_log_data(spark, dataset_type="standard"):
    """Load log data from HDFS (course pattern)"""

    print(f"\n=== Loading log data from HDFS ({dataset_type}) ===")

    # Different dataset paths based on type
    if dataset_type == "standard":
        hdfs_path = "hdfs://localhost:9000/logs/standard/logs_medium.json"
    elif dataset_type == "partitioned":
        hdfs_path = "hdfs://localhost:9000/logs/partitioned/*/*"  # All partitioned data
    elif dataset_type == "realistic":
        hdfs_path = "hdfs://localhost:9000/logs/realistic/*/*"  # All realistic data
    elif dataset_type == "all":
        hdfs_path = "hdfs://localhost:9000/logs/*/*"  # All data
    else:
        hdfs_path = "hdfs://localhost:9000/logs/standard/logs_small.json"  # Default small

    try:
        # Read JSON files from HDFS (following course examples)
        logs_df = spark.read.json(hdfs_path)

        count = logs_df.count()
        print(f"Loaded {count:,} log entries from HDFS")
        print(f"Number of partitions: {logs_df.rdd.getNumPartitions()}")

        return logs_df

    except Exception as e:
        print(f"Failed to load data from {hdfs_path}: {e}")
        print("Make sure HDFS is running and data is uploaded")
        return None


def basic_data_exploration(logs_df):
    """Basic data exploration (course style)"""

    print("\n=== Basic Data Exploration ===")

    # Show schema (course style)
    print("\nData schema:")
    logs_df.printSchema()

    # Show sample data (course pattern)
    print("\nSample log entries:")
    logs_df.show(5, truncate=False)

    # Basic statistics
    print(f"\nDataset statistics:")
    print(f"Total records: {logs_df.count():,}")
    print(f"Columns: {len(logs_df.columns)}")

    # Check for null values
    print("\nNull value check:")
    for column in logs_df.columns:
        null_count = logs_df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"  {column}: {null_count} null values")

    return logs_df


def create_temporary_views(logs_df):
    """Create temporary views for SQL queries (course pattern)"""

    print("\n=== Creating Temporary Views ===")

    # Create temporary view for SQL queries (course pattern)
    logs_df.createOrReplaceTempView("logs")
    print("Temporary view 'logs' created")

    # Create additional views for time-based analysis
    logs_with_time = logs_df.withColumn("date", to_date(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp")))

    logs_with_time.createOrReplaceTempView("logs_time")
    print("Temporary view 'logs_time' created with time components")

    return logs_with_time


def basic_analytics(spark):
    """Basic analytics using Spark SQL (course style)"""

    print("\n=== Basic Log Analytics Results ===")

    # 1. Count by log level (course SQL pattern)
    print("\n1. Log entries by level:")
    level_counts = spark.sql("""
        SELECT level, COUNT(*) as count 
        FROM logs 
        GROUP BY level 
        ORDER BY count DESC
    """)
    level_counts.show()

    # 2. Top endpoints (course SQL pattern)
    print("\n2. Top 10 endpoints:")
    top_endpoints = spark.sql("""
        SELECT endpoint, COUNT(*) as requests
        FROM logs 
        GROUP BY endpoint 
        ORDER BY requests DESC 
        LIMIT 10
    """)
    top_endpoints.show()

    # 3. Error analysis (course pattern)
    print("\n3. Error analysis (status codes >= 400):")
    errors = spark.sql("""
        SELECT status_code, COUNT(*) as error_count
        FROM logs 
        WHERE status_code >= 400
        GROUP BY status_code 
        ORDER BY error_count DESC
    """)
    errors.show()

    # 4. Server performance (course aggregation pattern)
    print("\n4. Average response time by server:")
    server_performance = spark.sql("""
        SELECT server, 
               AVG(response_time) as avg_response_time,
               COUNT(*) as total_requests,
               MIN(response_time) as min_response_time,
               MAX(response_time) as max_response_time
        FROM logs 
        GROUP BY server 
        ORDER BY avg_response_time DESC
    """)
    server_performance.show()

    # 5. HTTP method distribution
    print("\n5. HTTP method distribution:")
    method_dist = spark.sql("""
        SELECT method, COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as percentage
        FROM logs 
        GROUP BY method 
        ORDER BY count DESC
    """)
    method_dist.show()

    return {
        'level_counts': level_counts,
        'top_endpoints': top_endpoints,
        'errors': errors,
        'server_performance': server_performance,
        'method_dist': method_dist
    }


def time_based_analytics(spark):
    """Time-based analysis (course pattern)"""

    print("\n=== Time-based Analysis ===")

    # Hourly traffic distribution
    hourly_traffic = spark.sql("""
        SELECT hour, COUNT(*) as requests,
               AVG(response_time) as avg_response_time,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as errors
        FROM logs_time 
        GROUP BY hour 
        ORDER BY hour
    """)

    print("Hourly traffic distribution:")
    hourly_traffic.show(24)

    # Daily traffic patterns
    daily_traffic = spark.sql("""
        SELECT date, COUNT(*) as requests,
               AVG(response_time) as avg_response_time,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as errors,
               ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate
        FROM logs_time 
        GROUP BY date 
        ORDER BY date
    """)

    print("\nDaily traffic patterns:")
    daily_traffic.show()

    # Day of week analysis
    dow_traffic = spark.sql("""
        SELECT day_of_week,
               CASE day_of_week 
                   WHEN 1 THEN 'Sunday'
                   WHEN 2 THEN 'Monday' 
                   WHEN 3 THEN 'Tuesday'
                   WHEN 4 THEN 'Wednesday'
                   WHEN 5 THEN 'Thursday'
                   WHEN 6 THEN 'Friday'
                   WHEN 7 THEN 'Saturday'
               END as day_name,
               COUNT(*) as requests,
               AVG(response_time) as avg_response_time
        FROM logs_time 
        GROUP BY day_of_week 
        ORDER BY day_of_week
    """)

    print("\nDay of week analysis:")
    dow_traffic.show()

    return {
        'hourly_traffic': hourly_traffic,
        'daily_traffic': daily_traffic,
        'dow_traffic': dow_traffic
    }


def user_analytics(spark):
    """User activity analysis (course pattern)"""

    print("\n=== User Activity Analysis ===")

    # Top active users
    user_activity = spark.sql("""
        SELECT user_id, COUNT(*) as requests, 
               AVG(response_time) as avg_response_time,
               COUNT(DISTINCT session_id) as sessions,
               COUNT(DISTINCT endpoint) as unique_endpoints
        FROM logs 
        WHERE user_id IS NOT NULL
        GROUP BY user_id 
        ORDER BY requests DESC 
        LIMIT 20
    """)

    print("Top 20 most active users:")
    user_activity.show()

    # Session analysis
    session_analysis = spark.sql("""
        SELECT session_id, user_id, COUNT(*) as page_views,
               COUNT(DISTINCT endpoint) as unique_pages,
               MIN(timestamp) as session_start,
               MAX(timestamp) as session_end,
               AVG(response_time) as avg_response_time
        FROM logs 
        WHERE session_id IS NOT NULL
        GROUP BY session_id, user_id
        ORDER BY page_views DESC
        LIMIT 15
    """)

    print("\nTop 15 user sessions:")
    session_analysis.show(truncate=False)

    return {
        'user_activity': user_activity,
        'session_analysis': session_analysis
    }


def rdd_operations_demo(logs_df):
    """RDD operations demonstration (course RDD pattern)"""

    print("\n=== RDD Operations (Course Style) ===")

    # Convert to RDD (course pattern)
    logs_rdd = logs_df.rdd
    print(f"RDD created with {logs_rdd.count():,} elements")

    # Filter operations (course narrow transformation)
    error_logs_rdd = logs_rdd.filter(lambda row: row.status_code >= 400)
    error_count = error_logs_rdd.count()
    print(f"Error logs: {error_count:,}")

    # Map operations (course transformation)
    endpoints_rdd = logs_rdd.map(lambda row: row.endpoint)
    unique_endpoints = endpoints_rdd.distinct().collect()
    print(f"Unique endpoints: {len(unique_endpoints)}")

    # Show some unique endpoints
    print("Sample endpoints:")
    for i, endpoint in enumerate(unique_endpoints[:8]):
        print(f"  {i + 1}. {endpoint}")

    # MapReduce pattern example (course style)
    print("\nMapReduce pattern - IP address frequency:")
    ip_counts = logs_rdd.map(lambda row: (row.ip_address, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False) \
        .take(10)

    for ip, count in ip_counts:
        print(f"  {ip}: {count} requests")

    return {
        'total_records': logs_rdd.count(),
        'error_count': error_count,
        'unique_endpoints': len(unique_endpoints),
        'top_ips': ip_counts
    }


def save_results_to_hdfs(results, time_results, user_results):
    """Save analysis results to HDFS (course pattern)"""

    print("\n=== Saving Results to HDFS ===")

    hdfs_manager = HDFSManager()
    hdfs_manager.create_directory("/logs/results")

    try:
        # Save basic analytics results
        results['top_endpoints'].coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("hdfs://localhost:9000/logs/results/top_endpoints")
        print("✅ Top endpoints saved to HDFS")

        results['errors'].coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("hdfs://localhost:9000/logs/results/error_analysis")
        print("✅ Error analysis saved to HDFS")

        results['server_performance'].coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("hdfs://localhost:9000/logs/results/server_performance")
        print("✅ Server performance saved to HDFS")

        # Save time-based results
        time_results['hourly_traffic'].coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("hdfs://localhost:9000/logs/results/hourly_traffic")
        print("✅ Hourly traffic analysis saved to HDFS")

        time_results['daily_traffic'].coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("hdfs://localhost:9000/logs/results/daily_traffic")
        print("✅ Daily traffic analysis saved to HDFS")

        # Save user analytics
        user_results['user_activity'].coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("hdfs://localhost:9000/logs/results/user_activity")
        print("✅ User activity analysis saved to HDFS")

        print("\nAll results saved to HDFS at /logs/results/")

    except Exception as e:
        print(f"Error saving results: {e}")


def performance_metrics(spark, start_time):
    """Show performance metrics (course style)"""

    end_time = time.time()
    total_time = end_time - start_time

    print(f"\n=== Performance Metrics ===")
    print(f"Total processing time: {total_time:.2f} seconds")

    # Spark metrics
    try:
        sc = spark.sparkContext
        print(f"Spark application ID: {sc.applicationId}")
        print(f"Total tasks: {sc.statusTracker().getExecutorInfos()[0].totalTasks}")
        print(f"Active tasks: {sc.statusTracker().getExecutorInfos()[0].activeTasks}")
    except:
        print("Could not retrieve detailed Spark metrics")


def main():
    """Main analytics function following course style"""

    print("=== Enhanced Big Data Log Analytics with Spark and HDFS ===")

    start_time = time.time()

    # Create Spark Session
    spark = create_spark_session()

    try:
        # Load data from HDFS
        # Load directly from where we uploaded the data
        logs_df = spark.read.json("hdfs://localhost:9000/logs/raw/*")
        if logs_df is None:
            print("Failed to load data. Exiting.")
            return

        # Basic data exploration
        basic_data_exploration(logs_df)

        # Create temporary views
        logs_with_time = create_temporary_views(logs_df)

        # Run analytics
        results = basic_analytics(spark)
        time_results = time_based_analytics(spark)
        user_results = user_analytics(spark)
        rdd_results = rdd_operations_demo(logs_df)

        # Save results to HDFS
        save_results_to_hdfs(results, time_results, user_results)

        # Show performance metrics
        performance_metrics(spark, start_time)

        print("\n=== Analysis Complete! ===")
        print("Results demonstrate:")
        print("1. ✅ HDFS data loading and storage")
        print("2. ✅ Spark SQL queries on distributed data")
        print("3. ✅ RDD transformations and actions")
        print("4. ✅ Data aggregation and filtering")
        print("5. ✅ Time-based analytics")
        print("6. ✅ User behavior analysis")
        print("7. ✅ Results saved back to HDFS")

        print(f"\nCheck results in HDFS: hdfs dfs -ls /logs/results/")
        print(f"Spark UI: http://localhost:4040/")
        print(f"HDFS Web UI: http://localhost:9870/")

    except Exception as e:
        print(f"Error during analysis: {e}")

    finally:
        # Stop Spark session (course pattern)
        spark.stop()
        print("\nSpark session stopped")


if __name__ == "__main__":
    main()