#!/usr/bin/env python3


import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import time
import subprocess

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


def create_spark_session():
    """Create Spark Session with HDFS configuration"""

    print("=== Initializing Spark Session ===")

    # Fix Python path for workers
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Create Spark Session with optimizations
    spark = pyspark.sql.SparkSession.builder \
        .appName("LogAnalyticsHDFS") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    # Configure HDFS access
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://localhost:9000")

    print(f" Spark {spark.version} initialized successfully")
    print(f" Spark UI: http://localhost:4040/")

    return spark


def load_log_data(spark):
    """Load log data from HDFS"""

    print("\n=== Loading Data from HDFS ===")

    hdfs_path = "hdfs://localhost:9000/logs/raw/*"

    try:
        logs_df = spark.read.json(hdfs_path)
        count = logs_df.count()
        partitions = logs_df.rdd.getNumPartitions()

        print(f" Loaded {count:,} log entries")
        print(f" Data distributed across {partitions} partitions")

        return logs_df

    except Exception as e:
        print(f" Failed to load data: {e}")
        return None


def explore_data(logs_df):
    """Basic data exploration and setup"""

    print("\n=== Data Overview ===")

    # Show schema
    print("\nData structure:")
    logs_df.printSchema()

    # Sample data
    print(f"\nSample log entries:")
    logs_df.show(3, truncate=False)

    # Data quality check
    print(f"\nDataset info:")
    print(f"  Records: {logs_df.count():,}")
    print(f"  Columns: {len(logs_df.columns)}")

    # Create time-enhanced view
    logs_with_time = logs_df.withColumn("date", to_date(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp")))

    logs_df.createOrReplaceTempView("logs")
    logs_with_time.createOrReplaceTempView("logs_time")

    print(" Temporary views created for SQL analysis")

    return logs_with_time


def analyze_log_patterns(spark):
    """Core log analysis using Spark SQL"""

    print("\n=== Log Pattern Analysis ===")

    results = {}

    # 1. Request distribution by log level
    print("\n1. Log Level Distribution:")
    level_counts = spark.sql("""
        SELECT level, COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as percentage
        FROM logs 
        GROUP BY level 
        ORDER BY count DESC
    """)
    level_counts.show()
    results['levels'] = level_counts

    # 2. Top endpoints analysis
    print("\n2. Most Accessed Endpoints:")
    top_endpoints = spark.sql("""
        SELECT endpoint, COUNT(*) as requests
        FROM logs 
        GROUP BY endpoint 
        ORDER BY requests DESC 
        LIMIT 10
    """)
    top_endpoints.show(truncate=False)
    results['endpoints'] = top_endpoints

    # 3. Error analysis
    print("\n3. Error Pattern Analysis:")
    error_analysis = spark.sql("""
        SELECT status_code, COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as error_rate
        FROM logs 
        WHERE status_code >= 400
        GROUP BY status_code 
        ORDER BY count DESC
    """)
    error_analysis.show()
    results['errors'] = error_analysis

    # 4. Server performance metrics
    print("\n4. Server Performance:")
    server_perf = spark.sql("""
        SELECT server, 
               COUNT(*) as total_requests,
               ROUND(AVG(response_time), 2) as avg_response_ms,
               MAX(response_time) as max_response_ms
        FROM logs 
        GROUP BY server 
        ORDER BY avg_response_ms DESC
    """)
    server_perf.show()
    results['servers'] = server_perf

    return results


def analyze_time_patterns(spark):
    """Time-based traffic analysis"""

    print("\n=== Time-Based Analysis ===")

    # Hourly traffic distribution
    print("\n1. Hourly Traffic Pattern:")
    hourly = spark.sql("""
        SELECT hour, 
               COUNT(*) as requests,
               ROUND(AVG(response_time), 2) as avg_response_time,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as errors
        FROM logs_time 
        GROUP BY hour 
        ORDER BY hour
    """)
    hourly.show(24)

    # Daily patterns
    print("\n2. Daily Traffic Summary:")
    daily = spark.sql("""
        SELECT date, 
               COUNT(*) as requests,
               ROUND(AVG(response_time), 2) as avg_response_time,
               ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate
        FROM logs_time 
        GROUP BY date 
        ORDER BY date
    """)
    daily.show()

    return {'hourly': hourly, 'daily': daily}


def analyze_user_behavior(spark):
    """User activity and session analysis"""

    print("\n=== User Behavior Analysis ===")

    # Top users by activity
    print("\n1. Most Active Users:")
    user_activity = spark.sql("""
        SELECT user_id, 
               COUNT(*) as requests,
               COUNT(DISTINCT session_id) as sessions,
               COUNT(DISTINCT endpoint) as unique_pages,
               ROUND(AVG(response_time), 2) as avg_response_time
        FROM logs 
        WHERE user_id IS NOT NULL
        GROUP BY user_id 
        ORDER BY requests DESC 
        LIMIT 15
    """)
    user_activity.show()

    # Session analysis
    print("\n2. Session Patterns:")
    sessions = spark.sql("""
        SELECT session_id, user_id, 
               COUNT(*) as page_views,
               COUNT(DISTINCT endpoint) as unique_pages,
               MIN(timestamp) as session_start,
               MAX(timestamp) as session_end
        FROM logs 
        WHERE session_id IS NOT NULL
        GROUP BY session_id, user_id
        ORDER BY page_views DESC
        LIMIT 10
    """)
    sessions.show(truncate=False)

    return {'users': user_activity, 'sessions': sessions}


def demonstrate_rdd_operations(spark, logs_df):
    """RDD operations using SQL for demonstration"""

    print("\n=== Distributed Processing Demo ===")

    # RDD info
    rdd = logs_df.rdd
    total_records = logs_df.count()
    partitions = rdd.getNumPartitions()

    print(f"Processing {total_records:,} records across {partitions} partitions")

    # Demonstrate key operations through SQL equivalents
    print("\n1. Filter Operations (Error logs):")
    error_count = spark.sql("SELECT COUNT(*) as count FROM logs WHERE status_code >= 400").collect()[0]['count']
    print(f"   Found {error_count:,} error entries")

    print("\n2. Aggregation Operations (IP frequency):")
    ip_counts = spark.sql("""
        SELECT ip_address, COUNT(*) as requests
        FROM logs 
        GROUP BY ip_address 
        ORDER BY requests DESC 
        LIMIT 5
    """).collect()

    for row in ip_counts:
        print(f"   {row['ip_address']}: {row['requests']} requests")

    print("\n3. Transformation Operations:")
    unique_endpoints = spark.sql("SELECT COUNT(DISTINCT endpoint) as count FROM logs").collect()[0]['count']
    print(f"   Identified {unique_endpoints} unique endpoints")

    return {
        'total_records': total_records,
        'partitions': partitions,
        'error_count': error_count,
        'unique_endpoints': unique_endpoints
    }


def simple_pattern_mining(spark):
    print("\n=== Frequent Pattern Analysis ===")

    # Check what data we actually have
    print("\nData validation:")
    total_records = spark.sql("SELECT COUNT(*) as count FROM logs").collect()[0]['count']
    print(f"Total records: {total_records:,}")

    # Check for null values in key fields
    null_check = spark.sql("""
        SELECT 
            SUM(CASE WHEN endpoint IS NULL THEN 1 ELSE 0 END) as null_endpoints,
            SUM(CASE WHEN method IS NULL THEN 1 ELSE 0 END) as null_methods,
            SUM(CASE WHEN server IS NULL THEN 1 ELSE 0 END) as null_servers
        FROM logs
    """).collect()[0]

    print(f"Null endpoints: {null_check['null_endpoints']}")
    print(f"Null methods: {null_check['null_methods']}")
    print(f"Null servers: {null_check['null_servers']}")

    # 1. Most common endpoint-method pairs (LOWERED threshold)
    print("\n1. Most Frequent Endpoint-Method Combinations:")

    # Try with a much lower threshold first
    frequent_patterns = spark.sql("""
        SELECT endpoint, method, COUNT(*) as frequency,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as percentage
        FROM logs 
        WHERE endpoint IS NOT NULL AND method IS NOT NULL
        GROUP BY endpoint, method
        HAVING COUNT(*) > 5
        ORDER BY frequency DESC
        LIMIT 10
    """)

    pattern_count = frequent_patterns.count()

    if pattern_count > 0:
        frequent_patterns.show(truncate=False)
    else:
        print("No patterns found with threshold > 5. Showing top patterns without threshold:")
        # Fallback: show top patterns regardless of threshold
        fallback_patterns = spark.sql("""
            SELECT endpoint, method, COUNT(*) as frequency,
                   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as percentage
            FROM logs 
            WHERE endpoint IS NOT NULL AND method IS NOT NULL
            GROUP BY endpoint, method
            ORDER BY frequency DESC
            LIMIT 10
        """)
        fallback_patterns.show(truncate=False)

    # 2. Error co-occurrence patterns (LOWERED threshold)
    print("\n2. Common Error Patterns:")
    error_patterns = spark.sql("""
        SELECT server, endpoint, status_code, COUNT(*) as frequency
        FROM logs 
        WHERE status_code >= 400 
        AND server IS NOT NULL 
        AND endpoint IS NOT NULL
        GROUP BY server, endpoint, status_code
        HAVING COUNT(*) > 2
        ORDER BY frequency DESC
        LIMIT 8
    """)

    error_count = error_patterns.count()

    if error_count > 0:
        error_patterns.show(truncate=False)
    else:
        print("No error patterns found with threshold > 2. Showing all error patterns:")
        # Fallback: show all error patterns
        fallback_errors = spark.sql("""
            SELECT server, endpoint, status_code, COUNT(*) as frequency
            FROM logs 
            WHERE status_code >= 400 
            AND server IS NOT NULL 
            AND endpoint IS NOT NULL
            GROUP BY server, endpoint, status_code
            ORDER BY frequency DESC
            LIMIT 8
        """)
        fallback_errors.show(truncate=False)

    # 3. Basic pattern statistics
    print("\n3. Pattern Statistics:")
    pattern_stats = spark.sql("""
        SELECT 
            COUNT(DISTINCT endpoint) as unique_endpoints,
            COUNT(DISTINCT method) as unique_methods,
            COUNT(DISTINCT CONCAT(endpoint, '-', method)) as unique_combinations,
            COUNT(DISTINCT server) as unique_servers
        FROM logs
        WHERE endpoint IS NOT NULL AND method IS NOT NULL
    """)
    pattern_stats.show()

    # 4. Most active servers
    print("\n4. Server Activity Patterns:")
    server_patterns = spark.sql("""
        SELECT server, COUNT(*) as requests,
               COUNT(DISTINCT endpoint) as unique_endpoints,
               COUNT(DISTINCT method) as unique_methods
        FROM logs 
        WHERE server IS NOT NULL
        GROUP BY server
        ORDER BY requests DESC
        LIMIT 5
    """)
    server_patterns.show()

    return frequent_patterns if pattern_count > 0 else fallback_patterns


def run_performance_tests(spark, logs_df):
    print("\n=== Performance Testing ===")

    response = input("Run performance benchmarks? (y/N): ").strip().lower()

    if response != 'y':
        print(" Skipping performance tests")
        return True

    try:
        print(" Running  performance analysis...")

        # Run all tests silently (no individual output)
        load_results = test_data_loading_performance_silent(spark)
        query_results = test_query_performance_silent(spark)
        rdd_results = test_rdd_performance_silent(spark, logs_df)

        # Generate  report
        generate_final_performance_report(load_results, query_results, rdd_results)

        return True

    except Exception as e:
        print(f" Performance testing failed: {e}")
        return False


def test_data_loading_performance_silent(spark):
    """Test data loading performance - SILENT """

    test_datasets = [
        ("Small Dataset", "hdfs://localhost:9000/logs/raw/logs_small.json"),
        ("Medium Dataset", "hdfs://localhost:9000/logs/raw/logs_medium.json"),
        ("Large Dataset", "hdfs://localhost:9000/logs/raw/logs_large.json"),
        ("All Raw Data", "hdfs://localhost:9000/logs/raw/*")
    ]

    results = []

    for test_name, path in test_datasets:
        try:
            start_time = time.time()
            df = spark.read.json(path)
            count = df.count()
            load_time = time.time() - start_time
            num_partitions = df.rdd.getNumPartitions()

            results.append({
                "test": test_name,
                "record_count": count,
                "load_time": load_time,
                "num_partitions": num_partitions,
                "records_per_second": count / load_time if load_time > 0 else 0
            })

        except Exception as e:
            results.append({
                "test": test_name,
                "error": str(e)
            })

    return results


def test_query_performance_silent(spark):
    """Test query performance - SILENT """

    try:
        df = spark.read.json("hdfs://localhost:9000/logs/raw/*")
        df.createOrReplaceTempView("logs")
        record_count = df.count()
    except Exception:
        return []

    test_queries = [
        ("Simple Count", "SELECT COUNT(*) FROM logs"),
        ("Group By Level", "SELECT level, COUNT(*) FROM logs GROUP BY level"),
        ("Filter + Count", "SELECT COUNT(*) FROM logs WHERE status_code >= 400"),
        ("Complex Aggregation",
         "SELECT server, COUNT(*) as requests, AVG(response_time) as avg_time FROM logs GROUP BY server ORDER BY requests DESC"),
        ("Time-based Query",
         "SELECT HOUR(timestamp) as hour, COUNT(*) as requests FROM logs GROUP BY HOUR(timestamp) ORDER BY hour"),
        ("Join-like Query",
         "SELECT endpoint, method, COUNT(*) as requests FROM logs GROUP BY endpoint, method ORDER BY requests DESC LIMIT 20")
    ]

    results = []

    for query_name, query in test_queries:
        try:
            start_time = time.time()
            result_df = spark.sql(query)
            result_count = result_df.count()
            query_time = time.time() - start_time

            results.append({
                "query": query_name,
                "execution_time": query_time,
                "result_rows": result_count,
                "input_records": record_count,
                "records_per_second": record_count / query_time if query_time > 0 else 0
            })

        except Exception as e:
            results.append({
                "query": query_name,
                "error": str(e)
            })

    return results


def test_rdd_performance_silent(spark, logs_df):
    """Test RDD operations performance - SILENT """

    try:
        rdd = logs_df.rdd
        record_count = rdd.count()
    except Exception:
        return []

    rdd_tests = [
        ("Count", lambda r: r.count()),
        ("Filter", lambda r: r.filter(lambda row: row.status_code >= 400).count()),
        ("Map", lambda r: r.map(lambda row: row.endpoint).distinct().count()),
        ("MapReduce", lambda r: r.map(lambda row: (row.server, 1)).reduceByKey(lambda a, b: a + b).count()),
        ("Complex Filter+Map", lambda r: r.filter(lambda row: row.level == "ERROR").map(
            lambda row: (row.server, row.response_time)).collect())
    ]

    results = []

    for test_name, operation in rdd_tests:
        try:
            start_time = time.time()
            result = operation(rdd)
            execution_time = time.time() - start_time

            result_info = {
                "operation": test_name,
                "execution_time": execution_time,
                "input_records": record_count,
                "records_per_second": record_count / execution_time if execution_time > 0 else 0
            }

            if isinstance(result, list):
                result_info["result_size"] = len(result)
            else:
                result_info["result_value"] = result

            results.append(result_info)

        except Exception as e:
            results.append({
                "operation": test_name,
                "error": str(e)
            })

    return results


def generate_final_performance_report(load_results, query_results, rdd_results):
    print(f"\n{'=' * 70}")
    print(" PERFORMANCE REPORT")
    print(f"{'=' * 70}")

    print(f"\nTest conducted: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"\n DATA LOADING PERFORMANCE")
    print("-" * 50)
    print(f"{'Test':<20} {'Records':<10} {'Time(s)':<8} {'Rate(rec/s)':<12} {'Status':<8}")
    print("-" * 50)

    for result in load_results:
        if 'error' not in result:
            print(
                f"{result['test']:<20} {result['record_count']:<10,} {result['load_time']:<8.2f} {result['records_per_second']:<12,.0f} {' PASS':<8}")
        else:
            print(f"{result['test']:<20} {'N/A':<10} {'N/A':<8} {'N/A':<12} {' FAIL':<8}")

    # === QUERY PERFORMANCE ===
    print(f"\n QUERY PERFORMANCE")
    print("-" * 50)
    print(f"{'Query':<20} {'Time(s)':<8} {'Rows':<8} {'Rate(rec/s)':<12} {'Status':<8}")
    print("-" * 50)

    for result in query_results:
        if 'error' not in result:
            print(
                f"{result['query']:<20} {result['execution_time']:<8.2f} {result['result_rows']:<8} {result['records_per_second']:<12,.0f} {' PASS':<8}")
        else:
            print(f"{result['query']:<20} {'N/A':<8} {'N/A':<8} {'N/A':<12} {' FAIL':<8}")

    # === RDD PERFORMANCE ===
    print(f"\n  RDD OPERATIONS PERFORMANCE")
    print("-" * 50)
    print(f"{'Operation':<20} {'Time(s)':<8} {'Rate(rec/s)':<12} {'Status':<8}")
    print("-" * 50)

    for result in rdd_results:
        if 'error' not in result:
            print(
                f"{result['operation']:<20} {result['execution_time']:<8.2f} {result['records_per_second']:<12,.0f} {' PASS':<8}")
        else:
            print(f"{result['operation']:<20} {'N/A':<8} {'N/A':<12} {' FAIL':<8}")

    # === PERFORMANCE SUMMARY ===
    successful_loads = len([r for r in load_results if 'error' not in r])
    successful_queries = len([r for r in query_results if 'error' not in r])
    successful_rdd = len([r for r in rdd_results if 'error' not in r])

    total_tests = len(load_results) + len(query_results) + len(rdd_results)
    total_passed = successful_loads + successful_queries + successful_rdd

    print(f"\n PERFORMANCE SUMMARY")
    print("-" * 30)
    print(f"Data Loading Tests:  {successful_loads}/{len(load_results)} passed")
    print(f"Query Tests:         {successful_queries}/{len(query_results)} passed")
    print(f"RDD Tests:           {successful_rdd}/{len(rdd_results)} passed")
    print(f"Overall Success:     {total_passed}/{total_tests} ({total_passed / total_tests * 100:.1f}%)")

    if successful_queries > 0:
        fastest_time = float('inf')
        fastest_query_name = None

        for result in query_results:
            if 'error' not in result and result['execution_time'] < fastest_time:
                fastest_time = result['execution_time']
                fastest_query_name = result['query']

        if fastest_query_name:
            print(f"Fastest Query:       {fastest_query_name} ({fastest_time:.2f}s)")

    if successful_rdd > 0:
        fastest_rdd_time = float('inf')
        fastest_rdd_name = None

        for result in rdd_results:
            if 'error' not in result and result['execution_time'] < fastest_rdd_time:
                fastest_rdd_time = result['execution_time']
                fastest_rdd_name = result['operation']

        if fastest_rdd_name:
            print(f"Fastest RDD Op:      {fastest_rdd_name} ({fastest_rdd_time:.2f}s)")

    if successful_loads > 0:
        best_rate = 0
        best_load_name = None

        for result in load_results:
            if 'error' not in result and result['records_per_second'] > best_rate:
                best_rate = result['records_per_second']
                best_load_name = result['test']

        if best_load_name:
            print(f"Best Load Rate:      {best_load_name} ({best_rate:.0f} rec/s)")

    print(f"\n Performance testing completed - All metrics captured")
    print(f"{'=' * 70}")


def save_results_to_hdfs(spark, results, time_results, user_results):
    """Save analysis results back to HDFS"""

    print("\n=== Saving Results to HDFS ===")

    try:
        # Save key results
        save_operations = [
            (results['endpoints'], "top_endpoints"),
            (results['errors'], "error_analysis"),
            (results['servers'], "server_performance"),
            (time_results['hourly'], "hourly_traffic"),
            (user_results['users'], "user_activity")
        ]

        successful = 0
        for dataframe, name in save_operations:
            try:
                hdfs_path = f"hdfs://localhost:9000/logs/results/{name}"
                dataframe.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_path)
                successful += 1
            except Exception as e:
                print(f" Failed to save {name}: {str(e)[:50]}...")

        return successful > 0

    except Exception as e:
        print(f" Save operation failed: {e}")
        return False


def generate_summary_report(results, time_results, user_results, rdd_info, start_time):
    end_time = time.time()
    processing_time = end_time - start_time

    print(f"\n{'=' * 60}")
    print(" LOG ANALYTICS SUMMARY REPORT")
    print(f"{'=' * 60}")

    print(f"\n⚡ Performance Metrics:")
    print(f"   Total processing time: {processing_time:.2f} seconds")
    print(f"   Records processed: {rdd_info['total_records']:,}")
    print(f"   Processing rate: {rdd_info['total_records'] / processing_time:,.0f} records/second")
    print(f"   Data partitions: {rdd_info['partitions']}")

    print(f"\n Key Findings:")
    print(f"   Total log entries: {rdd_info['total_records']:,}")
    print(
        f"   Error entries: {rdd_info['error_count']:,} ({rdd_info['error_count'] / rdd_info['total_records'] * 100:.1f}%)")
    print(f"   Unique endpoints: {rdd_info['unique_endpoints']}")

    # Top endpoint
    top_endpoint = results['endpoints'].first()
    print(f"   Most popular endpoint: {top_endpoint['endpoint']} ({top_endpoint['requests']} requests)")

    # Server performance
    slowest_server = results['servers'].first()
    print(f"   Slowest server: {slowest_server['server']} ({slowest_server['avg_response_ms']}ms avg)")

    print(f"\n Access Points:")
    print(f"   Spark UI: http://localhost:4040/")
    print(f"   HDFS Web UI: http://localhost:9870/")
    print(f"   Results: hdfs://localhost:9000/logs/results/")


def main():
    """Main analytics pipeline"""

    print("=== Big Data Log Analytics with Spark and HDFS ===")

    start_time = time.time()

    # Initialize Spark
    spark = create_spark_session()

    try:
        # Load and explore data
        logs_df = load_log_data(spark)
        if logs_df is None:
            print(" Cannot proceed without data. Please run upload_to_hdfs.py first.")
            return

        logs_with_time = explore_data(logs_df)

        # Run analytics
        print("\n Running distributed analytics...")
        results = analyze_log_patterns(spark)
        time_results = analyze_time_patterns(spark)
        user_results = analyze_user_behavior(spark)
        rdd_info = demonstrate_rdd_operations(spark, logs_df)

        # Pattern mining
        pattern_results = simple_pattern_mining(spark)

        # Performance testing (optional)
        run_performance_tests(spark, logs_df)

        # Save results
        save_success = save_results_to_hdfs(spark, results, time_results, user_results)

        # Generate summary
        generate_summary_report(results, time_results, user_results, rdd_info, start_time)

        # Keep Spark UI available for inspection
        print(f"\n  Spark UI available for 15 seconds: http://localhost:4040/")
        time.sleep(15)

    except Exception as e:
        print(f" Analytics failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print("\n Spark session terminated")


if __name__ == "__main__":
    main()
