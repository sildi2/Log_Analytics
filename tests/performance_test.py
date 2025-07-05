#!/usr/bin/env python3


import time
import pyspark
from pyspark.sql.functions import *
import os
import sys

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


def create_test_spark_session():
    """Create Spark session for testing"""

    spark = pyspark.sql.SparkSession.builder \
        .appName("PerformanceTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Set HDFS configuration
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://localhost:9000")

    return spark


def test_data_loading_performance():
    """Test data loading performance with different dataset sizes"""

    print("=== Testing Data Loading Performance ===")

    spark = create_test_spark_session()

    test_datasets = [
        ("Small Dataset", "hdfs://localhost:9000/logs/raw/logs_small.json"),
        ("Medium Dataset", "hdfs://localhost:9000/logs/raw/logs_medium.json"),
        ("Large Dataset", "hdfs://localhost:9000/logs/raw/logs_large.json"),
        ("All Raw Data", "hdfs://localhost:9000/logs/raw/*")
    ]

    results = []

    for test_name, path in test_datasets:
        print(f"\n--- Testing {test_name} ---")

        try:
            start_time = time.time()

            # Load data
            df = spark.read.json(path)

            # Force evaluation with count
            count = df.count()

            load_time = time.time() - start_time

            # Get partition info
            num_partitions = df.rdd.getNumPartitions()

            result = {
                "test": test_name,
                "record_count": count,
                "load_time": load_time,
                "num_partitions": num_partitions,
                "records_per_second": count / load_time if load_time > 0 else 0
            }

            results.append(result)

            print(f"Records: {count:,}")
            print(f"Load time: {load_time:.2f}s")
            print(f"Partitions: {num_partitions}")
            print(f"Records/sec: {count / load_time:,.0f}")

        except Exception as e:
            print(f"Failed to test {test_name}: {e}")
            results.append({
                "test": test_name,
                "error": str(e)
            })

    spark.stop()
    return results


def test_query_performance():
    """Test query performance on different operations"""

    print("\n=== Testing Query Performance ===")

    spark = create_test_spark_session()

    # Load data for testing
    try:
        df = spark.read.json("hdfs://localhost:9000/logs/raw/*")
        df.createOrReplaceTempView("logs")

        record_count = df.count()
        print(f"Testing with {record_count:,} records")

    except Exception as e:
        print(f"Failed to load test data: {e}")
        spark.stop()
        return []

    # Define test queries
    test_queries = [
        ("Simple Count", "SELECT COUNT(*) FROM logs"),
        ("Group By Level", "SELECT level, COUNT(*) FROM logs GROUP BY level"),
        ("Filter + Count", "SELECT COUNT(*) FROM logs WHERE status_code >= 400"),
        ("Complex Aggregation", """
            SELECT server, COUNT(*) as requests, AVG(response_time) as avg_time
            FROM logs GROUP BY server ORDER BY requests DESC
        """),
        ("Time-based Query", """
            SELECT HOUR(timestamp) as hour, COUNT(*) as requests
            FROM logs GROUP BY HOUR(timestamp) ORDER BY hour
        """)
    ]

    results = []

    for query_name, query in test_queries:
        print(f"\n--- Testing {query_name} ---")

        try:
            start_time = time.time()

            # Execute query
            result_df = spark.sql(query)

            # Force evaluation
            result_count = result_df.count()

            query_time = time.time() - start_time

            result = {
                "query": query_name,
                "execution_time": query_time,
                "result_rows": result_count,
                "input_records": record_count,
                "records_per_second": record_count / query_time if query_time > 0 else 0
            }

            results.append(result)

            print(f"Execution time: {query_time:.2f}s")
            print(f"Result rows: {result_count}")
            print(f"Processing rate: {record_count / query_time:,.0f} records/sec")

        except Exception as e:
            print(f"Failed to execute {query_name}: {e}")
            results.append({
                "query": query_name,
                "error": str(e)
            })

    spark.stop()
    return results


def generate_performance_report(load_results, query_results):
    """Generate performance report"""

    print(f"\n{'=' * 60}")
    print("PERFORMANCE TEST REPORT")
    print(f"{'=' * 60}")

    print(f"\nTest conducted on: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Data Loading Performance
    print(f"\n--- DATA LOADING PERFORMANCE ---")
    for result in load_results:
        if 'error' not in result:
            print(
                f"{result['test']:20s}: {result['record_count']:>8,} records in {result['load_time']:>6.2f}s ({result['records_per_second']:>8,.0f} rec/s)")
        else:
            print(f"{result['test']:20s}: ERROR - {result['error']}")

    # Query Performance
    print(f"\n--- QUERY PERFORMANCE ---")
    for result in query_results:
        if 'error' not in result:
            print(
                f"{result['query']:20s}: {result['execution_time']:>6.2f}s ({result['records_per_second']:>8,.0f} rec/s)")
        else:
            print(f"{result['query']:20s}: ERROR - {result['error']}")

    # Performance Summary - FIXED: Removed problematic min() calls
    print(f"\n--- PERFORMANCE SUMMARY ---")

    # Count successful operations
    successful_loads = len([r for r in load_results if 'error' not in r])
    successful_queries = len([r for r in query_results if 'error' not in r])

    print(f"Successful load tests: {successful_loads}/{len(load_results)}")
    print(f"Successful query tests: {successful_queries}/{len(query_results)}")

    # Find best performing operations manually
    if successful_queries > 0:
        fastest_time = float('inf')
        fastest_query = None

        for result in query_results:
            if 'error' not in result and result['execution_time'] < fastest_time:
                fastest_time = result['execution_time']
                fastest_query = result

        if fastest_query:
            print(f"Fastest Query: {fastest_query['query']} ({fastest_query['execution_time']:.2f}s)")




def main():

    print("=== Big Data Log Analytics Performance Testing ===")
    print("Testing system performance with corrected paths")

    start_time = time.time()

    # Run performance tests
    print("\nStarting performance tests...")

    load_results = test_data_loading_performance()
    query_results = test_query_performance()

    # Generate report
    generate_performance_report(load_results, query_results)

    total_time = time.time() - start_time
    print(f"\nTotal testing time: {total_time:.2f} seconds")

    print(f"\n=== Performance Testing Complete ===")


if __name__ == "__main__":
    main()