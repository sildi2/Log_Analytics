#!/usr/bin/env python3
"""
Performance testing for the log analytics system
Following course patterns and measuring key metrics
"""

import time
import pyspark
from pyspark.sql.functions import *
import os
import sys

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.hdfs_operations import HDFSManager
from utils.config import get_spark_config, HDFS_CONFIG


def create_test_spark_session():
    """Create Spark session for testing"""

    spark_config = get_spark_config()

    spark = pyspark.sql.SparkSession.builder \
        .appName("PerformanceTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Set HDFS configuration
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", HDFS_CONFIG['namenode_rpc'])

    return spark


def test_data_loading_performance():
    """Test data loading performance with different dataset sizes"""

    print("=== Testing Data Loading Performance ===")

    spark = create_test_spark_session()

    # Test different data sizes
    test_datasets = [
        ("Small Dataset", "hdfs://localhost:9000/logs/standard/logs_small.json"),
        ("Medium Dataset", "hdfs://localhost:9000/logs/standard/logs_medium.json"),
        ("Large Dataset", "hdfs://localhost:9000/logs/standard/logs_large.json"),
        ("Partitioned Data", "hdfs://localhost:9000/logs/partitioned/*/*"),
        ("All Data", "hdfs://localhost:9000/logs/*/*")
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

    # Load medium dataset for testing
    try:
        df = spark.read.json("hdfs://localhost:9000/logs/partitioned/*/*")
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
        """),
        ("Join-like Query", """
            SELECT endpoint, method, COUNT(*) as requests
            FROM logs GROUP BY endpoint, method ORDER BY requests DESC LIMIT 20
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


def test_rdd_performance():
    """Test RDD operations performance"""

    print("\n=== Testing RDD Performance ===")

    spark = create_test_spark_session()

    try:
        # Load data
        df = spark.read.json("hdfs://localhost:9000/logs/partitioned/*/*")
        rdd = df.rdd

        record_count = rdd.count()
        print(f"Testing RDD operations with {record_count:,} records")

    except Exception as e:
        print(f"Failed to load test data: {e}")
        spark.stop()
        return []

    # Define RDD operations tests
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
        print(f"\n--- Testing RDD {test_name} ---")

        try:
            start_time = time.time()

            # Execute operation
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

            print(f"Execution time: {execution_time:.2f}s")
            print(f"Processing rate: {record_count / execution_time:,.0f} records/sec")

        except Exception as e:
            print(f"Failed RDD {test_name}: {e}")
            results.append({
                "operation": test_name,
                "error": str(e)
            })

    spark.stop()
    return results


def test_hdfs_performance():
    """Test HDFS read/write performance"""

    print("\n=== Testing HDFS Performance ===")

    hdfs_manager = HDFSManager()

    # Test file operations
    test_data = "Performance test data\n" * 1000
    test_file = "/logs/performance_test.txt"

    results = []

    # Test write performance
    print("Testing HDFS write performance...")
    try:
        start_time = time.time()
        hdfs_manager.client.write(test_file, data=test_data, encoding='utf-8')
        write_time = time.time() - start_time

        data_size = len(test_data.encode('utf-8'))
        write_speed = data_size / write_time / 1024  # KB/s

        results.append({
            "operation": "HDFS Write",
            "time": write_time,
            "data_size_kb": data_size / 1024,
            "speed_kbps": write_speed
        })

        print(f"Write time: {write_time:.2f}s")
        print(f"Write speed: {write_speed:.2f} KB/s")

    except Exception as e:
        print(f"HDFS write test failed: {e}")

    # Test read performance
    print("\nTesting HDFS read performance...")
    try:
        start_time = time.time()

        with hdfs_manager.client.read(test_file, encoding='utf-8') as reader:
            read_data = reader.read()

        read_time = time.time() - start_time

        data_size = len(read_data.encode('utf-8'))
        read_speed = data_size / read_time / 1024  # KB/s

        results.append({
            "operation": "HDFS Read",
            "time": read_time,
            "data_size_kb": data_size / 1024,
            "speed_kbps": read_speed
        })

        print(f"Read time: {read_time:.2f}s")
        print(f"Read speed: {read_speed:.2f} KB/s")

    except Exception as e:
        print(f"HDFS read test failed: {e}")

    # Cleanup
    try:
        hdfs_manager.client.delete(test_file)
    except:
        pass

    return results


def generate_performance_report(load_results, query_results, rdd_results, hdfs_results):
    """Generate comprehensive performance report"""

    print("\n" + "=" * 60)
    print("PERFORMANCE TEST REPORT")
    print("=" * 60)

    print(f"\nTest conducted on: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Data Loading Performance
    print(f"\n--- DATA LOADING PERFORMANCE ---")
    for result in load_results:
        if 'error' not in result:
            print(
                f"{result['test']:20s}: {result['record_count']:>8,} records in {result['load_time']:>6.2f}s ({result['records_per_second']:>8,.0f} rec/s)")

    # Query Performance
    print(f"\n--- QUERY PERFORMANCE ---")
    for result in query_results:
        if 'error' not in result:
            print(
                f"{result['query']:20s}: {result['execution_time']:>6.2f}s ({result['records_per_second']:>8,.0f} rec/s)")

    # RDD Performance
    print(f"\n--- RDD PERFORMANCE ---")
    for result in rdd_results:
        if 'error' not in result:
            print(
                f"{result['operation']:20s}: {result['execution_time']:>6.2f}s ({result['records_per_second']:>8,.0f} rec/s)")

    # HDFS Performance
    print(f"\n--- HDFS PERFORMANCE ---")
    for result in hdfs_results:
        if 'error' not in result:
            print(f"{result['operation']:20s}: {result['time']:>6.2f}s ({result['speed_kbps']:>8.2f} KB/s)")

    # Performance Summary
    print(f"\n--- PERFORMANCE SUMMARY ---")

    # Find best performing operations
    if query_results:
        fastest_query = min([r for r in query_results if 'error' not in r],
                            key=lambda x: x['execution_time'], default=None)
        if fastest_query:
            print(f"Fastest Query: {fastest_query['query']} ({fastest_query['execution_time']:.2f}s)")

    if rdd_results:
        fastest_rdd = min([r for r in rdd_results if 'error' not in r],
                          key=lambda x: x['execution_time'], default=None)
        if fastest_rdd:
            print(f"Fastest RDD Op: {fastest_rdd['operation']} ({fastest_rdd['execution_time']:.2f}s)")

    print(f"\n--- RECOMMENDATIONS ---")
    print("1. Use partitioned data for better performance")
    print("2. Leverage Spark SQL for complex queries")
    print("3. Cache frequently accessed datasets")
    print("4. Optimize partition size for your data")
    print("5. Use appropriate file formats (Parquet for production)")


def main():
    """Main performance testing function"""

    print("=== Big Data Log Analytics Performance Testing ===")
    print("This will test various performance aspects of the system")

    start_time = time.time()

    # Run all performance tests
    print("\nStarting performance tests...")

    load_results = test_data_loading_performance()
    query_results = test_query_performance()
    rdd_results = test_rdd_performance()
    hdfs_results = test_hdfs_performance()

    # Generate report
    generate_performance_report(load_results, query_results, rdd_results, hdfs_results)

    total_time = time.time() - start_time
    print(f"\nTotal testing time: {total_time:.2f} seconds")

    print(f"\n=== Performance Testing Complete ===")
    print("Use these metrics to:")
    print("1. Optimize your Spark configurations")
    print("2. Choose appropriate dataset sizes for demos")
    print("3. Understand system bottlenecks")
    print("4. Plan scaling strategies")


if __name__ == "__main__":
    main()