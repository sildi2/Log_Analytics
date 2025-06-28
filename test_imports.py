#!/usr/bin/env python3
"""
Quick test to verify all packages are installed correctly
"""


def test_imports():
    """Test all required imports"""

    print("=== Testing Package Imports ===")

    try:
        from hdfs import InsecureClient
        print("✅ hdfs package imported successfully")
    except ImportError as e:
        print(f"❌ hdfs import failed: {e}")
        return False

    try:
        import pyspark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, avg
        print("✅ pyspark package imported successfully")
    except ImportError as e:
        print(f"❌ pyspark import failed: {e}")
        return False

    try:
        import pandas as pd
        print("✅ pandas package imported successfully")
    except ImportError as e:
        print(f"❌ pandas import failed: {e}")
        return False

    try:
        import numpy as np
        print("✅ numpy package imported successfully")
    except ImportError as e:
        print(f"❌ numpy import failed: {e}")
        return False

    return True


def test_hdfs_client():
    """Test HDFS client creation"""

    print("\n=== Testing HDFS Client ===")

    try:
        from hdfs import InsecureClient

        # Create client (like in your course materials)
        hdfs_client = InsecureClient('http://localhost:9870/', user='hadoop')
        print("✅ HDFS client created successfully")
        print(f"   Client URL: {hdfs_client.url}")
        print(f"   User: {hdfs_client.user}")

        return True

    except Exception as e:
        print(f"❌ HDFS client creation failed: {e}")
        return False


def test_spark_session():
    """Test Spark session creation"""

    print("\n=== Testing Spark Session ===")

    try:
        import pyspark
        from pyspark.sql import SparkSession

        # Create simple Spark session
        spark = SparkSession.builder \
            .appName("TestApp") \
            .master("local[1]") \
            .getOrCreate()

        print("✅ Spark session created successfully")
        print(f"   Spark version: {spark.version}")
        print(f"   Application name: {spark.sparkContext.appName}")

        # Stop session
        spark.stop()
        print("✅ Spark session stopped successfully")

        return True

    except Exception as e:
        print(f"❌ Spark session creation failed: {e}")
        return False


if __name__ == "__main__":
    print("=== Package Installation Verification ===")

    all_good = True

    # Test imports
    if not test_imports():
        all_good = False

    # Test HDFS client
    if not test_hdfs_client():
        all_good = False

    # Test Spark
    if not test_spark_session():
        all_good = False

    print(f"\n{'=' * 50}")
    if all_good:
        print("🎉 ALL TESTS PASSED!")
        print("✅ Your environment is ready for the Big Data project")
        print("\nNext steps:")
        print("1. Make sure HDFS is running: start-dfs.sh")
        print("2. Run: python log_generator.py")
        print("3. Run: python scripts/upload_to_hdfs.py")
        print("4. Run: python analytics/log_analytics.py")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
    print(f"{'=' * 50}")