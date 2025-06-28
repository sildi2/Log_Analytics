#!/usr/bin/env python3
"""
Configuration settings for Log Analytics Project
Following course patterns and style
"""

import os

# HDFS Configuration (matching your setup)
HDFS_CONFIG = {
    'namenode_url': 'http://localhost:9870',
    'namenode_rpc': 'hdfs://localhost:9000',
    'user': 'hadoop',
    'replication_factor': 1
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'LogAnalytics',
    'master': 'local[*]',
    'driver_memory': '2g',
    'executor_memory': '1g',
    'sql_adaptive_enabled': True,
    'sql_adaptive_coalesce_partitions_enabled': True
}

# Data Paths
DATA_PATHS = {
    'local_data_dir': 'data',
    'hdfs_base_path': '/logs',
    'hdfs_raw_path': '/logs/raw',
    'hdfs_processed_path': '/logs/processed',
    'hdfs_results_path': '/logs/results',
    'hdfs_standard_path': '/logs/standard',
    'hdfs_partitioned_path': '/logs/partitioned',
    'hdfs_realistic_path': '/logs/realistic'
}

# Log Generation Settings
LOG_GENERATION = {
    'default_entries_per_day': 2000,
    'default_days': 7,
    'small_dataset_size': 1000,
    'medium_dataset_size': 10000,
    'large_dataset_size': 50000,
    'realistic_days': 14
}

# Analytics Settings
ANALYTICS_CONFIG = {
    'top_n_endpoints': 10,
    'top_n_users': 20,
    'top_n_sessions': 15,
    'top_n_ips': 10,
    'partition_size_mb': 128
}

# Course-specific settings
COURSE_CONFIG = {
    'instructor': 'Marco Maggini',
    'course_name': 'Big Data',
    'project_title': 'Real-time Log Analytics with Apache Spark and HDFS',
    'team_size': 2,
    'demo_port': 4040,
    'hdfs_web_port': 9870
}


def get_hdfs_path(path_type):
    """Get HDFS path by type (course helper function)"""
    return DATA_PATHS.get(f'hdfs_{path_type}_path', DATA_PATHS['hdfs_base_path'])


def get_spark_config():
    """Get Spark configuration (course style)"""
    return SPARK_CONFIG


def get_log_generation_config():
    """Get log generation configuration"""
    return LOG_GENERATION


def print_config():
    """Print current configuration (course style)"""
    print("=== Log Analytics Project Configuration ===")
    print(f"Course: {COURSE_CONFIG['course_name']}")
    print(f"Instructor: {COURSE_CONFIG['instructor']}")
    print(f"Project: {COURSE_CONFIG['project_title']}")
    print(f"Team Size: {COURSE_CONFIG['team_size']} students")

    print(f"\nHDFS Configuration:")
    print(f"  NameNode Web UI: {HDFS_CONFIG['namenode_url']}")
    print(f"  NameNode RPC: {HDFS_CONFIG['namenode_rpc']}")
    print(f"  User: {HDFS_CONFIG['user']}")
    print(f"  Replication: {HDFS_CONFIG['replication_factor']}")

    print(f"\nSpark Configuration:")
    print(f"  Application: {SPARK_CONFIG['app_name']}")
    print(f"  Master: {SPARK_CONFIG['master']}")
    print(f"  Driver Memory: {SPARK_CONFIG['driver_memory']}")
    print(f"  Web UI: http://localhost:{COURSE_CONFIG['demo_port']}")

    print(f"\nData Paths:")
    for key, path in DATA_PATHS.items():
        print(f"  {key}: {path}")


if __name__ == "__main__":
    print_config()