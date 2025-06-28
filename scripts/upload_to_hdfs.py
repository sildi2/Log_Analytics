#!/usr/bin/env python3
"""
Upload generated logs to HDFS
Following exact course patterns and style
"""

import os
import subprocess
import sys
import time

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.hdfs_operations import HDFSManager


def check_hdfs_running():
    """Check if HDFS is running (course style)"""

    print("=== Checking HDFS Status ===")

    try:
        # Try to connect to NameNode web interface
        hdfs_manager = HDFSManager()
        hdfs_manager.list_files("/")
        print("✅ HDFS is running and accessible")
        return True
    except Exception as e:
        print(f"❌ HDFS is not accessible: {e}")
        print("\nTo start HDFS:")
        print("1. Run: start-dfs.sh")
        print("2. Check web interface: http://localhost:9870/")
        print("3. Verify configuration files are correct")
        return False


def upload_standard_datasets():
    """Upload standard log datasets to HDFS (course style)"""

    print("\n=== Uploading Standard Datasets ===")

    hdfs_manager = HDFSManager()

    # Create HDFS directories
    directories = ["/logs", "/logs/standard"]
    for directory in directories:
        hdfs_manager.create_directory(directory)

    # Upload standard datasets
    standard_files = [
        "logs_small.json",
        "logs_medium.json",
        "logs_large.json"
    ]

    for filename in standard_files:
        if os.path.exists(filename):
            local_path = filename
            hdfs_path = f"/logs/standard/{filename}"

            print(f"\nUploading {filename}...")
            if hdfs_manager.upload_logs(local_path, hdfs_path):
                # Get file info
                hdfs_manager.get_file_info(hdfs_path)
            else:
                print(f"Failed to upload {filename}")
        else:
            print(f"File {filename} not found. Run log_generator.py first.")


def upload_partitioned_datasets():
    """Upload partitioned datasets to HDFS (course style)"""

    print("\n=== Uploading Partitioned Datasets ===")

    hdfs_manager = HDFSManager()

    # Create HDFS directories
    hdfs_manager.create_directory("/logs/partitioned")

    partitioned_path = "data/partitioned"

    if not os.path.exists(partitioned_path):
        print(f"Partitioned data not found at {partitioned_path}")
        print("Run log_generator.py first to generate partitioned data")
        return

    uploaded_count = 0

    # Upload each day's data maintaining partition structure
    for root, dirs, files in os.walk(partitioned_path):
        for file in files:
            if file.endswith('.json'):
                local_file = os.path.join(root, file)

                # Maintain directory structure in HDFS
                relative_path = os.path.relpath(local_file, partitioned_path)
                hdfs_file = f"/logs/partitioned/{relative_path}".replace('\\', '/')

                print(f"Uploading: {relative_path}")
                if hdfs_manager.upload_logs(local_file, hdfs_file):
                    uploaded_count += 1
                else:
                    print(f"Failed to upload {relative_path}")

    print(f"\nUploaded {uploaded_count} partitioned files")


def upload_realistic_datasets():
    """Upload realistic traffic pattern datasets to HDFS (course style)"""

    print("\n=== Uploading Realistic Traffic Datasets ===")

    hdfs_manager = HDFSManager()

    # Create HDFS directories
    hdfs_manager.create_directory("/logs/realistic")

    realistic_path = "data/realistic"

    if not os.path.exists(realistic_path):
        print(f"Realistic data not found at {realistic_path}")
        print("Run log_generator.py first to generate realistic data")
        return

    uploaded_count = 0

    # Upload each day's realistic data
    for root, dirs, files in os.walk(realistic_path):
        for file in files:
            if file.endswith('.json'):
                local_file = os.path.join(root, file)

                # Maintain directory structure in HDFS
                relative_path = os.path.relpath(local_file, realistic_path)
                hdfs_file = f"/logs/realistic/{relative_path}".replace('\\', '/')

                print(f"Uploading: {relative_path}")
                if hdfs_manager.upload_logs(local_file, hdfs_file):
                    uploaded_count += 1
                else:
                    print(f"Failed to upload {relative_path}")

    print(f"\nUploaded {uploaded_count} realistic traffic files")


def verify_uploads():
    """Verify all uploads completed successfully (course style)"""

    print("\n=== Verifying HDFS Uploads ===")

    hdfs_manager = HDFSManager()

    # Check directory structure
    print("\nHDFS directory structure:")
    hdfs_manager.list_files("/logs")

    # Check each subdirectory
    subdirs = ["/logs/standard", "/logs/partitioned", "/logs/realistic"]

    for subdir in subdirs:
        if hdfs_manager.file_exists(subdir):
            print(f"\n{subdir} contents:")
            files = hdfs_manager.list_files(subdir)

            # Count files in subdirectories
            if "partitioned" in subdir or "realistic" in subdir:
                total_files = 0
                try:
                    # Use subprocess for recursive listing
                    result = subprocess.run(
                        ["hdfs", "dfs", "-ls", "-R", subdir],
                        capture_output=True, text=True
                    )
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        total_files = len([line for line in lines if line.startswith('-')])
                        print(f"  Total files: {total_files}")
                except:
                    print("  Could not count files")
        else:
            print(f"\n{subdir}: Not found")


def cleanup_local_files():
    """Clean up local files after successful upload (course style)"""

    print("\n=== Cleanup Options ===")

    response = input("Do you want to remove local log files after upload? (y/N): ").strip().lower()

    if response == 'y':
        # Remove standard files
        standard_files = ["logs_small.json", "logs_medium.json", "logs_large.json"]
        for filename in standard_files:
            if os.path.exists(filename):
                os.remove(filename)
                print(f"Removed {filename}")

        # Remove data directories
        import shutil
        data_dirs = ["data/partitioned", "data/realistic"]
        for data_dir in data_dirs:
            if os.path.exists(data_dir):
                shutil.rmtree(data_dir)
                print(f"Removed {data_dir}")

        print("Local files cleaned up")
    else:
        print("Local files preserved")


def show_hdfs_usage():
    """Show HDFS space usage (course style)"""

    print("\n=== HDFS Usage Summary ===")

    try:
        # Show disk usage
        result = subprocess.run(
            ["hdfs", "dfs", "-du", "-h", "/logs"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print("HDFS space usage:")
            print(result.stdout)

        # Show filesystem status
        result = subprocess.run(
            ["hdfs", "dfsadmin", "-report"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            for line in lines[:10]:  # First 10 lines with summary
                if any(keyword in line for keyword in ['Configured', 'Present', 'DFS Used', 'DFS Remaining']):
                    print(line)

    except Exception as e:
        print(f"Could not get HDFS usage: {e}")


def main():
    """Main upload function following course style"""

    print("=== HDFS Upload Script for Log Analytics Project ===")

    # Check HDFS status
    if not check_hdfs_running():
        print("\nPlease start HDFS and try again")
        return

    start_time = time.time()

    # Upload all datasets
    upload_standard_datasets()
    upload_partitioned_datasets()
    upload_realistic_datasets()

    # Verify uploads
    verify_uploads()

    # Show usage
    show_hdfs_usage()

    # Cleanup option
    cleanup_local_files()

    end_time = time.time()

    print(f"\n=== Upload Complete ===")
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print("\nNext steps:")
    print("1. Run analytics: python analytics/log_analytics.py")
    print("2. Check HDFS web UI: http://localhost:9870/")
    print("3. View uploaded files: hdfs dfs -ls -R /logs/")


if __name__ == "__main__":
    main()