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
tool_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(tool_dir, '..'))
from utils.hdfs_operations import HDFSManager


def check_hdfs_running():
    """Check if HDFS is running (course style)"""

    print("=== Checking HDFS Status ===")

    try:
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

    # Create HDFS directories for raw logs
    for directory in ["/logs", "/logs/raw"]:
        hdfs_manager.create_directory(directory)

    # Upload unpartitioned JSON into /logs/raw
    standard_files = [
        "logs_small.json",
        "logs_medium.json",
        "logs_large.json"
    ]

    for filename in standard_files:
        if os.path.exists(filename):
            local_path = filename
            hdfs_path = f"/logs/raw/{filename}"

            print(f"\nUploading {filename} to raw...")
            if hdfs_manager.upload_logs(local_path, hdfs_path):
                hdfs_manager.get_file_info(hdfs_path)
            else:
                print(f"Failed to upload {filename}")
        else:
            print(f"File {filename} not found. Run log_generator.py first.")


def upload_partitioned_datasets():
    """Upload partitioned datasets to HDFS (course style)"""

    print("\n=== Uploading Partitioned Datasets ===")

    hdfs_manager = HDFSManager()
    hdfs_manager.create_directory("/logs/partitioned")

    partitioned_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'partitioned')
    partitioned_path = os.path.normpath(partitioned_path)

    if not os.path.exists(partitioned_path):
        print(f"Partitioned data not found at {partitioned_path}")
        print("Run log_generator.py first to generate partitioned data")
        return

    uploaded_count = 0
    for root, dirs, files in os.walk(partitioned_path):
        for file in files:
            if file.endswith('.json'):
                local_file = os.path.join(root, file)
                # Compute relative path and normalize separators
                relative = os.path.relpath(local_file, partitioned_path)
                normalized = relative.replace('\\', '/')
                hdfs_file = f"/logs/partitioned/{normalized}"
                print(f"Uploading partitioned: {normalized}")
                if hdfs_manager.upload_logs(local_file, hdfs_file):
                    uploaded_count += 1
                else:
                    print(f"Failed to upload {normalized}")

    print(f"\nUploaded {uploaded_count} partitioned files")


def upload_realistic_datasets():
    """Upload realistic traffic pattern datasets to HDFS (course style)"""

    print("\n=== Uploading Realistic Traffic Datasets ===")

    hdfs_manager = HDFSManager()
    hdfs_manager.create_directory("/logs/realistic")

    realistic_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'realistic')
    realistic_path = os.path.normpath(realistic_path)

    if not os.path.exists(realistic_path):
        print(f"Realistic data not found at {realistic_path}")
        print("Run log_generator.py first to generate realistic data")
        return

    uploaded_count = 0
    for root, dirs, files in os.walk(realistic_path):
        for file in files:
            if file.endswith('.json'):
                local_file = os.path.join(root, file)
                relative = os.path.relpath(local_file, realistic_path)
                normalized = relative.replace('\\', '/')
                hdfs_file = f"/logs/realistic/{normalized}"
                print(f"Uploading realistic: {normalized}")
                if hdfs_manager.upload_logs(local_file, hdfs_file):
                    uploaded_count += 1
                else:
                    print(f"Failed to upload {normalized}")

    print(f"\nUploaded {uploaded_count} realistic traffic files")


def verify_uploads():
    """Verify all uploads completed successfully (course style)"""

    print("\n=== Verifying HDFS Uploads ===")

    hdfs_manager = HDFSManager()
    print("\n/logs structure:")
    hdfs_manager.list_files("/logs")

    for sub in ["/logs/raw", "/logs/partitioned", "/logs/realistic"]:
        if hdfs_manager.file_exists(sub):
            print(f"\n{sub} contents:")
            hdfs_manager.list_files(sub)
        else:
            print(f"\n{sub}: Not found")


def cleanup_local_files():
    """Clean up local files after successful upload (course style)"""

    print("\n=== Cleanup Options ===")
    resp = input("Remove local logs after upload? (y/N): ").strip().lower()
    if resp == 'y':
        for f in ["logs_small.json","logs_medium.json","logs_large.json"]:
            if os.path.exists(f):
                os.remove(f)
                print(f"Removed {f}")
        import shutil
        for d in ["data/partitioned","data/realistic"]:
            if os.path.exists(d):
                shutil.rmtree(d)
                print(f"Removed {d}")
        print("Local files cleaned up")
    else:
        print("Local files preserved")


def show_hdfs_usage():
    """Show HDFS space usage (course style)"""

    print("\n=== HDFS Usage Summary ===")
    try:
        du = subprocess.run(["hdfs","dfs","-du","-h","/logs"],capture_output=True,text=True)
        if du.returncode == 0:
            print("HDFS space usage:\n" + du.stdout)
        rep = subprocess.run(["hdfs","dfsadmin","-report"],capture_output=True,text=True)
        if rep.returncode == 0:
            for line in rep.stdout.splitlines()[:10]:
                if any(k in line for k in ['Configured','DFS Used','DFS Remaining']):
                    print(line)
    except Exception as e:
        print(f"Could not get HDFS usage: {e}")


def main():
    print("=== HDFS Upload Script for Log Analytics Project ===")
    if not check_hdfs_running():
        print("\nStart HDFS and try again")
        return
    start = time.time()
    upload_standard_datasets()
    upload_partitioned_datasets()
    upload_realistic_datasets()
    verify_uploads()
    show_hdfs_usage()
    cleanup_local_files()
    elapsed = time.time() - start
    print(f"\n=== Upload Complete (in {elapsed:.2f}s) ===")
    print("Next steps:")
    print("1. python analytics/log_analytics.py")
    print("2. HDFS Web UI: http://localhost:9870/")
    print("3. hdfs dfs -ls -R /logs/")

if __name__ == "__main__":
    main()