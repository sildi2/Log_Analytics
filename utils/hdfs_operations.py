#!/usr/bin/env python3
"""
HDFS Operations for Log Analytics Project
Following exact course patterns and style
"""

import subprocess
import os
from hdfs import InsecureClient


class HDFSManager:
    """HDFS operations manager following course style"""

    def __init__(self, namenode_url="http://localhost:9870", user="hadoop"):
        """Initialize HDFS client (course pattern)"""
        self.client = InsecureClient(namenode_url, user=user)
        self.namenode_url = namenode_url

    def upload_logs(self, local_path, hdfs_path):
        """Upload log files to HDFS (course style)"""
        try:
            self.client.upload(hdfs_path, local_path, overwrite=True)
            print(f"Uploaded {local_path} to {hdfs_path}")
            return True
        except Exception as e:
            print(f"Upload failed: {e}")
            return False

    def list_files(self, path="/"):
        """List files in HDFS directory (course pattern)"""
        try:
            files = self.client.list(path)
            print(f"Files in {path}:")
            for file in files:
                print(f"  - {file}")
            return files
        except Exception as e:
            print(f"List failed: {e}")
            return []

    def download_results(self, hdfs_path, local_path):
        """Download results from HDFS (course style)"""
        try:
            self.client.download(hdfs_path, local_path, overwrite=True)
            print(f"Downloaded {hdfs_path} to {local_path}")
            return True
        except Exception as e:
            print(f"Download failed: {e}")
            return False

    def create_directory(self, hdfs_path):
        """Create directory in HDFS (course pattern)"""
        try:
            self.client.makedirs(hdfs_path)
            print(f"Created directory: {hdfs_path}")
            return True
        except Exception as e:
            print(f"Directory creation failed: {e}")
            return False

    def file_exists(self, hdfs_path):
        """Check if file exists in HDFS (course style)"""
        try:
            status = self.client.status(hdfs_path, strict=False)
            return status is not None
        except:
            return False

    def get_file_info(self, hdfs_path):
        """Get file information (course pattern)"""
        try:
            status = self.client.status(hdfs_path)
            print(f"File info for {hdfs_path}:")
            print(f"  Size: {status['length']} bytes")
            print(f"  Type: {status['type']}")
            print(f"  Modified: {status['modificationTime']}")
            return status
        except Exception as e:
            print(f"Failed to get file info: {e}")
            return None


def setup_hdfs_directories():
    """Setup HDFS directory structure for project (course style)"""

    print("=== Setting up HDFS directories ===")

    hdfs_manager = HDFSManager()

    # Create directory structure
    directories = [
        "/logs",
        "/logs/raw",
        "/logs/processed",
        "/logs/results"
    ]

    for directory in directories:
        hdfs_manager.create_directory(directory)

    # List root directory
    print("\nHDFS directory structure:")
    hdfs_manager.list_files("/")
    hdfs_manager.list_files("/logs")


def test_hdfs_connection():
    """Test HDFS connection (course style)"""

    print("=== Testing HDFS Connection ===")

    try:
        hdfs_manager = HDFSManager()

        # Test basic operations
        print("1. Testing directory listing...")
        files = hdfs_manager.list_files("/")

        print("2. Testing directory creation...")
        hdfs_manager.create_directory("/test")

        print("3. Testing file operations...")
        test_data = "Hello HDFS from Python!"

        # Write test file
        hdfs_manager.client.write("/test/hello.txt", data=test_data, encoding='utf-8')
        print("   Test file written successfully")

        # Read test file
        with hdfs_manager.client.read("/test/hello.txt", encoding='utf-8') as reader:
            content = reader.read()
            print(f"   Test file content: {content}")

        print("✅ HDFS connection test successful!")
        return True

    except Exception as e:
        print(f"❌ HDFS connection test failed: {e}")
        return False


if __name__ == "__main__":
    """Main function for testing (course style)"""

    print("=== HDFS Operations Testing ===")

    # Test connection
    if test_hdfs_connection():
        # Setup directories
        setup_hdfs_directories()
        print("\n=== HDFS setup complete! ===")
    else:
        print("\n=== Please check HDFS configuration ===")
        print("Make sure HDFS is running: start-dfs.sh")
        print("Check NameNode web interface: http://localhost:9870/")