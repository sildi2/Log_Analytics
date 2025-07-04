#!/usr/bin/env python3

import os
import sys
import time
import subprocess

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.hdfs_operations import HDFSManager


def check_prerequisites():
    print("=== System Prerequisites Check ===")

    # Check HDFS
    try:
        hdfs_manager = HDFSManager()
        hdfs_manager.list_files("/")
        print(" HDFS is accessible")
    except Exception as e:
        print(f" HDFS not accessible: {e}")
        return False

    # Check for data
    try:
        files = hdfs_manager.list_files("/logs")
        if len(files) > 0:
            print(" Data found in HDFS")
        else:
            print("  No data in HDFS - will be created")
    except:
        print("  /logs directory doesn't exist - will be created")

    return True


def run_data_generation():
    """Generate sample data if needed"""

    print("\n=== Data Generation ===")

    if not os.path.exists("data") and not any(
            os.path.exists(f) for f in ["logs_small.json", "logs_medium.json", "logs_large.json"]):
        print(" Generating sample log data...")

        try:
            sys.path.append('data_generation')
            from log_generator import main as generate_logs
            generate_logs()
            print(" Sample data generated")
        except ImportError:
            print(" Cannot import log generator")
            print("Please run: python data_generation/log_generator.py")
            return False
    else:
        print(" Sample data available")

    return True


def run_hdfs_upload():
    """Upload data to HDFS if needed"""

    print("\n=== HDFS Data Upload ===")

    hdfs_manager = HDFSManager()

    try:
        files = hdfs_manager.list_files("/logs/raw")
        if len(files) == 0:
            print("🔄 Uploading data to HDFS...")

            try:
                from upload_to_hdfs import main as upload_main
                upload_main()
                print(" Data uploaded to HDFS")
            except ImportError:
                print(" Cannot import upload script")
                print("Please run: python scripts/upload_to_hdfs.py")
                return False
        else:
            print(" Data already in HDFS")
    except Exception as e:
        print(f" HDFS upload check failed: {e}")
        return False

    return True


def run_analytics():
    print("\n=== Running Analytics ===")

    try:
        sys.path.append('analytics')
        from log_analytics import main as analytics_main

        print("🚀 Starting Spark analytics pipeline...")
        analytics_main()
        print(" Analytics completed successfully")
        return True

    except ImportError as e:
        print(f" Cannot import analytics: {e}")
        print("Please run: python analytics/log_analytics.py")
        return False
    except Exception as e:
        print(f" Analytics execution failed: {e}")
        return False


def show_results():
    print("\n=== Results & Access Points ===")

    # Show system interfaces
    print(f"\n System Interfaces:")
    print(f"   HDFS Web UI: http://localhost:9870/")

    # Show useful commands
    print(f"\n Useful Commands:")
    print(f"   View HDFS: hdfs dfs -ls -R /logs/")
    print(f"   HDFS usage: hdfs dfs -du -h /logs/")


def main():
    """Main pipeline execution"""

    print("=== Big Data Log Analytics - Core Pipeline ===")
    print("Apache Spark + HDFS distributed processing system\n")

    start_time = time.time()

    # Check prerequisites
    if not check_prerequisites():
        return

    # Run core pipeline phases
    phases = [
        ("Data Generation", run_data_generation),
        ("HDFS Upload", run_hdfs_upload),
        ("Analytics Execution", run_analytics)
    ]

    completed_phases = 0

    for phase_name, phase_func in phases:
        print(f"\n{'=' * 50}")
        print(f"Phase: {phase_name}")
        print(f"{'=' * 50}")

        if phase_func():
            completed_phases += 1
            print(f" {phase_name} - SUCCESS")
        else:
            print(f" {phase_name} - FAILED")

            response = input(f"Continue to next phase? (y/N): ").strip().lower()
            if response != 'y':
                break

    # Show results
    show_results()

    # Final status
    total_time = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"PIPELINE EXECUTION COMPLETE")
    print(f"{'=' * 60}")
    print(f"Phases completed: {completed_phases}/{len(phases)}")
    print(f"Total execution time: {total_time:.2f} seconds")

    if completed_phases == len(phases):
        print(" All phases executed successfully!")
    else:
        print(" ** Some phases incomplete. Review logs above.")


if __name__ == "__main__":
    main()
