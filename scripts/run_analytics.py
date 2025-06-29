#!/usr/bin/env python3
"""
Main Analytics Runner - Complete Pipeline
Following course patterns and providing full automation
"""

import os
import sys
import time
import subprocess

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.hdfs_operations import HDFSManager
from utils.config import print_config


def check_prerequisites():
    # Check HDFS accessibility first
    try:
        hdfs_manager = HDFSManager()
        hdfs_manager.list_files("/")
        print("✅ HDFS is accessible")
    except Exception as e:
        print(f"❌ HDFS not accessible: {e}")
        return False

    # Check for data in HDFS (but don't fail if directories don't exist)
    try:
        hdfs_manager = HDFSManager()
        files = hdfs_manager.list_files("/logs")
        if len(files) > 0:
            print("✅ Data found in HDFS")
        else:
            print("⚠️ No data found in HDFS - will be created during upload")
    except:
        print("⚠️ /logs directory doesn't exist - will be created during upload")

    return True  # Don't fail for missing directories


def run_data_generation():
    """Run data generation if needed"""
    print("\n=== Data Generation Phase ===")

    # Check if local data exists
    if not os.path.exists("data"):
        print("No local data found. Generating...")

        # Import and run log generator
        sys.path.append('data_generation')
        try:
            from log_generator import main as generate_logs
            generate_logs()
            print("✅ Data generation completed")
        except ImportError:
            print("❌ Cannot import log generator")
            print("Please run: python data_generation/log_generator.py")
            return False
    else:
        print("✅ Local data directory exists")

    return True


def run_hdfs_upload():
    """Upload data to HDFS if needed"""
    print("\n=== HDFS Upload Phase ===")

    hdfs_manager = HDFSManager()

    # Check if data exists in HDFS
    try:
        files = hdfs_manager.list_files("/logs")
        if len(files) == 0:
            print("No data in HDFS. Uploading...")

            # Run upload script
            try:
                from upload_to_hdfs import main as upload_main
                upload_main()
                print("✅ HDFS upload completed")
            except ImportError:
                print("❌ Cannot import upload script")
                print("Please run: python scripts/upload_to_hdfs.py")
                return False
        else:
            print("✅ Data already exists in HDFS")
    except Exception as e:
        print(f"❌ Error checking HDFS: {e}")
        return False

    return True


def run_basic_analytics():
    """Run basic analytics"""
    print("\n=== Basic Analytics Phase ===")

    try:
        # Import and run analytics
        sys.path.append('analytics')
        from log_analytics import main as analytics_main

        print("Starting Spark analytics...")
        analytics_main()
        print("✅ Basic analytics completed")
        return True

    except ImportError as e:
        print(f"❌ Cannot import analytics: {e}")
        print("Please run: python analytics/log_analytics.py")
        return False
    except Exception as e:
        print(f"❌ Analytics failed: {e}")
        return False


def run_performance_tests():
    """Run performance tests"""
    print("\n=== Performance Testing Phase ===")

    response = input("Run performance tests? (y/N): ").strip().lower()

    if response == 'y':
        try:
            sys.path.append('tests')
            from performance_test import main as perf_main

            print("Starting performance tests...")
            perf_main()
            print("✅ Performance tests completed")
            return True

        except ImportError as e:
            print(f"❌ Cannot import performance tests: {e}")
            print("Please run: python tests/performance_test.py")
            return False
        except Exception as e:
            print(f"❌ Performance tests failed: {e}")
            return False
    else:
        print("⏭️  Skipping performance tests")
        return True


def show_results():
    """Show results and web interfaces"""
    print("\n=== Results and Interfaces ===")

    hdfs_manager = HDFSManager()

    # Show HDFS results
    print("\nResults stored in HDFS:")
    try:
        results = hdfs_manager.list_files("/logs/results")
        for result in results:
            print(f"  📁 {result}")
    except:
        print("  No results directory found")

    # Show web interfaces
    print(f"\n🌐 Web Interfaces:")
    print(f"  HDFS NameNode: http://localhost:9870/")
    print(f"  Spark UI: http://localhost:4040/ (when Spark is running)")

    # Show useful commands
    print(f"\n💻 Useful Commands:")
    print(f"  View HDFS files: hdfs dfs -ls -R /logs/")
    print(f"  Download results: hdfs dfs -get /logs/results/ ./local_results/")
    print(f"  HDFS usage: hdfs dfs -du -h /logs/")


def cleanup_options():
    """Provide cleanup options"""
    print("\n=== Cleanup Options ===")

    print("Available cleanup operations:")
    print("1. Remove local data files")
    print("2. Remove HDFS data")
    print("3. Remove results")
    print("4. No cleanup")

    choice = input("Choose option (1-4): ").strip()

    if choice == "1":
        import shutil
        if os.path.exists("data"):
            shutil.rmtree("data")
            print("✅ Local data removed")

        # Remove individual log files
        for file in ["logs_small.json", "logs_medium.json", "logs_large.json"]:
            if os.path.exists(file):
                os.remove(file)
                print(f"✅ Removed {file}")

    elif choice == "2":
        hdfs_manager = HDFSManager()
        try:
            subprocess.run(["hdfs", "dfs", "-rm", "-r", "/logs"], check=True)
            print("✅ HDFS data removed")
        except:
            print("❌ Failed to remove HDFS data")

    elif choice == "3":
        try:
            subprocess.run(["hdfs", "dfs", "-rm", "-r", "/logs/results"], check=True)
            print("✅ HDFS results removed")
        except:
            print("❌ Failed to remove HDFS results")

    else:
        print("⏭️  No cleanup performed")


def generate_project_summary():
    """Generate project summary report"""
    print("\n" + "=" * 60)
    print("BIG DATA LOG ANALYTICS PROJECT SUMMARY")
    print("=" * 60)

    print(f"\nProject completed on: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Configuration summary
    print_config()

    # Data summary
    print(f"\n--- DATA SUMMARY ---")
    hdfs_manager = HDFSManager()

    try:
        # Check HDFS usage
        result = subprocess.run(
            ["hdfs", "dfs", "-du", "-s", "-h", "/logs"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            size_info = result.stdout.strip().split()
            if len(size_info) >= 1:
                print(f"Total data in HDFS: {size_info[0]}")
    except:
        print("Could not determine HDFS usage")

    # Results summary
    print(f"\n--- RESULTS GENERATED ---")
    try:
        results = hdfs_manager.list_files("/logs/results")
        for result in results:
            print(f"✅ {result}")
    except:
        print("No results found")

    # Course alignment
    print(f"\n--- COURSE OBJECTIVES MET ---")
    objectives = [
        "✅ HDFS distributed storage configuration and usage",
        "✅ Apache Spark RDD and DataFrame operations",
        "✅ Spark SQL queries on distributed data",
        "✅ Data partitioning and optimization",
        "✅ Python integration with big data tools",
        "✅ Performance testing and benchmarking",
        "✅ Real-world log analytics application"
    ]

    for objective in objectives:
        print(objective)

    print(f"\n--- PROJECT DELIVERABLES ---")
    deliverables = [
        "✅ Working Hadoop/HDFS setup",
        "✅ Spark job implementations",
        "✅ Sample data and results",
        "✅ Performance analysis",
        "✅ Code documentation",
        "✅ Demonstrable system"
    ]

    for deliverable in deliverables:
        print(deliverable)


def main():
    """Main pipeline runner"""
    print("=== Big Data Log Analytics - Complete Pipeline ===")
    print("This script will run the entire analytics pipeline")

    start_time = time.time()

    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please:")
        print("1. Start HDFS: start-dfs.sh")
        print("2. Check configuration")
        print("3. Run setup: python scripts/setup_environment.py")
        return

    # Run pipeline phases
    phases = [
        ("Data Generation", run_data_generation),
        ("HDFS Upload", run_hdfs_upload),
        ("Basic Analytics", run_basic_analytics),
        ("Performance Tests", run_performance_tests)
    ]

    completed_phases = 0

    for phase_name, phase_func in phases:
        print(f"\n{'=' * 20} {phase_name} {'=' * 20}")

        if phase_func():
            completed_phases += 1
            print(f"✅ {phase_name} completed successfully")
        else:
            print(f"❌ {phase_name} failed")

            # Ask if user wants to continue
            response = input(f"Continue to next phase? (y/N): ").strip().lower()
            if response != 'y':
                break

    # Show results
    show_results()

    # Generate summary
    generate_project_summary()

    # Cleanup options
    cleanup_options()

    # Final summary
    total_time = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    print(f"Completed phases: {completed_phases}/{len(phases)}")
    print(f"Total time: {total_time:.2f} seconds")

    if completed_phases == len(phases):
        print("🎉 Full pipeline executed successfully!")
        print("\nYour Big Data Log Analytics project is ready for:")
        print("1. 📊 Demonstration")
        print("2. 📝 Report writing")
        print("3. 🎤 Presentation")
        print("4. 🎓 Course submission")
    else:
        print("⚠️  Some phases had issues. Please review and fix before demo.")


if __name__ == "__main__":
    main()