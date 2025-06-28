#!/usr/bin/env python3
"""
Environment Setup Script for Log Analytics Project
Following course patterns and checking all dependencies
"""

import os
import subprocess
import sys
import importlib
import platform


def check_python_version():
    """Check Python version (course requirement)"""
    print("=== Checking Python Version ===")

    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")

    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ required")
        return False
    else:
        print("✅ Python version OK")
        return True


def check_java_version():
    """Check Java version (Spark requirement)"""
    print("\n=== Checking Java Version ===")

    try:
        result = subprocess.run(['java', '-version'],
                                capture_output=True, text=True, stderr=subprocess.STDOUT)

        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0]
            print(f"Java version: {java_version}")

            # Check for Java 8 or 11
            if 'version "1.8' in java_version or 'version "8' in java_version:
                print("✅ Java 8 detected - compatible with Spark")
                return True
            elif 'version "11' in java_version:
                print("✅ Java 11 detected - compatible with Spark")
                return True
            else:
                print("⚠️  Java version may not be optimal for Spark")
                print("   Recommended: Java 8 or 11")
                return True  # Allow but warn
        else:
            print("❌ Java not found")
            return False

    except FileNotFoundError:
        print("❌ Java not found in PATH")
        print("   Please install Java 8 or 11")
        return False


def check_hadoop_installation():
    """Check Hadoop/HDFS installation"""
    print("\n=== Checking Hadoop Installation ===")

    try:
        # Check hadoop command
        result = subprocess.run(['hadoop', 'version'],
                                capture_output=True, text=True)

        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            print(f"Hadoop version: {version_line}")
            print("✅ Hadoop installation found")

            # Check HDFS command
            hdfs_result = subprocess.run(['hdfs', 'version'],
                                         capture_output=True, text=True)
            if hdfs_result.returncode == 0:
                print("✅ HDFS command available")
                return True
            else:
                print("⚠️  HDFS command not working")
                return False
        else:
            print("❌ Hadoop not found")
            return False

    except FileNotFoundError:
        print("❌ Hadoop not found in PATH")
        print("   Please install Hadoop and add to PATH")
        return False


def check_spark_installation():
    """Check Spark installation"""
    print("\n=== Checking Spark Installation ===")

    # Check SPARK_HOME environment variable
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home:
        print(f"SPARK_HOME: {spark_home}")

        # Check if spark-submit exists
        spark_submit = os.path.join(spark_home, 'bin', 'spark-submit')
        if platform.system() == 'Windows':
            spark_submit += '.cmd'

        if os.path.exists(spark_submit):
            print("✅ Spark installation found")

            # Try to get version
            try:
                result = subprocess.run([spark_submit, '--version'],
                                        capture_output=True, text=True, stderr=subprocess.STDOUT)

                for line in result.stdout.split('\n'):
                    if 'version' in line.lower() and 'spark' in line.lower():
                        print(f"Spark version: {line.strip()}")
                        break

            except:
                print("⚠️  Could not determine Spark version")

            return True
        else:
            print("❌ spark-submit not found in SPARK_HOME/bin")
            return False
    else:
        print("❌ SPARK_HOME environment variable not set")
        print("   Please set SPARK_HOME to your Spark installation directory")
        return False


def check_python_packages():
    """Check required Python packages"""
    print("\n=== Checking Python Packages ===")

    required_packages = [
        'pyspark',
        'hdfs',
        'pandas',
        'numpy'
    ]

    missing_packages = []

    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"✅ {package} installed")
        except ImportError:
            print(f"❌ {package} not installed")
            missing_packages.append(package)

    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Install with: pip install " + " ".join(missing_packages))
        return False
    else:
        print("✅ All required packages installed")
        return True


def check_hdfs_running():
    """Check if HDFS is running"""
    print("\n=== Checking HDFS Status ===")

    try:
        # Try to list HDFS root directory
        result = subprocess.run(['hdfs', 'dfs', '-ls', '/'],
                                capture_output=True, text=True)

        if result.returncode == 0:
            print("✅ HDFS is running and accessible")
            return True
        else:
            print("❌ HDFS not accessible")
            print("   Error:", result.stderr.strip())
            print("   Try: start-dfs.sh")
            return False

    except Exception as e:
        print(f"❌ Cannot check HDFS status: {e}")
        print("   Make sure Hadoop is installed and HDFS is formatted")
        return False


def check_hdfs_configuration():
    """Check HDFS configuration files"""
    print("\n=== Checking HDFS Configuration ===")

    hadoop_home = os.environ.get('HADOOP_HOME')
    if not hadoop_home:
        print("❌ HADOOP_HOME not set")
        return False

    config_dir = os.path.join(hadoop_home, 'etc', 'hadoop')

    config_files = {
        'core-site.xml': 'fs.defaultFS',
        'hdfs-site.xml': 'dfs.replication'
    }

    all_good = True

    for config_file, key_setting in config_files.items():
        config_path = os.path.join(config_dir, config_file)

        if os.path.exists(config_path):
            print(f"✅ {config_file} found")

            # Read and check basic content
            try:
                with open(config_path, 'r') as f:
                    content = f.read()
                    if key_setting in content:
                        print(f"   Contains {key_setting} configuration")
                    else:
                        print(f"   ⚠️  Missing {key_setting} configuration")
            except:
                print(f"   ⚠️  Could not read {config_file}")

        else:
            print(f"❌ {config_file} not found at {config_path}")
            all_good = False

    return all_good


def create_project_directories():
    """Create project directory structure"""
    print("\n=== Creating Project Directories ===")

    directories = [
        'data',
        'data/partitioned',
        'data/realistic',
        'results',
        'utils',
        'scripts',
        'analytics',
        'tests',
        'documentation'
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created directory: {directory}")

    return True


def create_requirements_file():
    """Create requirements.txt file"""
    print("\n=== Creating requirements.txt ===")

    requirements = [
        "pyspark>=3.3.0",
        "hdfs>=2.7.0",
        "pandas>=1.5.0",
        "numpy>=1.21.0"
    ]

    with open('requirements.txt', 'w') as f:
        for req in requirements:
            f.write(req + '\n')

    print("✅ requirements.txt created")
    return True


def show_next_steps():
    """Show next steps for setup"""
    print("\n=== Next Steps ===")
    print("1. Generate log data:")
    print("   python data_generation/log_generator.py")
    print()
    print("2. Upload data to HDFS:")
    print("   python scripts/upload_to_hdfs.py")
    print()
    print("3. Run analytics:")
    print("   python analytics/log_analytics.py")
    print()
    print("4. Check web interfaces:")
    print("   HDFS: http://localhost:9870/")
    print("   Spark: http://localhost:4040/ (when running)")


def main():
    """Main setup function"""
    print("=== Big Data Log Analytics Environment Setup ===")
    print("Checking system requirements and dependencies...")

    checks = [
        ("Python Version", check_python_version()),
        ("Java Installation", check_java_version()),
        ("Hadoop Installation", check_hadoop_installation()),
        ("Spark Installation", check_spark_installation()),
        ("Python Packages", check_python_packages()),
        ("HDFS Configuration", check_hdfs_configuration()),
        ("HDFS Running", check_hdfs_running())
    ]

    print(f"\n=== Environment Check Summary ===")
    passed = 0
    for check_name, result in checks:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{check_name}: {status}")
        if result:
            passed += 1

    print(f"\nPassed: {passed}/{len(checks)} checks")

    if passed == len(checks):
        print("\n🎉 Environment setup complete!")
        create_project_directories()
        create_requirements_file()
        show_next_steps()
    else:
        print("\n⚠️  Some checks failed. Please resolve issues before proceeding.")
        print("\nCommon solutions:")
        print("- Install Java 8 or 11")
        print("- Install Hadoop and set HADOOP_HOME")
        print("- Install Spark and set SPARK_HOME")
        print("- Run: pip install pyspark hdfs pandas numpy")
        print("- Format HDFS: hdfs namenode -format")
        print("- Start HDFS: start-dfs.sh")


if __name__ == "__main__":
    main()