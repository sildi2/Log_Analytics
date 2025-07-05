#!/usr/bin/env python3

import json
import random
from datetime import datetime, timedelta
import os


def generate_log_entry(timestamp=None):
    if timestamp is None:
        timestamp = datetime.now()

    # Sample data for realistic logs
    endpoints = [
        "/api/users/login", "/api/users/logout", "/api/users/profile", "/api/users/register",
        "/api/products/search", "/api/products/list", "/api/products/details", "/api/products/categories",
        "/api/orders/create", "/api/orders/status", "/api/orders/history", "/api/orders/cancel",
        "/api/cart/add", "/api/cart/remove", "/api/cart/checkout", "/api/cart/view",
        "/", "/home", "/about", "/contact", "/help", "/privacy", "/terms"
    ]

    methods = ["GET", "POST", "PUT", "DELETE"]
    log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    servers = ["web-01", "web-02", "web-03", "api-01", "api-02", "api-03"]
    applications = ["web-server", "api-gateway", "auth-service", "payment-service"]

    # Generate error vs success (85% success, 15% error for more realistic distribution)
    is_error = random.random() < 0.15

    if is_error:
        status_code = random.choice([400, 401, 403, 404, 500, 502, 503])
        level = "ERROR"
        response_time = random.randint(100, 2000)  # Slower for errors
    else:
        status_code = random.choice([200, 201, 202, 204])
        level = random.choice(["INFO", "INFO", "INFO", "DEBUG"])
        response_time = random.randint(50, 1500)

    # Create log entry
    log_entry = {
        "timestamp": timestamp.isoformat() + "Z",
        "level": level,
        "server": random.choice(servers),
        "application": random.choice(applications),
        "ip_address": f"192.168.{random.randint(1, 10)}.{random.randint(100, 199)}",
        "method": random.choice(methods),
        "endpoint": random.choice(endpoints),
        "status_code": status_code,
        "response_time": response_time,
        "user_id": f"user_{random.randint(1000, 9999)}",
        "session_id": f"sess_{random.randint(100000, 999999)}",
        "bytes_sent": random.randint(500, 50000),
        "user_agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        ])
    }

    return log_entry


def generate_log_dataset(num_entries=10000, filename="web_logs.json"):
    print(f"Generating {num_entries} log entries...")

    # Generate logs over last 7 days
    start_time = datetime.now() - timedelta(days=7)
    time_increment = timedelta(days=7) / num_entries

    logs = []
    current_time = start_time

    for i in range(num_entries):
        # Add some randomness to timestamps
        random_offset = timedelta(seconds=random.randint(-30, 30))
        entry_time = current_time + random_offset

        log_entry = generate_log_entry(entry_time)
        logs.append(log_entry)

        current_time += time_increment

        # Progress indicator
        if (i + 1) % 1000 == 0:
            print(f"Generated {i + 1} entries...")

    # Save in JSON Lines format (one JSON object per line)
    with open(filename, 'w') as f:
        for log in logs:
            f.write(json.dumps(log) + '\n')

    print(f"Dataset saved to {filename}")
    print(f"File size: {os.path.getsize(filename) / 1024 / 1024:.2f} MB")

    # Show sample entries
    print("\nSample log entries:")
    for i in range(3):
        print(f"Entry {i + 1}:", json.dumps(logs[i], indent=2))

    return filename


def generate_partitioned_logs(base_path="data", days=7, entries_per_day=1000):
    """Generate logs partitioned by date for HDFS efficiency """

    print(f"=== Generating partitioned logs for {days} days ===")
    print(f"Entries per day: {entries_per_day}")
    print(f"Total entries: {days * entries_per_day}")

    start_date = datetime.now() - timedelta(days=days)

    # Create base directory
    os.makedirs(base_path, exist_ok=True)

    total_entries = 0

    for day in range(days):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        # Create directory structure (HDFS partitioning pattern)
        day_path = os.path.join(base_path, f"date={date_str}")
        os.makedirs(day_path, exist_ok=True)

        filename = os.path.join(day_path, f"logs_{date_str}.json")

        logs = []
        for hour in range(24):
            # Generate entries distributed throughout the day
            entries_this_hour = entries_per_day // 24 + random.randint(-10, 10)
            entries_this_hour = max(1, entries_this_hour)  # At least 1 entry per hour

            for _ in range(entries_this_hour):
                # Generate timestamp within the hour
                hour_start = current_date.replace(hour=hour, minute=0, second=0)
                minute_offset = timedelta(minutes=random.randint(0, 59),
                                          seconds=random.randint(0, 59))
                entry_time = hour_start + minute_offset

                log_entry = generate_log_entry(entry_time)
                logs.append(log_entry)

        # Save daily logs
        with open(filename, 'w') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')

        total_entries += len(logs)
        print(f"Generated {filename} with {len(logs)} entries")

    print(f"\n=== Partitioned data generation complete ===")
    print(f"Total entries generated: {total_entries}")
    print(f"Data stored in: {base_path}")
    print(f"Partitioning: By date (HDFS-friendly)")

    return base_path


def generate_realistic_traffic_patterns(base_path="data", days=7):
    """Generate logs with realistic traffic patterns """

    print(f"=== Generating realistic traffic patterns ===")

    start_date = datetime.now() - timedelta(days=days)
    os.makedirs(base_path, exist_ok=True)

    for day in range(days):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        # Weekend vs weekday traffic
        is_weekend = current_date.weekday() >= 5

        # Create directory structure
        day_path = os.path.join(base_path, f"date={date_str}")
        os.makedirs(day_path, exist_ok=True)

        filename = os.path.join(day_path, f"logs_{date_str}.json")

        logs = []
        for hour in range(24):
            # Realistic hourly traffic patterns
            if is_weekend:
                # Weekend: slower mornings, peak afternoon
                if 6 <= hour <= 11:
                    base_traffic = 30
                elif 12 <= hour <= 18:
                    base_traffic = 80
                else:
                    base_traffic = 15
            else:
                # Weekday: morning rush, lunch peak, evening rush
                if 8 <= hour <= 10:
                    base_traffic = 100
                elif 12 <= hour <= 13:
                    base_traffic = 90
                elif 17 <= hour <= 19:
                    base_traffic = 110
                elif 1 <= hour <= 6:
                    base_traffic = 5
                else:
                    base_traffic = 40

            # Add randomness
            entries_this_hour = base_traffic + random.randint(-20, 20)
            entries_this_hour = max(1, entries_this_hour)

            for _ in range(entries_this_hour):
                hour_start = current_date.replace(hour=hour, minute=0, second=0)
                minute_offset = timedelta(minutes=random.randint(0, 59),
                                          seconds=random.randint(0, 59))
                entry_time = hour_start + minute_offset

                log_entry = generate_log_entry(entry_time)
                logs.append(log_entry)

        # Save daily logs
        with open(filename, 'w') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')

        print(f"Generated {filename} with {len(logs)} entries ({'Weekend' if is_weekend else 'Weekday'})")

    return base_path


def main():
    print("=== Log Data Generator ===")

    # Generate different sized datasets for testing
    print("\n--- Generating Standard Datasets ---")
    datasets = [
        (1000, "logs_small.json"),  # Small for testing
        (10000, "logs_medium.json"),  # Medium for main analysis
        (50000, "logs_large.json")  # Large for performance demo
    ]

    for num_entries, filename in datasets:
        print(f"\n--- Generating {filename} ---")
        generate_log_dataset(num_entries, filename)

    # Generate partitioned datasets
    print("\n--- Generating Partitioned Datasets (HDFS-friendly) ---")
    generate_partitioned_logs("data/partitioned", days=7, entries_per_day=2000)

    # Generate realistic traffic patterns
    print("\n--- Generating Realistic Traffic Patterns ---")
    generate_realistic_traffic_patterns("data/realistic", days=14)

    print("\n=== All datasets generated successfully! ===")
    print("\nNext steps:")
    print("1. Upload files to HDFS using: python scripts/upload_to_hdfs.py")
    print("2. Verify upload with: hdfs dfs -ls -R /logs/")
    print("3. Start Spark analysis with: python analytics/log_analytics.py")

    print("\nGenerated datasets:")
    print("- Standard datasets: logs_small.json, logs_medium.json, logs_large.json")
    print("- Partitioned data: data/partitioned/ (by date)")
    print("- Realistic patterns: data/realistic/ (with traffic patterns)")


if __name__ == "__main__":
    main()
