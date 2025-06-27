import csv
import random
import time
from pathlib import Path

# --- Configuration ---
NUM_RECORDS = 1_000_000
NUM_FILES = 5
DIRTY_FRACTION = 0.05
OUTPUT_DIR = Path("data/raw")
# ---------------------

DEVICES = [f"device_{i:04d}" for i in range(100)]
STATUS_CODES = [200, 201, 400, 404, 500]


def create_record(is_dirty=False):
    record = {
        "device_id": random.choice(DEVICES),
        "timestamp": int(time.time()) - random.randint(0, 3600 * 24),
        "temperature": round(random.uniform(15.0, 45.0), 2),
        "metric_1": random.randint(0, 1000),
        "status_code": random.choice(STATUS_CODES),
    }
    if is_dirty:
        if random.random() < 0.3:
            record["device_id"] = None
        if random.random() < 0.3:
            record["temperature"] = "INVALID"
        if random.random() < 0.3:
            record["status_code"] = 999
    return record


def generate():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating {NUM_RECORDS} records into {NUM_FILES} files...")

    records_per_file = NUM_RECORDS // NUM_FILES
    fieldnames = ["device_id", "timestamp", "temperature", "metric_1", "status_code"]

    for i in range(NUM_FILES):
        filepath = OUTPUT_DIR / f"telemetry_data_{i+1}.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for _ in range(records_per_file):
                is_dirty = random.random() < DIRTY_FRACTION
                writer.writerow(create_record(is_dirty))
    print("Data generation complete.")


if __name__ == "__main__":
    generate()
