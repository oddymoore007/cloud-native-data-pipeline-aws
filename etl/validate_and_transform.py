import datetime
import os
import csv
import boto3
import logging
import argparse


RAW_INPUT = "data/raw/transactions.csv"



REQUIRED_FIELDS = [
    "transaction_id",
    "timestamp",
    "account_id",
    "amount",
    "currency",
    "merchant",
    "category",
    "country",
]

# Cloud configuration
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX", "transactions")


## Helper function
def ensure_input_exists(path):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Raw input file not found: {path}"
        )
    
def upload_directory_to_s3(local_dir, bucket, prefix):
    """
    Upload all files in a directory to S3,
    preserving partition structure and normalizing path separators.
    """
    s3 = boto3.client("s3")

    base_dir = "data/processed"

    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)

            relative_path = os.path.relpath(local_path, base_dir)

            # Normalize Windows backslashes to S3 forward slashes
            relative_path = relative_path.replace("\\", "/")

            s3_key = f"{prefix}/{relative_path}"

            print(f"Uploading {local_path} → s3://{bucket}/{s3_key}")
            s3.upload_file(local_path, bucket, s3_key)
            
def is_valid_record(record):
    """Basic validation rules for raw transactions."""
    try:
        # Required fields present
        for field in REQUIRED_FIELDS:
            if not record.get(field):
                return False

        # Validate timestamp
        datetime.datetime.fromisoformat(record["timestamp"])

        # Validate amount
        amount = float(record["amount"])
        if amount <= 0:
            return False

        return True
    except Exception:
        return False




def transform_record(record):
    """Transform raw record into clean, standardised format."""
    return {
        "transaction_id": record["transaction_id"],
        "timestamp": record["timestamp"],
        "account_id": int(record["account_id"]),
        "amount": round(float(record["amount"]), 2),
        "currency": record["currency"],
        "merchant": record["merchant"].title(),
        "category": record["category"].title(),
        "country": record["country"].upper(),
    }

def aggregate_transactions(valid_records):
    daily_totals = {}
    daily_counts = {}

    for record in valid_records:
        account_id = record["account_id"]
        amount = record["amount"]
        date = record["timestamp"].split("T")[0]

        key = f"{account_id}_{date}"

        if key not in daily_totals:
            daily_totals[key] = 0
            daily_counts[key] = 0

        daily_totals[key] += amount
        daily_counts[key] += 1

    return daily_totals, daily_counts


def process_transactions():
    ensure_input_exists(RAW_INPUT)

    valid_records = []
    rejected_records = []
    total_records = 0

    with open(RAW_INPUT, newline="") as infile:
        reader = csv.DictReader(infile)

        for record in reader:
            total_records += 1

            if is_valid_record(record):
                valid_records.append(transform_record(record))
            else:
                rejected_records.append(record)

    print("Starting pipeline run (idempotent mode)")

    # Partitioned output
    today = datetime.date.today()
    processed_output_dir = f"data/processed/date={today}/"

    # Ensure directory exists
    os.makedirs(processed_output_dir, exist_ok=True)

    # Write clean data
    clean_output = processed_output_dir + "transactions_clean.csv"
    with open(clean_output, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=REQUIRED_FIELDS)
        writer.writeheader()
        writer.writerows(valid_records)

    # Write rejected data
    rejected_output = processed_output_dir + "transactions_rejected.csv"

    if rejected_records:
        with open(rejected_output, "w", newline="") as badfile:
            writer = csv.DictWriter(
                badfile,
                fieldnames=rejected_records[0].keys()
            )
            writer.writeheader()
            writer.writerows(rejected_records)

       # Aggregate daily totals
    daily_totals, daily_counts = aggregate_transactions(valid_records)

    daily_output = processed_output_dir + "daily_totals.csv"

    with open(daily_output, "w", newline="") as dailyfile:
        writer = csv.writer(dailyfile)
        writer.writerow(["account_id", "date", "total_amount", "transaction_count"])

        for key in daily_totals:
            account_id, date = key.split("_")
            writer.writerow([account_id, date, daily_totals[key], daily_counts[key]])

    print(f"Daily totals written to: {daily_output}")        

    # Emit metrics
    print("Pipeline run metrics")
    print("--------------------")
    print(f"Total records read: {total_records}")
    print(f"Valid records written: {len(valid_records)}")
    print(f"Rejected records: {len(rejected_records)}")

    if rejected_records:
        print(f"Rejected records saved to: {rejected_output}")
    
    if UPLOAD_TO_S3:
        print("Uploading partition to S3...")
        upload_directory_to_s3(
            processed_output_dir,
            S3_BUCKET_NAME,
            S3_PREFIX
        )

if __name__ == "__main__":
    process_transactions()