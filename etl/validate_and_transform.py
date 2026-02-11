import datetime
import os
import csv


RAW_INPUT = "data/raw/transactions.csv"
def ensure_input_exists(path):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Raw input file not found: {path}"
        )
    


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

    # Emit metrics
    print("Pipeline run metrics")
    print("--------------------")
    print(f"Total records read: {total_records}")
    print(f"Valid records written: {len(valid_records)}")
    print(f"Rejected records: {len(rejected_records)}")

    if rejected_records:
        print(f"Rejected records saved to: {rejected_output}")

if __name__ == "__main__":
    process_transactions()