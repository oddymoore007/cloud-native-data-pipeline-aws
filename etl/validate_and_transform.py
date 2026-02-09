import csv
from datetime import datetime

RAW_INPUT = "data/raw/transactions.csv"
PROCESSED_OUTPUT = "data/processed/transactions_clean.csv"

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
        datetime.fromisoformat(record["timestamp"])

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
    valid_records = []

    with open(RAW_INPUT, newline="") as infile:
        reader = csv.DictReader(infile)

        for record in reader:
            if is_valid_record(record):
                valid_records.append(transform_record(record))

    with open(PROCESSED_OUTPUT, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=REQUIRED_FIELDS)
        writer.writeheader()
        writer.writerows(valid_records)

    print(
        f"Processed {len(valid_records)} valid records "
        f"→ {PROCESSED_OUTPUT}"
    )


if __name__ == "__main__":
    process_transactions()