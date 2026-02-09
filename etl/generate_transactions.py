import csv
import random
import uuid
from datetime import datetime, timedelta

# Reference data
MERCHANTS = ["Amazon", "Tesco", "Uber", "Netflix", "Apple", "Spotify"]
CATEGORIES = ["Retail", "Transport", "Entertainment", "Subscription"]
COUNTRIES = ["UK", "US", "DE", "FR"]
CURRENCY = "GBP"


def generate_transactions(num_records=500):
    """Generate synthetic raw transaction events."""
    transactions = []
    start_time = datetime.now() - timedelta(days=30)

    for _ in range(num_records):
        transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "timestamp": (
                start_time + timedelta(minutes=random.randint(0, 43200))
            ).isoformat(),
            "account_id": random.randint(10000, 99999),
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": CURRENCY,
            "merchant": random.choice(MERCHANTS),
            "category": random.choice(CATEGORIES),
            "country": random.choice(COUNTRIES),
        })

    return transactions


def write_csv(records, output_path):
    """Write records to a CSV file in the raw data zone."""
    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)


if __name__ == "__main__":
    output_file = "data/raw/transactions.csv"
    data = generate_transactions()
    write_csv(data, output_file)
    print(f"Generated {len(data)} raw transactions → {output_file}")