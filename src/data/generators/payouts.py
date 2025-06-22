import pandas as pd
import random
import uuid
from faker import Faker
from datetime import timedelta

fake = Faker()

def generate_payouts(transactions_df: pd.DataFrame):
    payouts = []
    payout_statuses = [ 'completed', 'pending', 'failed']
    grouped_txns = transactions_df[transactions_df["status"] == "success"].groupby("merchant_id")
    for merchant_id, group in grouped_txns:
        if group.shape[0] < 5:
            continue
        for _ in range(random.randint(1, 3)):
            txn_sample = group.sample(n=random.randint(1, len(transactions_df.columns)))
            payout_date = fake.date_time_between(start_date='-3M', end_date='now')
            total_amt = txn_sample["amount"].sum()
            txn_count = txn_sample.shape[0]
            delay_days = random.randint(0, 5)
            payouts.append({
                "id": str(uuid.uuid4()),
                "merchant_id": merchant_id,
                "payout_date": payout_date,
                "amount": round(total_amt, 2),
                "payout_status": random.choices(payout_statuses, weights = [0.9, 0.5, 0.05])[0],
                "transaction_count": txn_count,
                "processing_time_delays": delay_days,
                "is_delayed": delay_days > 2
            })
    return pd.DataFrame(payouts)