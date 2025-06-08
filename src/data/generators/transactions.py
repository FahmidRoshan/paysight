from faker import Faker
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

fake = Faker()
N_TRANSACTIONS = 5000

def generate_transactions(n, users_df, merchants_df):
    statuses = ['success', 'failed', 'pending']
    methods = ['card', 'UPI', 'netbanking', 'wallet']
    currency = 'USD'
    txns = []

    for _ in range(n):
        user = users_df.sample(1).iloc[0]
        status = random.choices(statuses, weights=[0.85, 0.1, 0.05])[0]
        is_fraud = random.choices([True, False], weights=[0.02, 0.98])[0]
        amount = round(random.uniform(5, 500), 2)
        refunded = 0.0 if status != 'success' else round(random.uniform(0, amount), 2) if random.random() < 0.15 else 0.0
        merchant = merchants_df.sample(1).iloc[0]

        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id": user['id'],
            "merchant_id": merchant['id'],
            "amount": amount,
            "currency": currency,
            "status": status,
            "payment_method": random.choice(methods),
            "device_id": user['device_id'],
            "created_at": fake.date_time_between(start_date="-6M", end_date="now"),
            "is_fraud": is_fraud,
            "refunded_amount": refunded
        })

    return pd.DataFrame(txns)