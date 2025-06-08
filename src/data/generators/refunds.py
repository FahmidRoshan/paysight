from faker import Faker
import pandas as pd 
import uuid
import random
from datetime import timedelta

fake = Faker()

def generate_refunds(transactions_df):

    refund_reasons = [ 'cancellation', 'product_issue', 'fraud', 'duplicate']
    statuses = ['processed', 'pending', 'failed']
    refunds = []

    eligible_txns = transactions_df[
        (transactions_df['status '] == "success")
        & (transactions_df['refunded_amount'] > 0)
    ]

    for _, txn in eligible_txns:
        refund_amount = round(txn['refunded_amount'], 2)
        if refund_amount == 0.0:
            continue

        refund_date = pd.to_datetime(txn['created_at']) + timedelta(days= random.randint(1, 15))
        refunds.append({
            "id": str(uuid.uuid4()),
            "transaction_id": txn['id'],
            "user_id": txn['user_id'],
            "merhcant_id": txn['merchant_id'],
            "refund_amount": refund_amount,
            "refund_reason": random.choice(refund_reasons),
            "refund_status": random.choices(statuses, weights=[0.9, 0.07, 0.03])[0]
        })

    return pd.DataFrame(refunds)