import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime

fake = Faker()
N_MERCHANTS = 50

def generate_merchants(n):
    categories = ['ecommerce', 'edtech', 'healthcare', 'finance', 'logistics', 'retail']
    kyc_statuses = ['Verified', 'Pending', 'Failed']
    cities = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Seattle']
    merchants = []

    for _ in range(n):
        signup = fake.date_between(start_date='-2y', end_date='today')
        kyc = random.choices(kyc_statuses, weights=[0.8, 0.15, 0.05])[0]
        risk = random.choices([True, False], weights=[0.1, 0.9])[0]

        merchants.append({
            "id": str(uuid.uuid4()),
            "name": fake.company(),
            "business_category": random.choice(categories),
            "signup_date": signup,
            "location": random.choice(cities),
            "kyc_status": kyc,
            "is_active": random.choice([True, False]),
            "risk_flag": risk
        })

    return pd.DataFrame(merchants)
