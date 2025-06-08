from faker import Faker
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

fake = Faker()
N_USERS = 1000

def generate_users(n):

    signup_methods = ['App', 'Web', 'Referral']
    kyc_statuses = ['Not Started', 'In Progress', 'Verified', 'Failed']
    locations = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Boston']
    users = []

    for _ in range(n):
        signup_date = fake.date_between(start_date='-1y', end_date='today')
        last_login = signup_date + timedelta(days=random.randint(0, 300))
        kyc = random.choices(kyc_statuses, weights=[0.1, 0.2, 0.6, 0.1])[0]
        is_active = random.choice([True, False])
        risk = random.choices([True, False], weights=[0.05, 0.95])[0]

        users.append({
            "id": str(uuid.uuid4()),
            "full_name": fake.name(),
            "email": fake.email(),
            "signup_date": signup_date,
            "signup_method": random.choice(signup_methods),
            "kyc_status": kyc,
            "device_id": str(uuid.uuid4()),
            "location": random.choice(locations),
            "is_active": is_active,
            "last_login": last_login,
            "risk_flag": risk
        })

    return pd.DataFrame(users)