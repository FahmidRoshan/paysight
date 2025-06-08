import pandas as pd
import random
import uuid
from faker import Faker
from datetime import timedelta

fake = Faker()

def generate_login_events(users_df, n_events=10000):
    login_statuses = ['success', 'failed']
    locations = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Seattle']
    login_events = []

    for _ in range(n_events):
        user = users_df.sample(1).iloc[0]
        status = random.choices(login_statuses, weights=[0.9, 0.1])[0]
        is_suspicious = random.choice([True, False], weights =[0.05, 0.95])[0]
        device_id = user['device_id'] if random.random() > 0.3 else str(uuid.uuid4())


        login_events.append({
            "id": str(uuid.uuid4()),
            "user_id": user['id'],
            "device_id": device_id,
            "login_time": fake.date_time_between(start_date="-6M", end_date="now"),
            "login_status": status,
            "ip_address": fake.ipv4_public(),
            "location": random.choice(locations),
            "is_suspicious": is_suspicious
        })
        
    return pd.DataFrame(login_events)