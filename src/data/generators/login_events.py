import pandas as pd
import random
import uuid
from faker import Faker
from datetime import timedelta

fake = Faker()

N_EVENTS = 10000

def generate_login_events(n, users_df ):
    login_statuses = ['success', 'failed']
    locations = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Seattle']
    login_events = []
    n = max(n, N_EVENTS)
    for _ in range(n):
        user = users_df.sample(1).iloc[0]
        status = random.choices(login_statuses, weights=[0.9, 0.1])[0]
        is_suspicious = random.choices([True, False], weights =[0.05, 0.95])[0]
        random_value = random.random()
        device_id = user['device_id'] if random_value > 0.3 else str(uuid.uuid4())
        location = user['location'] if random_value > 0.3 else random.choice(locations)


        login_events.append({
            "id": str(uuid.uuid4()),
            "user_id": user['id'],
            "device_id": device_id,
            "login_time": fake.date_time_between(start_date="-6M", end_date="now"),
            "login_status": status,
            "ip_address": fake.ipv4_public(),
            "location": location,
            "is_suspicious": is_suspicious
        })
        
    return pd.DataFrame(login_events)