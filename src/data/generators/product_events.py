from faker import Faker
import pandas as pd
import uuid
import random 
from datetime import timedelta

fake = Faker()

def generate_product_events(users_df, n_events=8000):
    event_types = ['view_product', 'add_to_cart', 'checkout', 'payment_success']
    locations = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Seattle']
    product_events = []

    for _ in range(n_events):
        user = users_df.sample(1).iloc[0]
        event = random.choices(
            event_types,
            weights=[0.5, 0.25, 0.15, 0.10]
        )[0]
        event_time = fake.date_time_between(start_date="-6M", end_date="now")

        product_events.append({
            "id": str(uuid.uuid4()),
            "user_id": user["id"],
            "event_type": event,
            "event_timestamp": event_time,
            "session_id": str(uuid.uuid4()),
            "product_id": f"PROD-{random.randint(100,999)}",
            "device_id": user["device_id"] if random.random() > 0.2 else str(uuid.uuid4()),
            "location": random.choice(locations),
        })

    return pd.DataFrame(product_events)    