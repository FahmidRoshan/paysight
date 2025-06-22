from data.load.definitions import Pipeline
from data.generators.users import generate_users
from data.generators.merchants import generate_merchants
from data.generators.transactions import generate_transactions
from data.generators.payouts import generate_payouts
from data.generators.refunds import generate_refunds
from data.generators.login_events import generate_login_events
from data.generators.product_events import generate_product_events

PIPELINES = [
    Pipeline("Users", generate_users, table_name="users", rows=2000, truncate_before_load=True),
    Pipeline("Merchants", generate_merchants, table_name="merchants", rows=400, truncate_before_load=True),
    Pipeline("Transactions", generate_transactions, table_name="transactions",
            required_tables=[
                {"table": "users", "columns": ["id", "device_id", "location"]},
                {"table": "merchants", "columns": ["id"]}
            ], 
            rows=8000, truncate_before_load=True),
    Pipeline("Payouts", generate_payouts, table_name="payouts", 
            required_tables=[
                {"table": "transactions", "columns": ["id", "merchant_id", "status", "amount"]},
            ], 
            truncate_before_load=True),
    Pipeline("Login Events", generate_login_events, table_name="login_events",
            required_tables=[
                {"table": "users", "columns": ["id", "device_id", "location"]}
            ],
            rows=10000, truncate_before_load=True),
    Pipeline("Refunds", generate_refunds, table_name="refunds",
            required_tables=[
                {"table": "transactions", "columns": ["id", "user_id", "merchant_id", "status", "refunded_amount", "created_at"]}
            ],
            truncate_before_load=True),
    Pipeline("Product Events", generate_product_events, table_name="product_events", 
            required_tables=[
                {"table": "users", "columns": ["id", "device_id", "location"]}
            ],
            rows=20000, truncate_before_load=True),
]

def run_all():
    for pipeline in PIPELINES:
        print(f"\n Running pipeline: {pipeline.name}")
        pipeline.run()