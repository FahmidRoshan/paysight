from load.definitions import Pipeline
from generators.users import generate_users
from generators.merchants import generate_merchants
from generators.transactions import generate_transactions
from generators.payouts import generate_payouts
from generators.refunds import generate_refunds
from generators.login_events import generate_login_events
from generators.product_events import generate_product_events

PIPELINES = [
    Pipeline("Users", generate_users, table_name="users", rows=2000, truncate_before_load=True),
    Pipeline("Merchants", generate_merchants, table_name="merchants", rows=400, truncate_before_load=True),
    Pipeline("Payouts", generate_payouts, table_name="payouts", required_tables=["users", "merchants"], rows=5000, truncate_before_load=True),
    Pipeline("Transactions", generate_transactions, table_name="transactions", rows=8000, truncate_before_load=True),
    Pipeline("Refunds", generate_refunds, table_name="refunds", required_tables=["transactions"], truncate_before_load=True),
    Pipeline("Login Events", generate_login_events, table_name="login_events", required_tables=["users"], rows=10000, truncate_before_load=True),
    Pipeline("Product Events", generate_product_events, table_name="product_events", required_tables=["users"], rows=20000, truncate_before_load=True),
]

def run_all():
    for pipeline in PIPELINES:
        print(f"\n Running pipeline: {pipeline.name}")
        pipeline.run()