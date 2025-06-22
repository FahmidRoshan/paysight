
import psycopg2
from db.handler import PostgresHandler


def create_tables():
    with PostgresHandler() as db:
        create_statements = define_tables()

        for statement in create_statements:
            with PostgresHandler() as db:
                db.run_query(statement)




def define_tables():
    return [

        # Users Table
        """
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            signup_date DATE,
            signup_method TEXT,
            kyc_status TEXT,
            device_id UUID,
            location TEXT,
            is_active BOOLEAN,
            last_login TIMESTAMP,
            risk_flag BOOLEAN
        );
        """,

        # Merchants Table
        """
        CREATE TABLE IF NOT EXISTS merchants (
            id UUID PRIMARY KEY,
            merchant_name TEXT,
            business_category TEXT,
            signup_date DATE,
            location TEXT,
            kyc_status TEXT,
            is_active BOOLEAN,
            risk_flag BOOLEAN
        );
        """,

        # Transactions Table
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id UUID PRIMARY KEY,
            user_id UUID,
            merchant_id UUID,
            amount FLOAT,
            currency TEXT,
            status TEXT,
            payment_method TEXT,
            device_id UUID,
            created_at TIMESTAMP,
            is_fraud BOOLEAN,
            refunded_amount FLOAT
        );
        """,

        # Payouts Table
        """
        CREATE TABLE IF NOT EXISTS payouts (
            id UUID PRIMARY KEY,
            merchant_id UUID,
            payout_date TIMESTAMP,
            amount FLOAT,
            payout_status TEXT,
            transaction_count INT,
            processing_time_days INT,
            is_delayed BOOLEAN
        );
        """,

        # Refunds Table
        """
        CREATE TABLE IF NOT EXISTS refunds (
            id UUID PRIMARY KEY,
            transaction_id UUID,
            user_id UUID,
            merchant_id UUID,
            refund_date TIMESTAMP,
            refund_amount FLOAT,
            refund_reason TEXT,
            refund_status TEXT
        );
        """,

        # Login Events Table
        """
        CREATE TABLE IF NOT EXISTS login_events (
            id UUID PRIMARY KEY,
            user_id UUID,
            device_id UUID,
            login_time TIMESTAMP,
            login_status TEXT,
            ip_address TEXT,
            location TEXT,
            is_suspicious BOOLEAN
        );
        """,

        # Product Events Table
        """
        CREATE TABLE IF NOT EXISTS product_events (
            id UUID PRIMARY KEY,
            user_id UUID,
            event_type TEXT,
            event_timestamp TIMESTAMP,
            session_id UUID,
            product_id TEXT,
            device_id UUID,
            location TEXT
        );
        """
    ]



