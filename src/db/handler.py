import psycopg2
from psycopg2 import sql
import pandas as pd
from config import database_config

class PostgresHandler:

    def __init__(self):
        self.conn = psycopg2.connect(**database_config.DB_CONFIG)
        self.conn.autocommit = True

    def insert_data(self, table_name: str, records: pd.DataFrame):
        if records.empty:
            print("Empty Datafram, No records to insert")
            return

        columns = records.columns

        query = sql.SQL(    
            """INSERT INTO {table} ({fields}) VALUES {values}
        """
        ).format(
            table = sql.Identifier(table_name),
            fields = sql.SQL(', ').join(map(sql.Identifier, columns)),
            values = sql.SQL(', ').join([
                sql.SQL('({})').format(
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                for _ in range(len(records))
            ])
        )

        values_flat = records.to_numpy().flatten().tolist()
        with self.conn.cursor() as cur:
            cur.execute(query, values_flat)

    def close(self):
        self.conn.close()