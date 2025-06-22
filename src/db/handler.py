import psycopg2
from psycopg2 import sql
import pandas as pd
from settings import database_config
from utils.logger import get_logger
from core.models import TableData

class PostgresHandler:

    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(**database_config.DB_CONFIG)
        self.conn.autocommit = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger = get_logger("DB Handler")
        if self.conn:
            self.conn.close()

        if exc_type:
            logger.exception(f"Exception occurred: {exc_type.__name__} - {exc_val}")


    def run_query(self, query):
        logger = get_logger("DB Handler")
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)

            self.conn.commit()
            logger.info("Query is executed successfully.")
        except Exception as e:
            self.conn.rollback()
            logger.exception("Failed to execute SQL statements.")
            raise


    def insert_data(self, table_data: TableData):
        logger = get_logger("DB Handler")

        if table_data.data.empty:
            logger.info("Empty DataFrame, no records to insert")
            return

        try:
            df = table_data.data
            table = sql.Identifier(table_data.table_name)
            columns = df.columns.tolist()
            values = df.to_numpy().tolist() 

            insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
                table=table,
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )

            with self.conn.cursor() as cur:
                for idx, row in enumerate(values):
                    try:
                        cur.execute(insert_query, row)
                    except Exception as row_err:
                        logger.error(f"Failed at row {idx}: {row}")
                        for col_idx, val in enumerate(row):
                            col_name = columns[col_idx]
                            try:
                                _ = str(val) < 1  # dummy operation to simulate the problematic one
                            except Exception as col_err:
                                logger.error(f"Column '{col_name}' with value '{val}' caused error: {col_err}")
                        raise  # optional: continue or re-raise

                self.conn.commit()
                logger.info(f"Inserted {len(values)} rows into '{table_data.table_name}'")
                return len(values)

        except Exception as e:
            logger.exception("Error while inserting data into %s", table_data.table_name)
            self.conn.rollback()
            raise


    def read_columns(self, table_name:str, columns:list):
        logger = get_logger("DB Handler")        
        if not columns: 
            logger.log("No columns to read")
            return 

        query = sql.SQL("SELECT {fields} FROM {table}").format(
            fields = sql.SQL(', ').join(map(sql.Identifier, columns)),
            table = sql.Identifier(table_name)
        )

        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                dataFrame = pd.DataFrame(rows, columns=columns)
            
            return dataFrame
        except:
            logger.exception("Error while reading columns")
            return pd.DataFrame
        
    def delete_all_data(self, table_name: str):
        try:
            with self.conn.cursor() as cur:
                query = sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name))
                cur.execute(query)
                print(f"All data deleted from table: {table_name}")
        except Exception as e:
            print(f"Error deleting data from {table_name}: {e}")


    def truncate_table(self, table_name: str):
        logger = get_logger("DB Handler")
        try:
            with self.conn.cursor() as cur:
                query = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(table_name))
                cur.execute(query)
                logger.info(f"Table '{table_name}' truncated successfully.")
        except Exception as e:
            logger.exception(f"Failed to truncate table '{table_name}': {e}")
