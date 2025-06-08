import psycopg2
from psycopg2 import sql
import pandas as pd
from config import database_config
from utils.logger import get_logger

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


    def insert_data(self, TableData):
        logger = get_logger("DB Handler")
        if TableData.records.empty:
            logger.log("Empty Datafram, No records to insert")
            return
        

        columns = TableData.records.columns

        query = sql.SQL(    
            """INSERT INTO {table} ({fields}) VALUES {values}
        """
        ).format(
            table = sql.Identifier(TableData.table_name),
            fields = sql.SQL(', ').join(map(sql.Identifier, columns)),
            values = sql.SQL(', ').join([
                sql.SQL('({})').format(
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                for _ in range(len(TableData.records))
            ])
        )

        try:
            values_flat = TableData.records.to_numpy().flatten().tolist()
            with self.conn.cursor() as cur:
                cur.execute(query, values_flat)
                return cur.rowcount  
        except:
            logger.exception("Error while inserting data")            


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
