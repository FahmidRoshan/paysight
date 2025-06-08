from db.handler import PostgresHandler
from data.transform.validate import validate_and_format
from utils.logger import get_logger

logger = get_logger("Pipeline")

class Pipeline:
    def __init__(self, name: str, generate_func, table_name: str, required_tables=None, rows=None, truncate_before_load=False):
        self.name = name
        self.generate_func = generate_func
        self.required_tables = required_tables or []
        self.rows = rows
        self.table_name = table_name
        self.truncate_before_load = truncate_before_load

    def run(self):
        try:
            with PostgresHandler() as db:
                if self.truncate_before_load:
                    db.truncate_table(self.table_name)
                    logger.info(f"Truncated table: {self.table_name}")

                foreign_keys = [db.read_columns(tbl, ["id"]) for tbl in self.required_tables]
                data = self.generate_func(*foreign_keys, self.rows) if self.rows else self.generate_func(*foreign_keys)
                data = validate_and_format(data)
                rows_inserted = db.insert_data(data)
                logger.info(f"{self.name} pipeline completed: {rows_inserted} rows inserted.")

        except Exception as e:
            logger.exception(f"Failed in {self.name} pipeline: {e}")
