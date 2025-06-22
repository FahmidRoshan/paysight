from db.handler import PostgresHandler
from data.transform.validate import validate_and_format
from utils.logger import get_logger
from core.models import TableData

logger = get_logger("Pipeline")

class Pipeline:
    def __init__(self, name: str, generate_func, table_name: str, required_tables=None, rows=None, truncate_before_load=False):
        self.name = name
        self.generate_func = generate_func
        self.rows = rows
        self.table_name = table_name
        self.truncate_before_load = truncate_before_load

        self.required_tables = []
        if required_tables:
            for item in required_tables:
                if isinstance(item, str):
                    self.required_tables.append({"table": item, "columns": ["id"]})
                elif isinstance(item, dict):
                    if "table" not in item:
                        raise ValueError(f"Missing 'table' key in required_tables entry: {item}")
                    if "columns" not in item:
                        item["columns"] = ["id"]
                    self.required_tables.append(item)
                else:
                    raise TypeError(f"Invalid type in required_tables: {item} (expected str or dict)")

    def run(self):
        try:
            with PostgresHandler() as db:
                if self.truncate_before_load:
                    db.truncate_table(self.table_name)
                    logger.info(f"Truncated table: {self.table_name}")

                foreign_data = []
                for table in self.required_tables:
                    if isinstance(table, str):
                        foreign_data.append(db.read_columns(table, ["id"]))
                    elif isinstance(table, dict):
                        tbl = table.get("table")
                        cols = table.get("columns", ["id"])
                        foreign_data.append(db.read_columns(tbl, cols))
                    else:
                        raise ValueError(f"Invalid required_table format: {table}")

                print(self.table_name)
                print(self.rows)
                data = self.generate_func(self.rows, *foreign_data) if self.rows else self.generate_func(*foreign_data)
                data_object: TableData = TableData(table_name=self.table_name, data=data)
                logger.info(f"{data_object.table_name} data is generated")

                data_object = validate_and_format(data_object)
                logger.info(f"{data_object.table_name} data is validated")

                rows_inserted = db.insert_data(data_object)
                logger.info(f"{self.name} pipeline completed: {rows_inserted} rows inserted.")

        except Exception as e:
            logger.exception(f"Failed in {self.name} pipeline: {e}")


