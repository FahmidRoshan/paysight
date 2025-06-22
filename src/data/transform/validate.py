import json
import uuid
import pandas as pd
from datetime import datetime
from utils.logger import get_logger
from core.models import TableData


with open("src/resources/field_schema.json") as f:
    FIELD_SCHEMAS = json.load(f)


TYPE_MAP = {
    "int": int,
    "str": str,
    "float": float,
    "datetime": lambda x: pd.to_datetime(x) if not isinstance(x, datetime) else x,
    "bool": lambda x: bool(int(x)) if isinstance(x, (int, str)) else bool(x),
    "uuid": lambda x: str(uuid.UUID(str(x)))  
}

def validate_and_format(data_object: TableData) -> TableData:
    logger = get_logger("Transform")

    if data_object.table_name not in FIELD_SCHEMAS:
        logger.error(f"Schema not found for table: {data_object.table_name}")
        raise ValueError(f"Schema not defined for table: {data_object.table_name}")
    
    if data_object.data.empty:
        logger.warning("No data to validate for table: %s", data_object.table_name)
        return data_object  

    schema = FIELD_SCHEMAS[data_object.table_name]
    expected_columns = schema.keys()

    df = data_object.data
    df = df[[col for col in expected_columns if col in df.columns]]  

    print(df.columns)
    for column, expected_type in schema.items():
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}, in table: {data_object.table_name}")
        df[column] = format_column(df, column, expected_type)

    data_object.data = df
    # logger.info(f"Data for table '{data_object.table_name}' validated and formatted.")
    return data_object

def format_column(data, column_name, column_type):
    logger = get_logger("Data Error")
    
    def safe_cast(val):
        try:
            return TYPE_MAP[column_type](val)
        except Exception as e:
            logger.warning(f"Failed to cast '{column_name}' value '{val}' to {column_type}: {e}")
            return None

    try:
        data[column_name] = data[column_name].apply(safe_cast)
        return data[column_name]
    except Exception as e:
        logger.error(f"Column formatting failed for '{column_name}': {e}")
        raise
