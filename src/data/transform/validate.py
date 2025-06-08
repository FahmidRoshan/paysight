import json
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
    "datetime": lambda x: pd.to_datetime(x) if not isinstance(x, datetime) else x
}

def validate_and_format(data_object: TableData) -> TableData:
    logger = get_logger("Transform")
    if data_object.table_name not in FIELD_SCHEMAS:
        logger.error(f"Schema not found for table: {data_object.table_name}")
    
    if data_object.data.size() == 0:
        logger.log("No data to insert")
        return

    schema = FIELD_SCHEMAS[data_object.table_name]
    
    expected_columns = schema.keys()
    df = data_object.data
    df = df[[col for col in expected_columns if col in df.columns]]
    
    for column, expected_type in schema.items():
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}")
        df[column] = format(df, column, expected_type)

    data_object.data = df
    return data_object


def format(data, columnName, columnType):
    try:
        data[columnName] = data[columnName].apply(TYPE_MAP[columnType])
        return data[columnName]
    except Exception as e:
        logger = get_logger("Data Error")
        logger.error(f"Error converting column '{columnName}' to {columnType}: {e}")
        return None
