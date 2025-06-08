from dataclasses import dataclass
import pandas as pd

@dataclass
class TableData:
    table_name: str
    data: pd.DataFrame