from sqlalchemy import Integer, String, DateTime, Float
from datetime import datetime


def infer_column_type(value):
    if isinstance(value, int):
        return Integer
    elif isinstance(value, float):
        return Float
    elif isinstance(value, str):
        return String
    elif isinstance(value, datetime):
        return DateTime
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")
