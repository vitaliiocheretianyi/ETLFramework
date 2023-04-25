from sqlalchemy import Table, MetaData
from sqlalchemy.orm import mapper


def create_dynamic_model(table_name, columns, metadata):
    table = Table(table_name, metadata, *columns)

    class DynamicModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    return mapper(DynamicModel, table)
