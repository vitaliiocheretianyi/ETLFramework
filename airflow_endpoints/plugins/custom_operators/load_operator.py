# airflow/plugins/custom_operators/LoadOperator.py

import json
from datetime import datetime
from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_endpoints.plugins.custom_hooks.database_hook import DatabaseHook
from data.dynamic_models import create_dynamic_model
from sqlalchemy import Column, Integer, String, DateTime, Float, MetaData
from utils.helpers import infer_column_type

class LoadOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        input_data: str,
        db_conn_id: str,
        destination_table: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.input_data = input_data
        self.db_conn_id = db_conn_id
        self.destination_table = destination_table

    def execute(self, context: Dict[str, Any]) -> None:
        # Read the input data
        with open(self.input_data, "r") as input_file:
            data = json.load(input_file)

        # Define columns based on the input data
        columns = []
        first_record = data[0]
        for key, value in first_record.items():
            column_type = infer_column_type(value)
            columns.append(Column(key, column_type))

        # Create a dynamic ORM model based on the destination table
        db_hook = DatabaseHook(db_conn_id=self.db_conn_id)
        engine = db_hook.get_conn()
        metadata = MetaData(engine)
        model_class = create_dynamic_model(self.destination_table, columns, metadata)

        # Load data into the destination table
        with db_hook.get_session() as session:
            if not engine.dialect.has_table(engine, self.destination_table):
                metadata.create_all(engine)

            # Assuming data is a list of dictionaries, where each dictionary represents a record
            records = [model_class(**record) for record in data]
            session.bulk_insert_mappings(model_class, records)
