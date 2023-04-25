# airflow/plugins/custom_operators/LoadOperator.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any
import pandas as pd
import os

class LoadOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        source_data_handler,
        destination_data_handler,
        source_file: str,
        destination_file: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_data_handler = source_data_handler
        self.destination_data_handler = destination_data_handler
        self.source_file = source_file
        self.destination_file = destination_file

    def execute(self, context: Dict[str, Any]) -> None:
        # Read the source data
        data = self.source_data_handler.read_data(self.source_file, file_format="parquet")

        # Load the data to the destination
        self.destination_data_handler.write_data(data, self.destination_file, file_format="parquet")
