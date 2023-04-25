from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any
from airflow.plugins.custom_hooks.APIExtractorHook import APIExtractorHook


class ExtractOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            http_conn_id: str,
            endpoint: str,
            output_path: str,
            params=None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.output_path = output_path
        self.params = params

    def execute(self, context: Dict[str, Any]) -> None:
        # Instantiate the APIExtractorHook
        api_hook = APIExtractorHook(http_conn_id=self.http_conn_id)

        # Fetch data from the API
        data = api_hook.fetch_data(self.endpoint, self.params)

        # Write the data to the output path
        with open(self.output_path, "w") as output_file:
            json.dump(data, output_file)
