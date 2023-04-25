import json
from airflow.providers.http.hooks.http import HttpHook


class APIExtractorHook(HttpHook):
    def __init__(self, http_conn_id: str, *args, **kwargs):
        super().__init__(http_conn_id=http_conn_id, *args, **kwargs)

    def fetch_data(self, endpoint: str, params=None):
        response = self.run(endpoint, params)
        data = json.loads(response.text)
        return data
