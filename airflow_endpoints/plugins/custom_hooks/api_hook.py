import requests
from airflow.hooks.base import BaseHook
from data.raw.api_extractor import APIExtractor


class APIExtractorHook(APIExtractor, BaseHook):
    def __init__(self, conn_id: str):
        connection = self.get_connection(conn_id)
        base_url = connection.host
        headers = {"Authorization": f"Bearer {connection.password}"}
        super().__init__(base_url, headers)

    def get_conn(self) -> requests.Session:
        return self.connect()

    def disconnect(self) -> None:
        self.close_connection()

    def __enter__(self):
        return self.get_conn()

    def __exit__(self, *args, **kwargs):
        self.disconnect()
