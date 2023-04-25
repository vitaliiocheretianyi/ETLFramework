from airflow.hooks.base import BaseHook
from utils.api_extractor import APIExtractor
import requests

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
