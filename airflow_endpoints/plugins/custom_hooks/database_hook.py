from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from contextlib import contextmanager


class DatabaseHook(BaseHook):
    def __init__(self, db_conn_id: str):
        super().__init__(source=None)
        self.db_conn_id = db_conn_id

    def get_conn(self):
        connection = self.get_connection(self.db_conn_id)
        url = URL(
            drivername=connection.conn_type,
            username=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            database=connection.schema,
        )
        return create_engine(url)

    @contextmanager
    def get_session(self):
        engine = self.get_conn()
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
