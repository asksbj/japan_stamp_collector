import logging
import mysql.connector
import threading
import os

from typing import Optional, Dict
from contextlib import contextmanager
from abc import ABC

logging.basicConfig(level=logging.INFO)

# Serialize all mysql.connector.connect() calls to avoid C-extension race/segfault
# when multiple threads create connections at the same time (e.g. main + worker).
_CONNECT_LOCK = threading.Lock()


class BaseDBManager(ABC):
    
    def __init__(self, config_prefix: str) -> None:
        self.config_prefix = config_prefix
        self._connection_pool = {}
        self._pool_lock = threading.Lock()

        self._config = self._load_config()


    def _load_config(self) -> Dict:
        return {
            'host': os.getenv(f'{self.config_prefix}_HOST', 'localhost'),
            'user': os.getenv(f'{self.config_prefix}_USER', 'root'),
            'password': os.getenv(f'{self.config_prefix}_PASSWORD', ''),
            'database': os.getenv(f'{self.config_prefix}_DATABASE', ''),
            'port': int(os.getenv(f'{self.config_prefix}_PORT', 3306)),
        }

    def set_config(self, config: dict) -> None:
        self._config.update(config)
        self.close_all_connections()

    def get_connection(self, thread_id: Optional[int] = None):
        if thread_id is None:
            thread_id = threading.get_ident()

        with self._pool_lock:
            if thread_id not in self._connection_pool:
                try:
                    with _CONNECT_LOCK:
                        conn = mysql.connector.connect(**self._config)
                    self._connection_pool[thread_id] = conn
                    logging.info(
                        f"{self.__class__.__name__}: Created new database connection for thread {thread_id} "
                        f"(config_prefix={self.config_prefix})"
                    )
                    return conn
                except Exception as e:
                    logging.error(
                        f"{self.__class__.__name__}: Failed to create database connection for thread {thread_id}: {e}"
                    )
                    raise

            conn = self._connection_pool[thread_id]
            try:
                conn.ping(reconnect=True)
            except Exception as e:
                old_conn = conn
                try:
                    with _CONNECT_LOCK:
                        conn = mysql.connector.connect(**self._config)
                    self._connection_pool[thread_id] = conn
                    logging.info(
                        f"{self.__class__.__name__}: Reconnected for thread {thread_id} (config_prefix={self.config_prefix})"
                    )
                finally:
                    try:
                        old_conn.close()
                    except Exception:
                        pass
            return conn
    
    def close_all_connections(self) -> None:
        with self._pool_lock:
            for thread_id, conn in list(self._connection_pool.items()):
                try:
                    conn.close()
                except Exception as e:
                    logging.warning(f"{self.__class__.__name__}: Error closing connection for thread {thread_id}: {e}")
                del self._connection_pool[thread_id]
            logging.info(f"{self.__class__.__name__}: Closed all database connections")
    
    @contextmanager
    def get_cursor(self, commit: bool = True, thread_id: Optional[int] = None):
        conn = self.get_connection(thread_id)
        cursor = None
        try:
            cursor = conn.cursor()
            yield cursor

            if cursor.with_rows:
                cursor.fetchall()

            if commit:
                conn.commit()
                
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Failed to get cursor for thread {thread_id}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def execute_query(self, query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = False):
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            if fetch_one:
                row = cursor.fetchone()
                if cursor.with_rows:
                    cursor.fetchall()
                return row
            elif fetch_all:
                return cursor.fetchall()
            else:
                return cursor.lastrowid, cursor.rowcount

    @property
    def database_name(self) -> str:
        return self._config.get('database', '')


class DefaultDBManager(BaseDBManager):

    def __init__(self) -> None:
        super().__init__(config_prefix="DB")


class ETLDBManager(BaseDBManager):

    def __init__(self) -> None:
        super().__init__(config_prefix="ETL")


db_manager = DefaultDBManager()
etl_db_manager = ETLDBManager()