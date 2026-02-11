import mysql.connector
import threading
import os
import logging

from typing import Optional
from contextlib import contextmanager


class DatabaseManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, 'initialized'):
            self.config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', 'password'),
                'database': os.getenv('DB_DATABASE', ''),
                'port': os.getenv('DB_PORT', 3306),
            }

            self._connection_pool = {}
            self._pool_lock = threading.Lock()
            self.initialized = True

    def set_config(self, config: dict) -> None:
        self.config.update(config)

    def get_connection(self, thread_id: Optional[int] = None):
        if thread_id is None:
            thread_id = threading.get_ident()

        with self._pool_lock:
            if thread_id not in self._connection_pool:
                try:
                    conn = mysql.connector.connect(**self.config)
                    self._connection_pool[thread_id] = conn
                    logging.info(f"Created new database connection for thread {thread_id}")
                except Exception as e:
                    logging.error(f"Failed to create database connection for thread {thread_id}: {e}")
                    raise
            
            conn = self._connection_pool[thread_id]
            try:
                conn.ping(True)
            except Exception as e:
                conn = mysql.connector.connect(**self.config)
                self._connection_pool[thread_id] = conn
                logging.info(f"Reconnected to database for thread {thread_id}")
            return conn

    def close_all_connections(self) -> None:
        with self._pool_lock:
            for thread_id, conn in self._connection_pool.items():
                conn.close()
            self._connection_pool.clear()
            logging.info("Closed all database connections")

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
                return cursor.fetchone()
            elif fetch_all:
                return cursor.fetchall()
            else:
                return cursor.lastrowid


db_manager = DatabaseManager()