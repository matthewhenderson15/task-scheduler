from __future__ import annotations

import pathlib
import sqlite3
import time
from enum import Enum
from typing import List, Optional

from scheduler.utils.helpers import Utils


class DBConnection:
    def __init__(
        self,
        file_system: pathlib.Path = pathlib.Path("../scheduler/database/database.db"),
        in_memory: bool = False,
        timeout: float = 3600.0,
        isolation_level: Optional[IsolationLevels] = None,
        check_same_thread: bool = False,
    ) -> None:
        """Initializes a SQLite3 database connection."""
        self.connection = self.__create_engine_conn(
            file_system=file_system,
            in_memory=in_memory,
            timeout=timeout,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
        )
        self.logger = Utils.__set_logger()

    def __create_engine_conn(
        self,
        file_system: pathlib.Path,
        in_memory: bool,
        timeout: float,
        isolation_level: str,
        check_same_thread: bool,
    ) -> sqlite3.Connection:
        """
        Creates the connection to the SQLite3 database engine.

        Args:
            file_system (pathlib.Path): Path to the SQLite database file. Ignored if in_memory=True.
            in_memory (bool): If True, creates an in-memory database (faster but not persistent).
            timeout (float): How long to wait for database locks before timing out (seconds).
                Default 3600 seconds (1 hour). Use shorter values for web apps.
            isolation_level: Controls when transactions are started:
                - None: Autocommit mode (no transactions, each statement commits immediately).
                - DEFERRED: Transaction starts when first write operation occurs.
                - IMMEDIATE: Transaction starts immediately, gets immediate lock.
                - EXCLUSIVE: Transaction starts immediately, gets exclusive lock.
            check_same_thread: If True, only the thread that created the connection
                can use it. Set to False for multi-threaded applications.

        Returns:
            sqlite3.Connection: A connection to the SQLite3 database.

        Raises:
            sqlite3.DatabaseError:
        """

        try:
            if in_memory:
                database_path = ":memory:"
            else:
                database_path = str(file_system)
                file_system.parent.mkdir(parents=True, exist_ok=True)

            isolation_value = isolation_level.value if isolation_level else None

            connection = sqlite3.connect(
                database=database_path,
                timeout=timeout,
                isolation_level=isolation_value,
                check_same_thread=check_same_thread,
            )

            """Enable foreign key constraints and write-ahead logging, better for concurrency."""
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")

            return connection
        except sqlite3.DatabaseError as e:
            self.logger.error(
                f"There was an error connecting to the SQLite database: {e}"
            )
            raise e

    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Database connection closed.")
        else:
            self.logger.warning("Connection already closed or never established.")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection is closed."""
        self.close()


class DatabaseManager(DBConnection):
    def __init__(self, **kwargs):
        """
        Initialize DatabaseManager with connection parameters.

        Args:
            **kwargs: Parameters passed to DBConnection constructor
        """
        super().__init__(**kwargs)

    def _execute_query(self, query: str, params: Optional[tuple], retries: int = 3):
        """
        Executes a query up to a certain number of retries.

        Params:
            query (str): The SQL query to be executed.
            params (tuple, optional): Parameters for the query (prevents SQL injection).
            retries (int): Number of retries for a query (max 10).

        Returns:
            List of row results from the SQLite3 database.

        Raises:
            ValueError: If retries > 10
            sqlite3.OperationalError: If database operations fail after retries
        """
        if retries > 10:
            raise ValueError("Maximum retries cannot exceed 10")

        for attempt in range(retries):
            try:
                with self.connection as connection:
                    cursor = connection.cursor()

                    cursor.execute(sql=query, parameters=params or ())
                    rows = cursor.fetchall()

                    self.logger.info(
                        f"Successfully fetched {len(rows)} rows for query: {query}"
                    )

                    return rows
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < retries - 1:
                    time.sleep(seconds=(0.1 * (2**attempt)))
                    continue
                self.logger.error(f"An error occurred while fetching records: {e}")
                raise e
            finally:
                cursor.close()

    def execute(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute a query that doesn't return rows (e.g., INSERT, UPDATE, DELETE).

        Returns:
            Number of rows affected
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or ())
            self.connection.commit()
            return cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error executing query: {e}")
            raise e
        finally:
            cursor.close()

    def _find_one(self, columns: List[str], table: str):
        pass

    def _find_all():
        pass

    def _find_many():
        pass


class DMLQueryBuilder:
    @staticmethod
    def select(
        columns: List[str],
        table: str,
        where_clause: Optional[str],
        order_by_col: Optional[str],
        asc_desc: Optional[SQLQueryParams],
        limit: Optional[int] = None,
    ) -> str:
        columns_str = ", ".join(columns)

        query_parts = [f"SELECT {columns_str}", f"FROM {table}"]

        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        if order_by_col:
            direction = f" {asc_desc.value.upper()}" if asc_desc else ""
            query_parts.append(f"ORDER BY {order_by_col}{direction}")

        if limit:
            query_parts.append(f"LIMIT {limit}")

        return " ".join(query_parts)

    @staticmethod
    def insert(
        columns: List[str],
        table: str,
        where_clause: str,
    ):
        pass

    @staticmethod
    def update():
        pass

    @staticmethod
    def delete():
        pass


class DDLQueryBuilder:
    @staticmethod
    def create():
        pass

    @staticmethod
    def alter():
        pass


class IsolationLevels(Enum):
    DEFERRED = "DEFERRED"
    EXCLUSIVE = "EXCLUSIVE"
    IMMEDIATE = "IMMEDIATE"


class SQLQueryParams(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"
