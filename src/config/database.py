from __future__ import annotations

import pathlib
import sqlite3
import threading
import time
from contextlib import contextmanager
from enum import Enum
from queue import Queue
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
        pool_size: int = 5,
    ) -> None:
        """Initializes a SQLite3 database connection pool."""
        self.file_system = file_system
        self.in_memory = in_memory
        self.timeout = timeout
        self.isolation_level = isolation_level
        self.check_same_thread = check_same_thread
        self.pool_size = pool_size
        self.connection_pool = Queue(maxsize=pool_size)
        self.pool_lock = threading.Lock()
        self._initialize_pool()

        self.connection = self.__create_engine_conn(
            file_system=file_system,
            in_memory=in_memory,
            timeout=timeout,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
        )
        self.logger = Utils.__set_logger()

    def _initialize_pool(self):
        """Initialize the connection pool with multiple connections."""
        for i in range(self.pool_size):
            conn = self.__create_engine_conn(
                file_system=self.file_system,
                in_memory=self.in_memory,
                timeout=self.timeout,
                isolation_level=self.isolation_level,
                check_same_thread=self.check_same_thread,
            )
            self.connection_pool.put(conn)
            self.logger.info(f"Created connection {i + 1}/{self.pool_size} for pool")

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
            sqlite3.DatabaseError: If there's an error in establishing the DB connection.
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

            """
            Enable foreign key constraints and write-ahead logging, better for concurrency.
            
            Set larger cache and store temp tables in memory.
            """
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.execute("PRAGMA cache_size = 10000")
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("PRAGMA mmap_size = 268435456")

            connection.row_factory = sqlite3.Row

            return connection
        except sqlite3.DatabaseError as e:
            self.logger.error(
                f"There was an error connecting to the SQLite database: {e}"
            )
            raise e

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting and returning connections from the pool.

        Usage:
            with db_manager.get_connection() as conn:
                cursor = conn.execute("SELECT * FROM tasks")
                results = cursor.fetchall()
        """
        conn = None
        try:
            conn = self.connection_pool.get(timeout=self.timeout)
            self.logger.debug("Retrieved connection from pool")
            yield conn
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                # TODO - what exception should be thrown here?
                except Exception:
                    pass
            raise e
        finally:
            if conn:
                self.connection_pool.put(conn)
                self.logger.debug("Returned connection to pool")

    def __close_pool(self):
        """Close all connections in the pool."""
        closed_count = 0
        while not self.connection_pool.empty():
            try:
                conn = self.connection_pool.get_nowait()
                conn.close()
                closed_count += 1
            except Empty:
                break
        self.logger.info(f"Closed {closed_count} connections from pool")

    def close(self):
        self.__close_pool()

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

    def _execute_select_query(
        self, query: str, params: Optional[tuple] = None, retries: int = 3
    ) -> List[sqlite3.Row]:
        """
        Executes a query up to a certain number of retries.

        Params:
            query (str): The SQL query to be executed.
            params (tuple, optional): Parameters for the query (prevents SQL injection).
            retries (int): Number of retries for a query (max 10).

        Returns:
            List of row results from the SQLite3 database.

        Raises:
            ValueError: If retries > 10.
            sqlite3.OperationalError: If database operations fail after retries.
        """
        if retries > 10:
            raise ValueError("Maximum retries cannot exceed 10")

        for attempt in range(retries):
            cursor = None
            try:
                cursor = self.connection.cursor()

                cursor.execute(sql=query, parameters=params or ())
                rows = cursor.fetchall()

                self.logger.info(
                    f"Successfully fetched {len(rows)} rows for query: {query}"
                )

                return rows
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < retries - 1:
                    wait_time = time.sleep(seconds=(0.1 * (2**attempt)))
                    self.logger.warning(
                        f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{retries})"
                    )
                    time.sleep(wait_time)
                    continue
                self.logger.error(f"An error occurred while fetching records: {e}")
                raise e
            finally:
                if cursor:
                    cursor.close()

    def _execute(
        self, query: str, params: Optional[tuple] = None, retries: int = 3
    ) -> int:
        """
        Execute a query that doesn't return rows (e.g., INSERT, UPDATE, DELETE).

        Args:
            query (str): The SQL query to be executed.
            params (tuple, optional): Parameters for the query (prevents SQL injection).
            retries (int): Number of retries for a query (max 10).

        Returns:
            Number of rows affected.

        Raises:
            ValueError: If retries > 10.
            sqlite3.OperationalError: If database operations fail after retries.
        """
        if retries > 10:
            raise ValueError("Maximum retries cannot exceed 10")

        cursor = self.connection.cursor()
        try:
            cursor = self.connection.cursor()

            cursor.execute(sql=query, parameters=params or ())
            self.connection.commit()

            return cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error executing query: {e}")
            raise e
        finally:
            cursor.close()

    def _find_one(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[sqlite3.Row]:
        """
        Find one record using connection pool.

        Args:
            query (str): The query to be executed.
            params (tuple, optional):

        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor._execute(query, params or ())
                return cursor.fetchone()
            finally:
                cursor.close()

    def _find_all(
        self, query: str, params: Optional[tuple] = None
    ) -> List[sqlite3.Row]:
        """
        Wrapper for execute select query function. Finds and returns all records.

        Args:
            query (str): The query to be executed.
            params (tuple, optional): The parameters to be passed to the query.

        Returns:
            List of all the database rows.
        """
        return self._execute_select_query(query=query, params=params)

    def _find_many(
        self, query: str, params: Optional[tuple] = None, limit: int = 100
    ) -> List[sqlite3.Row]:
        """
        Find many records with limit."""
        limited_query = f"{query} LIMIT {limit}"
        return self._execute_select_query(limited_query, params)

    def begin_transaction(self):
        """Begin a database transaction."""
        self.connection.execute("BEGIN")
        self.logger.info("Transaction started")

    def commit_transaction(self):
        """Commit the current transaction."""
        self.connection.commit()
        self.logger.info("Transaction committed")

    def rollback_transaction(self):
        """Rollback the current transaction."""
        self.connection.rollback()
        self.logger.info("Transaction rolled back")


class DMLQueryBuilder:
    @staticmethod
    def select(
        columns: List[str],
        table: str,
        where_clause: Optional[str],
        order_by_col: Optional[str],
        group_by_col: Optional[str],
        asc_desc: Optional[SQLQueryParams],
        limit: Optional[int] = None,
    ) -> str:
        columns_str = ", ".join(columns)

        query_parts = [f"SELECT {columns_str}", f"FROM {table}"]

        if where_clause:
            query_parts.append(f"WHERE {where_clause}")

        if group_by_col:
            query_parts.append(f"GROUP BY {group_by_col}")

        if order_by_col:
            direction = f"{asc_desc.value.upper()}" if asc_desc else ""
            query_parts.append(f"ORDER BY {order_by_col} {direction}")

        if limit:
            query_parts.append(f"LIMIT {limit}")

        return " ".join(query_parts)

    @staticmethod
    def insert(
        columns: List[str],
        table: str,
    ) -> str:
        """Build insert query."""
        columns_str = ", ".join(columns)

        placeholders = ", ".join(["?" for _ in columns])
        return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

    @staticmethod
    def update(
        table: str, columns: List[str], values: List[str], where_clause: str
    ) -> str:
        """Build update query"""
        if len(columns) != len(values):
            raise ValueError("Each column must have a corresponding update value!")

        base_col_str = ""
        for i in range(columns):
            base_col_str += f"{columns[i]} = {values[i]}"
            base_col_str += ", " if i != len(columns) - 1 else ""

        return f"UPDATE {table} SET {base_col_str} WHERE {where_clause}"

    @staticmethod
    def delete(table: str, where_clause: str) -> str:
        """Build delete query."""
        return f"DELETE FROM {table} WHERE {where_clause}"


class DDLQueryBuilder:
    @staticmethod
    def create(table: str, columns: List[tuple], primary_key_col: str) -> str:
        """
        Build create table query.

        Args:
            table (str): The desired table name.
            columns (List[tuple]): A list of column names and associated types.
            primary_key_col (str): The column name

        """
        if primary_key_col not in [col[0] for col in columns]:
            raise ValueError(
                "Primary key column must be one of the desired columns in the table!"
            )

        final_column_types = ""

        for col_type in columns:
            final_column_types += f"{col_type[0]} {col_type[1]}"
            final_column_types += (
                " PRIMARY_KEY," if col_type[0] == primary_key_col else ","
            )

        return f"CREATE TABLE IF NOT EXISTS {table} ({final_column_types})"

    @staticmethod
    def alter():
        """Build alter table query."""
        pass

    @staticmethod
    def drop_table(table: str) -> str:
        """Build drop table query."""
        return f"DROP TABLE IF EXISTS {table}"


class IsolationLevels(Enum):
    DEFERRED = "DEFERRED"
    EXCLUSIVE = "EXCLUSIVE"
    IMMEDIATE = "IMMEDIATE"


class SQLQueryParams(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"
