import pytest

from src.config.database import (
    AlterTypes,
    DDLQueryBuilder,
    DMLQueryBuilder,
    SQLQueryParams,
)


class TestDMLQueryBuilder:
    def test_select_basic(self):
        """Test basic SELECT query building."""
        query = DMLQueryBuilder.select(
            columns=["id", "name"],
            table="users",
            where_clause=None,
            order_by_col=None,
            group_by_col=None,
            asc_desc=None,
        )

        expected = "SELECT id, name FROM users"
        assert query == expected

    def test_select_with_where_clause(self):
        """Test SELECT query with WHERE clause."""
        query = DMLQueryBuilder.select(
            columns=["*"],
            table="products",
            where_clause="price > 100",
            order_by_col=None,
            group_by_col=None,
            asc_desc=None,
        )

        expected = "SELECT * FROM products WHERE price > 100"
        assert query == expected

    def test_select_with_order_by_ascending(self):
        """Test SELECT query with ORDER BY ascending."""
        query = DMLQueryBuilder.select(
            columns=["name", "age"],
            table="users",
            where_clause=None,
            order_by_col="age",
            group_by_col=None,
            asc_desc=SQLQueryParams.ASCENDING,
        )

        expected = "SELECT name, age FROM users ORDER BY age ASC"
        assert query == expected

    def test_select_with_order_by_descending(self):
        """Test SELECT query with ORDER BY descending."""
        query = DMLQueryBuilder.select(
            columns=["name", "salary"],
            table="employees",
            where_clause=None,
            order_by_col="salary",
            group_by_col=None,
            asc_desc=SQLQueryParams.DESCENDING,
        )

        expected = "SELECT name, salary FROM employees ORDER BY salary DESC"
        assert query == expected

    def test_select_with_group_by(self):
        """Test SELECT query with GROUP BY."""
        query = DMLQueryBuilder.select(
            columns=["department", "COUNT(*)"],
            table="employees",
            where_clause=None,
            order_by_col=None,
            group_by_col="department",
            asc_desc=None,
        )

        expected = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        assert query == expected

    def test_select_with_limit(self):
        """Test SELECT query with LIMIT."""
        query = DMLQueryBuilder.select(
            columns=["*"],
            table="posts",
            where_clause=None,
            order_by_col=None,
            group_by_col=None,
            asc_desc=None,
            limit=10,
        )

        expected = "SELECT * FROM posts LIMIT 10"
        assert query == expected

    def test_select_all_params(self):
        """Test complex SELECT query with all clauses."""
        query = DMLQueryBuilder.select(
            columns=["name", "age", "department"],
            table="employees",
            where_clause="age > 25",
            order_by_col="age",
            group_by_col="department",
            asc_desc=SQLQueryParams.DESCENDING,
            limit=5,
        )

        expected = "SELECT name, age, department FROM employees WHERE age > 25 GROUP BY department ORDER BY age DESC LIMIT 5"
        assert query == expected

    def test_insert_query(self):
        """Test INSERT query building."""
        query = DMLQueryBuilder.insert(columns=["name", "email", "age"], table="users")

        expected = "INSERT INTO users (name, email, age) VALUES (?, ?, ?)"
        assert query == expected

    def test_update_query(self):
        """Test UPDATE query building."""
        query = DMLQueryBuilder.update(
            table="users",
            columns=["name", "email"],
            values=["John Doe", "john@example.com"],
            where_clause="id = 1",
        )

        expected = "UPDATE users SET name = ?, email = ? WHERE id = 1"
        assert query == expected

    def test_update_single_column(self):
        """Test UPDATE query with single column."""
        query = DMLQueryBuilder.update(
            table="products", columns=["price"], values=["29.99"], where_clause="id = 5"
        )

        expected = "UPDATE products SET price = ? WHERE id = 5"
        assert query == expected

    def test_update_mismatched_columns_values(self):
        """Test UPDATE query with mismatched columns and values."""
        with pytest.raises(
            ValueError, match="Each column must have a corresponding update value!"
        ):
            DMLQueryBuilder.update(
                table="users",
                columns=["name", "email"],
                values=["John Doe"],  # Missing value for email
                where_clause="id = 1",
            )

    def test_delete_query(self):
        """Test DELETE query building."""
        query = DMLQueryBuilder.delete(table="users", where_clause="age < 18")

        expected = "DELETE FROM users WHERE age < 18"
        assert query == expected

    def test_delete_specific_record(self):
        """Test DELETE query for specific record."""
        query = DMLQueryBuilder.delete(table="posts", where_clause="id = 123")

        expected = "DELETE FROM posts WHERE id = 123"
        assert query == expected


class TestDDLQueryBuilder:
    def test_create_table_basic(self):
        """Test basic CREATE TABLE query."""
        query = DDLQueryBuilder.create(
            table="users",
            columns=[("id", "INTEGER"), ("name", "TEXT"), ("email", "TEXT")],
            primary_key_col="id",
        )

        expected = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
        assert query == expected

    def test_create_table_different_types(self):
        """Test CREATE TABLE with different column types."""
        query = DDLQueryBuilder.create(
            table="products",
            columns=[
                ("id", "INTEGER"),
                ("name", "TEXT NOT NULL"),
                ("price", "REAL"),
                ("in_stock", "BOOLEAN"),
                ("created_at", "TIMESTAMP"),
            ],
            primary_key_col="id",
        )

        expected = "CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL, in_stock BOOLEAN, created_at TIMESTAMP)"
        assert query == expected

    def test_create_table_invalid_primary_key(self):
        """Test CREATE TABLE with invalid primary key column."""
        with pytest.raises(
            ValueError,
            match="Primary key column must be one of the desired columns in the table!",
        ):
            DDLQueryBuilder.create(
                table="users",
                columns=[("id", "INTEGER"), ("name", "TEXT")],
                primary_key_col="invalid_column",
            )

    def test_alter_table_add_column(self):
        """Test ALTER TABLE ADD COLUMN."""
        query = DDLQueryBuilder.alter(
            table="users", operation=AlterTypes.ADD, column_def="phone TEXT"
        )

        expected = "ALTER TABLE users ADD COLUMN phone TEXT"
        assert query == expected

    def test_alter_table_add_column_missing_def(self):
        """Test ALTER TABLE ADD COLUMN without column definition."""
        with pytest.raises(ValueError, match="column_def required for ADD operation"):
            DDLQueryBuilder.alter(table="users", operation=AlterTypes.ADD)

    def test_alter_table_drop_column(self):
        """Test ALTER TABLE DROP COLUMN."""
        query = DDLQueryBuilder.alter(
            table="users", operation=AlterTypes.DROP, old_column="phone"
        )

        expected = "ALTER TABLE users DROP COLUMN phone"
        assert query == expected

    def test_alter_table_drop_column_missing_name(self):
        """Test ALTER TABLE DROP COLUMN without column name."""
        with pytest.raises(ValueError, match="old_column required for DROP operation"):
            DDLQueryBuilder.alter(table="users", operation=AlterTypes.DROP)

    def test_alter_table_rename_column(self):
        """Test ALTER TABLE RENAME COLUMN."""
        query = DDLQueryBuilder.alter(
            table="users",
            operation=AlterTypes.RENAME_COLUMN,
            old_column="email",
            new_column="email_address",
        )

        expected = "ALTER TABLE users RENAME COLUMN email TO email_address"
        assert query == expected

    def test_alter_table_rename_column_missing_params(self):
        """Test ALTER TABLE RENAME COLUMN with missing parameters."""
        with pytest.raises(
            ValueError,
            match="Both old_column and new_column required for RENAME_COLUMN",
        ):
            DDLQueryBuilder.alter(
                table="users",
                operation=AlterTypes.RENAME_COLUMN,
                old_column="email",
                # Missing new_column
            )

    def test_alter_table_rename_table(self):
        """Test ALTER TABLE RENAME TO."""
        query = DDLQueryBuilder.alter(
            table="users",
            operation=AlterTypes.RENAME_TABLE,
            new_column="customers",  # Note: new_column is reused for new table name
        )

        expected = "ALTER TABLE users RENAME TO customers"
        assert query == expected

    def test_alter_table_rename_table_missing_name(self):
        """Test ALTER TABLE RENAME TO without new table name."""
        with pytest.raises(
            ValueError,
            match="new_column \\(new table name\\) required for RENAME_TABLE",
        ):
            DDLQueryBuilder.alter(table="users", operation=AlterTypes.RENAME_TABLE)

    def test_alter_table_invalid_operation(self):
        """Test ALTER TABLE with invalid operation."""
        with pytest.raises(ValueError, match="Unsupported ALTER operation"):
            # Create a mock enum value that doesn't exist
            class MockEnum:
                def __init__(self, value):
                    self.value = value

                def __eq__(self, other):
                    return False

            DDLQueryBuilder.alter(table="users", operation=MockEnum("INVALID_OP"))

    def test_drop_table(self):
        """Test DROP TABLE query."""
        query = DDLQueryBuilder.drop_table("old_table")

        expected = "DROP TABLE IF EXISTS old_table"
        assert query == expected

    def test_drop_table_different_names(self):
        """Test DROP TABLE with different table names."""
        tables = ["users", "products", "orders", "temp_data"]

        for table in tables:
            query = DDLQueryBuilder.drop_table(table)
            expected = f"DROP TABLE IF EXISTS {table}"
            assert query == expected


class TestSQLQueryParams:
    def test_sql_query_params_values(self):
        """Test SQLQueryParams enum values."""
        assert SQLQueryParams.ASCENDING.value == "asc"
        assert SQLQueryParams.DESCENDING.value == "desc"


class TestAlterTypes:
    def test_alter_types_values(self):
        """Test AlterTypes enum values."""
        assert AlterTypes.ADD.value == "ADD"
        assert AlterTypes.DROP.value == "DROP"
        assert AlterTypes.RENAME_COLUMN.value == "RENAME_COLUMN"
        assert AlterTypes.RENAME_TABLE.value == "RENAME_TABLE"
