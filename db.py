"""Contains helper functions for database operations."""

import sqlite3


def sqlite3_execute_list(db_file, sql_list):
    """Executes a list of sql statements on SQLite3 database.

    Args:
        db_file (str): The full file path of the SQLite database
        sql_list (list): A list of 2-tuples, i.e. (sql, parameters).
            See https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.execute

    Returns: None

    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    for sql in sql_list:
        cursor.execute(sql[0], sql[1])
    conn.commit()
    conn.close()


class DatabaseCursor:
    """Represents a cursor from a database connection.

    This class should be used with the "with" statement.
    It will create a cursor when enter and close the connection when exit.

    Examples:
        with DatabaseCursor(sqlite3.connect(db_file)) as cursor:
            cursor.execute(sql_query)
            cursor.fetchall()

    """
    def __init__(self, connection):
        """Accepts a database connection.

        Args:
            connection: A database connection
        """
        self.conn = connection

    def __enter__(self):
        cursor = self.conn.cursor()
        return cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.conn.close()
