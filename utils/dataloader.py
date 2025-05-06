import sqlite3
import os
import pandas as pd
from typing import Optional


class FifaStatDatabase:
    def __init__(self, db_path: str = 'FifaStat.sqlite'):
        """
        Initialize the database connection

        Args:
            db_path (str): Path to the SQLite database file
        """
        # Ensure the database file exists
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")

        # Connect to the database
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row  # Return rows as dictionaries
        self.cursor = self.connection.cursor()

    def execute_query(self, query, params=None):
        """
        Execute an SQL query

        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query

        Returns:
            list: Query results
        """
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute_query_df(self, query: str, params=None):
        """
        Execute an SQL query and return the result as a pandas DataFrame

        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query

        Returns:
            pd.DataFrame: Query results as a DataFrame
        """
        if params:
            df = pd.read_sql_query(query, self.connection, params=params)
        else:
            df = pd.read_sql_query(query, self.connection)
        return df

    def get_all_tables(self):
        """
        Get a list of all tables in the database

        Returns:
            list: Names of all tables in the database
        """
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        result = self.execute_query(query)
        return [row['name'] for row in result]

    def get_table_header(self, table_name):
        """
        Get the header (column names) of a specific table

        Args:
            table_name (str): Name of the table

        Returns:
            list: Column names of the table
        """
        query = f"PRAGMA table_info({table_name})"
        result = self.execute_query(query)
        return [row['name'] for row in result]

    def get_tables_as_dataframes(self):
        """
        Get all tables in the database as pandas DataFrames

        Returns:
            dict: Dictionary of table names and their corresponding DataFrames
        """
        tables = self.get_all_tables()
        dataframes: dict[str, pd.DataFrame] = {}
        for table in tables:
            query = f"SELECT * FROM {table}"
            dataframes[table] = self.execute_query_df(query)
        return dataframes

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    # Example usage
    db_path = 'FifaStat.sqlite'
    with FifaStatDatabase(db_path) as db:
        tables = db.get_all_tables()
        print("Tables in the database:", tables)
        print("Number of tables:", len(tables))
        # Example query to get data from a specific table
        if tables:
            query = f"SELECT * FROM {tables[0]} LIMIT 10"
            results = db.execute_query(query)
            for row in results:
                # Convert Row object to dictionary for better readability
                print(dict(row))

        # Get all tables as DataFrames
        print("Match head data:")
        df_tables = db.get_tables_as_dataframes()
        if 'Match' in df_tables:
            print(df_tables['Match'].head(10))
        else:
            print("Match table not found")
