import connectorx as cx
import polars as pl
import logging

from .query import get_foreign_keys_query

logger = logging.getLogger(__name__)


class DB:

    conn = None

    def __init__(self, db, user, password, host="localhost", port=5432):
        self.conn = f"postgres://{user}:{password}@{host}:{port}/{db}"

    def get_joins(self, table, dataframe=True):
        query = get_foreign_keys_query(table)
        try:
            result = cx.read_sql(self.conn, query, return_type="polars")
        except Exception as e:
            raise e
        if dataframe:
            with pl.Config(tbl_rows=-1):
                # display all rows in the DataFrame
                return print(result)
        else:
            return result.to_dicts()

    def get_joined_query(self, table):
        """
        Returns a query to fetch joined data based on foreign keys for a specific table.

        Args:
            table (str): The name of the table to query joined data for.

        Returns:
            str: SQL query string to fetch joined data.
        """
        foreign_keys = self.get_joins(table, dataframe=False)
        if not foreign_keys:
            return f"SELECT * FROM {table};"

        joins = []
        for fk in foreign_keys:
            joins.append(
                f"\n\tLEFT JOIN {fk['referenced_table']} ON {table}."
                + f"{fk['foreign_key']} = {fk['referenced_table']}.{fk['column']}"
            )

        join_clause = " ".join(joins)
        return f"SELECT {table}.* FROM {table} {join_clause};"
