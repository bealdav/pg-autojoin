import connectorx as cx
import polars as pl
from collections import defaultdict
import logging

from .query import get_foreign_keys_query

logger = logging.getLogger(__name__)


class DB:

    # dsn
    conn: str
    # alias mapping for table names
    alias: dict = None

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

    def set_alias(self, alias: dict[str, str]) -> None:
        """
        Sets an alias mapping for table names.

        Args:
            alias (dict): A dictionary mapping original table names to their aliases.
        """
        self.alias = alias

    def get_joined_query(self, table: str) -> str:
        """
        Returns a query to fetch joined data based on foreign keys for a specific table.

        Args:
            table (str): The name of the table to query joined data for.

        Returns:
            str: SQL query string to fetch joined data.
        """

        def get_alias_cnt(tbl):
            result = f"{self.alias.get(tbl, tbl)}"
            if tbl_cnt[tbl] > 1:
                result += f"{tbl_cnt[tbl]}"
            return result

        joins = []
        tbl_cnt = defaultdict(int)
        foreign_keys = self.get_joins(table, dataframe=False)
        t_alias = self.alias.get(table, "")
        tbl_cnt[table] = 1
        for fk in foreign_keys:
            # we record the count of each table to handle aliases
            tbl_cnt[fk["to_table"]] += 1
            foreign_alias = get_alias_cnt(fk["to_table"])
            joins.append(
                f"\n  LEFT JOIN {fk['to_table']} {foreign_alias} ON "
                + f"{foreign_alias or fk['to_table']}"
                + f".{fk['column']} = {t_alias or table}.{fk['foreign_key']}"
            )
        join_clause = " ".join(joins)
        sql = f"SELECT {t_alias + '.'}* FROM {table} {t_alias} {join_clause}"
        return print(sql)
