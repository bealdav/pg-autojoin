import connectorx as cx
import polars as pl
from collections import defaultdict
import logging

from .query import (
    get_foreign_keys_query,
    get_columns_in_tables,
    get_json_col_in_tables,
    get_keys_in_json_col,
)

logger = logging.getLogger(__name__)


class SqlJoin:
    # dsn
    conn: str
    # alias mapping for table names
    aliases: dict = None
    # columns to search in foreign tables
    # the attribute is defined globally because in structured app
    # there is a recurrency in column names
    columns: list = None
    # columns in main table to ignore
    columns_to_ignore: list = None

    def __init__(
        self,
        db: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
    ) -> None:
        self.conn = f"postgres://{user}:{password}@{host}:{port}/{db}"
        # if exists this key will be used for json field
        json_key_pref = None
        fallback_json_key = None

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

    def get_joined_query(self, table: str) -> (str, str):
        """
        Args:
            table: The name of the table to query joined data for.

        Returns a query to fetch joined data
            SQL query string to fetch joined data based on foreign keys
            for a specific table.
        """

        def get_alias_count(tbl):
            result = f"{aliases.get(tbl, tbl)}"
            if tb_count[tbl] > 1:
                result += f"{tb_count[tbl]}"
            return result

        tb_alias, cols_by_tbl, json_by_tbl = {}, {}, {}
        col_str, joins, cols_list = "", [], []
        aliases = self.aliases or {}
        tb_count = defaultdict(int)
        foreigns = self.get_joins(table, dataframe=False)
        if self.columns and foreigns:
            tables = list(x["to_table"] for x in foreigns)
            # TODO remove
            tables.append(table)
            cols_by_tbl, json_by_tbl = self._search_columns(set(tables))
        tb_alias = aliases.get(table, "")
        tb_count[table] = 1
        foreign_cols = {}
        for fk in foreigns:
            if fk['foreign_key'] in self.columns_to_ignore:
                continue
            # we record the count of each table to handle aliases
            tb_count[fk["to_table"]] += 1
            # search for an existing aliases i.e.: u, c or u3
            foreign_alias = get_alias_count(fk["to_table"])
            if cols_by_tbl.get(fk["to_table"]):
                for col in cols_by_tbl[fk["to_table"]]:
                    if fk["foreign_key"] in foreign_cols.keys():
                        # a field is already set with this foreign key
                        new_col = f"{foreign_alias}.{col}"
                        if json_by_tbl.get(fk["to_table"]):
                            keys = json_by_tbl.get(fk["to_table"]).get(col)
                            if (
                                keys
                                and self.fallback_json_key in keys
                                and self.json_key_pref in keys
                            ):
                                new_col = (
                                    f"CASE WHEN {foreign_alias}.{col}->>'"
                                    + f"{self.json_key_pref}' IS NOT NULL THEN "
                                    + f"{foreign_alias}.{col}->>'{self.json_key_pref}' "
                                    + f"ELSE {foreign_alias}.{col}->>'"
                                    + f"{self.fallback_json_key}' END"
                                )
                            else:
                                # there is a jsonb in selected column,
                                # but identified keys doesn't allow to aggregate data
                                # then we stop process for this table
                                continue
                        # We complete col with other col
                        foreign_cols[fk["foreign_key"]] += f" || ', ' || {new_col}"
                    else:
                        # here we keep the original column i.e. partner_id
                        foreign_cols[fk["foreign_key"]] = f"{foreign_alias}.{col}"
            joins.append(
                # compute join: i.e. LEFT JOIN res_users u2 ON u2.id = u.create_uid
                f"\n  LEFT JOIN {fk['to_table']} {foreign_alias} ON "
                + f"{foreign_alias or fk['to_table']}"
                + f".{fk['column']} = {tb_alias or table}.{fk['foreign_key']}"
            )
        if foreign_cols:
            col_str = ", ".join([f"{string} AS {key}" for key, string in foreign_cols.items()])
        all_cols = (
            self._get_df(get_columns_in_tables([table])).get_column("column").to_list()
        )
        wished_cols = [x for x in all_cols if x not in self.columns_to_ignore]
        other_cols = [
            f"{tb_alias or table}.{x}" for x in wished_cols if x not in foreign_cols.keys()
        ]
        join_clause = " ".join(joins)
        if not joins:
            logger.info(f"No joins found for '{table}' table.")
            return False, False
        sql = f"SELECT {col_str}\n"
        if other_cols:
            sql += f", {', '.join(other_cols)}"
        sql += f"\nFROM {table} {tb_alias} {join_clause}"
        logger.info(f"Sql generated for '{table}' table")
        return sql

    def _search_columns(self, tables: list):
        """search for self.columns in the given tables."""

        df = self._get_df(
            get_columns_in_tables(tables=tables, column_names=self.columns)
        )
        cols_by_tbl = {
            x["table"]: x["column"]
            for x in df.group_by("table").agg(pl.col("column")).to_dicts()
        }
        df = self._get_df(
            get_json_col_in_tables(tables=tables, column_names=self.columns)
        )
        json_by_tbl = {
            x["table"]: x["column"]
            for x in df.group_by("table").agg(pl.col("column")).to_dicts()
        }
        json_keys = defaultdict(dict)

        def get_dict_keys_by_col(df):
            return {
                row["col"]: row["key"]
                for row in df.group_by("col").agg(pl.col("key")).to_dicts()
            }

        for tbl, cols in json_by_tbl.items():
            sql = (
                get_keys_in_json_col(tbl, cols)
                .replace("\n", " ")
                .replace("',)", "')")  # tuple with one value only
            )
            sql = self._get_df(sql).get_column("?column?").to_list()[0]
            keys_by_col = get_dict_keys_by_col(self._get_df(sql))
            json_keys[tbl] = keys_by_col
        return cols_by_tbl, json_keys

    def _get_df(self, sql):
        "Get dataframe from an sql query"
        return cx.read_sql(self.conn, sql, return_type="polars")

    def set_json_key_pref(self, key: str) -> None:
        self.json_key_pref = key

    def set_fallback_json_key(self, key: str) -> None:
        self.fallback_json_key = key

    def set_aliases(self, aliases: dict[str, str]) -> None:
        """
        Sets an aliases mapping for table names.

        Args:
            alias (dict): A dictionary mapping original table names to their aliases.
        """
        self.aliases = aliases

    def set_columns_to_ignore(self, columns: list) -> None:
        """
        Sets columns to ignore from main table
        """
        self.columns_to_ignore = columns

    def set_columns_to_retrieve(self, columns: list) -> None:
        """
        Sets columns to search in foreign tables
        """
        self.columns = columns
