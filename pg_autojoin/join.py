import connectorx as cx
import polars as pl
from collections import defaultdict
import logging

from .query import get_foreign_keys_query, get_columns_in_tables

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

    def __init__(
        self,
        db: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
    ) -> None:
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

    def get_joined_query(self, table: str) -> (str, str):
        """
        Returns a query to fetch joined data based on foreign keys for a specific table.

        Args:
            table: The name of the table to query joined data for.

        Returns:
            SQL query string to fetch joined data.
            string for asterisk columns like `table.*`.
        """

        def as_clause_override(value):
            # TODO allow to have space instead of _
            result = f"_{value}"
            if value == "name":
                result = ""
            return result

        def get_alias_count(tbl):
            result = f"{aliases.get(tbl, tbl)}"
            if tb_count[tbl] > 1:
                result += f"{tb_count[tbl]}"
            return result

        joins, col_names = [], []
        tb_alias, cols_by_tbl = {}, {}
        aliases = self.aliases or {}
        tb_count = defaultdict(int)
        foreigns = self.get_joins(table, dataframe=False)
        if self.columns and foreigns:
            cols_by_tbl = self._search_columns(set(x["to_table"] for x in foreigns))
        tb_alias = aliases.get(table, "")
        tb_count[table] = 1
        for fk in foreigns:
            # we record the count of each table to handle aliases
            tb_count[fk["to_table"]] += 1
            # search for an existing aliases i.e.: u, c or u3
            foreign_alias = get_alias_count(fk["to_table"])
            if cols_by_tbl.get(fk["to_table"]):
                # i.e. cols_by_tbl contains such data
                # {'res_company': ['name'], 'res_partner': ['name', 'ref']}
                col_names.append(
                    [
                        f'{foreign_alias}.{x} AS "{foreign_alias}{as_clause_override(x)}"'
                        for x in cols_by_tbl[fk["to_table"]]
                    ]
                )
            joins.append(
                # compute join: i.e. LEFT JOIN res_users u2 ON u2.id = u.create_uid
                f"\n  LEFT JOIN {fk['to_table']} {foreign_alias} ON "
                + f"{foreign_alias or fk['to_table']}"
                + f".{fk['column']} = {tb_alias or table}.{fk['foreign_key']}"
            )
        cols_list = []
        if col_names:
            # col_names contains such data [['c.name'], ['p.name', 'p.ref']]
            cols = [x.split(",") for x in [",".join(x) for x in col_names]]
            # we fill cols_list
            [cols_list.extend(x) for x in cols]
            sql = f"SELECT {tb_alias + '.'}* , {col_names} FROM {table} {tb_alias} "
        col_str = cols_list and ", ".join(cols_list) + "," or ""
        join_clause = " ".join(joins)
        if not joins:
            logger.info(f"No joins found for '{table}' table.")
            return False, False
        asterisk_cols = f"{tb_alias}.*"
        sql = f"SELECT {col_str} {asterisk_cols}\nFROM {table} {tb_alias} {join_clause}"
        logger.info(f"Sql generated for '{table}' table")
        return sql, asterisk_cols

    def _search_columns(self, tables: list):
        """search for self.columns in the given tables."""
        sql = get_columns_in_tables(tables=tables, column_names=self.columns)
        df = cx.read_sql(self.conn, sql, return_type="polars")
        dicts = df.group_by("table").agg(pl.col("column")).to_dicts()
        return {x["table"]: x["column"] for x in dicts}

    def set_aliases(self, aliases: dict[str, str]) -> None:
        """
        Sets an aliases mapping for table names.

        Args:
            alias (dict): A dictionary mapping original table names to their aliases.
        """
        self.aliases = aliases

    def set_columns_to_retrieve(self, columns: list) -> None:
        """
        Sets an alias mapping for table names.

        Args:
            alias (dict): A dictionary mapping original table names to their aliases.
        """
        self.columns = columns
