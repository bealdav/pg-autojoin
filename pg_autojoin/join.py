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
        tb_alias, cols_by_tbl, json_by_tbl = {}, {}, {}
        aliases = self.aliases or {}
        tb_count = defaultdict(int)
        foreigns = self.get_joins(table, dataframe=False)
        if self.columns and foreigns:
            cols_by_tbl, json_by_tbl = self._search_columns(
                set(x["to_table"] for x in foreigns)
            )
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
                for col in cols_by_tbl[fk["to_table"]]:
                    already_col = [x for x in col_names if fk["foreign_key"] in x]
                    if already_col:
                        # a field is already set with this foreign key
                        if json_by_tbl.get(fk["to_table"]):
                            # there is a jsonb in selected column,
                            # we can't aggregate easily jsonb with other column
                            # then we stop process for this table
                            continue
                        idx = already_col[0].find(fk["foreign_key"])
                        new_col = f"{foreign_alias}.{col}"
                        replacement = already_col[0].replace(
                            " AS", f" || ', ' || {new_col} AS"
                        )
                        col_names[col_names.index(already_col[0])] = replacement
                    else:
                        # here we keep the original column i.e. partner_id
                        col_names.append(
                            f'{foreign_alias}.{col} AS "{fk["foreign_key"]}'
                            + f'{as_clause_override(col)}"'
                        )
            joins.append(
                # compute join: i.e. LEFT JOIN res_users u2 ON u2.id = u.create_uid
                f"\n  LEFT JOIN {fk['to_table']} {foreign_alias} ON "
                + f"{foreign_alias or fk['to_table']}"
                + f".{fk['column']} = {tb_alias or table}.{fk['foreign_key']}"
            )
        col_str, cols_list = "", []
        if col_names:
            # col_names contains such data [['c.name'], ['p.name', 'p.ref']]
            cols = []
            for x in col_names:
                cols.append(x)
            col_str = ", ".join(col_names) + ","
        join_clause = " ".join(joins)
        if not joins:
            logger.info(f"No joins found for '{table}' table.")
            return False, False
        asterisk_cols = f"{tb_alias}.*"
        sql = f"SELECT {col_str} {tb_alias or table}{asterisk_cols}\n"
        sql += f"FROM {table} {tb_alias} {join_clause}"
        logger.info(f"Sql generated for '{table}' table")
        return sql, asterisk_cols

    def _search_columns(self, tables: list):
        """search for self.columns in the given tables."""
        sql = get_columns_in_tables(tables=tables, column_names=self.columns)
        df = cx.read_sql(self.conn, sql, return_type="polars")
        cols_by_tbl = {
            x["table"]: x["column"]
            for x in df.group_by("table").agg(pl.col("column")).to_dicts()
        }
        json_by_tbl = {
            x["table"]: x["column"]
            for x in df.filter(pl.col("data_type") == "jsonb")
            .select(["table", "column"])
            .to_dicts()
        }
        return cols_by_tbl, json_by_tbl

    def set_aliases(self, aliases: dict[str, str]) -> None:
        """
        Sets an aliases mapping for table names.

        Args:
            alias (dict): A dictionary mapping original table names to their aliases.
        """
        self.aliases = aliases

    def set_columns_to_retrieve(self, columns: list) -> None:
        """
        Sets columns to search in foreign tables

        Args:
            columns: list of columns to search in foreign tables
        """
        self.columns = columns
