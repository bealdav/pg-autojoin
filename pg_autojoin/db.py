import psycopg
from psycopg.rows import dict_row
import connectorx as cx
import polars as pl
import logging

from .query import FOREIGN_KEYS_QUERY

logger = logging.getLogger(__name__)


class DB:

    conn = None
    # default engine for database connection
    # other than 'connectorx' dive in psycopg connection
    engine = "connectorx"

    def __init__(
        self, dbname, user, password, host="localhost", port=5432, engine=None
    ):
        self.engine = engine or self.engine
        if self.engine == "connectorx":
            self.conn = f"postgres://{user}:{password}@{host}:{port}/{dbname}"
        else:
            try:
                self.conn = psycopg.connect(
                    f"host={host} port={port} dbname={dbname} user={user} "
                    + f" password={password}",
                    row_factory=dict_row,
                )
                # with read_only, commit and rollback are the same
                self.conn.read_only = True
            except BaseException:
                self.conn.rollback()
            else:
                self.conn.commit()

    def get_joins(self, table=None, output="dataframe"):
        query = FOREIGN_KEYS_QUERY
        if self.engine == "connectorx":
            df = cx.read_sql(self.conn, FOREIGN_KEYS_QUERY, return_type="polars")
            result = df
            if table:
                result = df.filter(pl.col("primary_table") == table)
            if output != "dataframe":
                result = result.to_dicts()
        else:
            if table:
                query = query.replace("WHERE", f"WHERE ccu.table_name = '{table}' AND")
                logger.info(query)
            try:
                cur = self.conn.execute(query)
            except psycopg.OperationalError as e:
                raise e
            result = cur.fetchall()
        if isinstance(result, pl.dataframe.frame.DataFrame):
            with pl.Config(tbl_rows=-1):
                return print(result)
        return result

    def close(self):
        if self.engine == "connectorx":
            # connectorx doesn't need to close connection
            return
        self.conn.close()

    def execute(self, query):
        """It's a commodity method just for testing purposes."""
        cur = self.conn.execute(query)
        return cur.fetchall()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
