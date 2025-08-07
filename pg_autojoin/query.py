# https://www.baeldung.com/sql/list-table-foreign-keys


def get_foreign_keys_query(table: str):
    """
    Returns a query to fetch foreign keys for a specific table.

    Args:
        table (str): The name of the table to query foreign keys for.

    Returns:
        str: SQL query string to fetch foreign keys.
    """
    return f"""
      SELECT 
          tc.table_name AS "table",
          kcu.column_name AS foreign_key,
          ccu.table_name AS to_table,
          ccu.column_name AS "column"
      FROM 
          information_schema.table_constraints AS tc
      JOIN 
          information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
      JOIN 
          information_schema.constraint_column_usage AS ccu
          ON kcu.constraint_name = ccu.constraint_name
      WHERE 
          tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_name = '{table}'
      ORDER BY ccu.table_name
      """


def get_columns_in_tables(tables: list, column_names: list):
    return f"""
    SELECT table_name AS "table", column_name AS "column" 
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name IN {tuple(tables)}
      AND column_name IN {tuple(column_names)}
    """
