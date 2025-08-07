Make a generic sql query with auto-join for PostgreSQL towards foreign keys.


```python

from pg_autojoin.db import DB

conn = DB(db="mydb", user="me", password="1234")
# host and port are optional, defaults to localhost:5432

conn.get_joins(table='warehouse')

# list all related tables with a dataframe output
shape: (6, 4)
┌─────────────┬─────────────────────┬────────────────────┬────────┐
│ table       ┆ foreign_key         ┆ referenced_table   ┆ column │
│ ---         ┆ ---                 ┆ ---                ┆ ---    │
│ str         ┆ str                 ┆ str                ┆ str    │
╞═════════════╪═════════════════════╪════════════════════╪════════╡
│  warehouse  ┆ company_id          ┆ company            ┆ id     │
│  warehouse  ┆ partner_id          ┆ partner            ┆ id     │
│  warehouse  ┆ create_uid          ┆ user               ┆ id     │
│  warehouse  ┆ lot_stock_id        ┆ stock_location     ┆ id     │
│  warehouse  ┆ delivery_route_id   ┆ stock_route        ┆ id     │
│  warehouse  ┆ crossdock_route_id  ┆ stock_route        ┆ id     │
└─────────────┴─────────────────────┴────────────────────┴────────┘
```


```python

conn.get_joined_query(table='warehouse')

```

give an auto-joined query output

```sql
SELECT warehouse.* FROM warehouse
    LEFT JOIN company ON warehouse.company_id = company.id
    LEFT JOIN partner ON warehouse.partner_id = partner.id
    ...
```
