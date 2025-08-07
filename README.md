Make a generic sql query with auto-join for PostgreSQL towards foreign keys.


# Usage

```python

from pg_autojoin.db import DB

conn = DB(db="mydb", user="me", password="1234")
# host and port are optional, default to localhost

conn.get_joins(table="res_users")

# list all related tables with a dataframe output
shape: (4, 4)
┌───────────┬─────────────┬─────────────┬────────┐
│ table     ┆ foreign_key ┆ to_table    ┆ column │
│ ---       ┆ ---         ┆ ---         ┆ ---    │
│ str       ┆ str         ┆ str         ┆ str    │
╞═══════════╪═════════════╪═════════════╪════════╡
│ res_users ┆ company_id  ┆ res_company ┆ id     │
│ res_users ┆ partner_id  ┆ res_partner ┆ id     │
│ res_users ┆ create_uid  ┆ res_users   ┆ id     │
│ res_users ┆ write_uid   ┆ res_users   ┆ id     │
└───────────┴─────────────┴─────────────┴────────┘

Dataset comes from [Odoo business app](https://github.com/odoo/odoo), but you can use it with any PostgreSQL database.

```


```python

# define your own alias, but it's not a mandatory step
conn.set_alias({"res_company": "c", "res_partner": "p", 'res_users': 'u'})

conn.get_joined_query(table="res_users")

```

give an auto-joined query output


```sql
SELECT u.* FROM res_users u 
  LEFT JOIN res_company c ON c.id = u.company_id 
  LEFT JOIN res_partner p ON p.id = u.partner_id 
  LEFT JOIN res_users u2 ON u2.id = u.create_uid 
  LEFT JOIN res_users u3 ON u3.id = u.write_uid
```


# Use case

You may dynamically want to produce dataframes with relevant data only based on a source table.

Have directly all joins might help a lot to specify the data to get back.


# Installation

```bash
pip install git+https://github.com/bealdav/pg-autojoin.git@main
```
