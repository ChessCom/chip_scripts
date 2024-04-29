import sqlite3

import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect(
    "C:/Users/pault/Documents/working_files/chip/v0L/cache/user_properties.sqlite3"
)

# Get a list of all tables in the database
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

# Loop through each table and dump it into a pandas DataFrame
for table in tables:
    table_name = table[0]
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    print(df)

# Close the connection to the database
conn.close()
