import csv
from trino.dbapi import connect

"""
This job extracts data from a Trino table and saves it to a CSV file.
"""

host = "localhost"  # Corrected assignment
port = 8081
user = "admin"
catalog = "iceberg"
schema = "gold"
table = "fct_simulation_agg"
output_file = f"./extract/{table}.csv"
query = f"""
    SELECT * FROM {catalog}.{schema}.{table}
"""

with connect(
    host=host, 
    port=port,
    user=user,
    catalog=catalog,
    schema=schema,
) as conn:
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    header = [desc[0] for desc in cur.description]

with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)
    writer.writerows(rows)
print(f"Data saved to {output_file}")