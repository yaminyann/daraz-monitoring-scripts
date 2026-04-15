import sqlite3
import pandas as pd

# Connect to .db file
conn = sqlite3.connect("product_list.db")

# Load table into pandas
df = pd.read_sql("SELECT * FROM product_list", conn)

# Export to Excel
df.to_excel("product_list.xlsx", index=False)

conn.close()

print("✅ Done! Excel file created.")