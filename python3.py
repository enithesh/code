'''extract the total quantities of each item bought per customer aged 18-35.
- For each customer, get the sum of each item
- Items with no purchase (total quantity=0) should be omitted from the final
list
- No decimal points allowed (The company doesn’t sell half of an item ;) )'''

# using pandas

import pandas as pd
import sqlite3

# Connect to the SQLite database file
conn = sqlite3.connect("/Users/nitheshreddy/Downloads/Data Engineer_ETL Assignment.db")

# Read tables into Pandas DataFrames
df_customers = pd.read_sql_query("SELECT * FROM customers", conn)
df_sales = pd.read_sql_query("SELECT * FROM sales", conn)
df_orders = pd.read_sql_query("SELECT * FROM orders", conn)
df_items = pd.read_sql_query("SELECT * FROM items", conn)

# Merge tables
merged_df = pd.merge(df_customers, df_sales, on='customer_id')
merged_df = pd.merge(merged_df, df_orders, on='sales_id')
merged_df = pd.merge(merged_df, df_items, on='item_id')

# Filter customers aged 18-35
merged_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35)]

# Group by customer_id and item_id and sum quantity
result_df = merged_df.groupby(['customer_id', 'item_name'])['quantity'].sum().reset_index()

# Convert quantity to integer type to remove decimal points
result_df['quantity'] = result_df['quantity'].astype(int)

# Filter out items with total quantity = 0
result_df = result_df[result_df['quantity'] > 0]

# Merge with customers to get age
result_df = pd.merge(result_df, df_customers[['customer_id', 'age']], on='customer_id')

# Reorder columns
result_df = result_df[['customer_id', 'age', 'item_name', 'quantity']]

# Rename columns for clarity
result_df = result_df.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item', 'quantity': 'Quantity'})

print(result_df)
result_df.to_csv("/Users/nitheshreddy/Downloads/Data Engineer_ETL Assignment/output_file_pandas.csv", sep=';', index=False)
