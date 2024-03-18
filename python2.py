'''extract the total quantities of each item bought per customer aged 18-35.
- For each customer, get the sum of each item
- Items with no purchase (total quantity=0) should be omitted from the final
list
- No decimal points allowed (The company doesn’t sell half of an item ;) )'''

# using SQL

import sqlite3
import pandas as pd

# Connect to the SQLite3 database
conn = sqlite3.connect('/Users/nitheshreddy/Downloads/Data Engineer_ETL Assignment.db')
cursor = conn.cursor()

# SQL query to extract total quantities of each item bought per customer aged 18-35
sql_query = """
SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS total_quantity
FROM customer c
JOIN sales s ON c.customer_id = s.customer_id
JOIN orders o ON s.sales_id = o.sales_id
JOIN items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
GROUP BY c.customer_id, i.item_name
HAVING total_quantity > 0
"""

# Execute the SQL query and fetch the results
cursor.execute(sql_query)
results = cursor.fetchall()

# Convert the results to a Pandas DataFrame
df = pd.DataFrame(results, columns=['Customer_ID', 'Age', 'Item_Name', 'Total_Quantity'])

# Store the DataFrame to a CSV file with semicolon delimiter
df.to_csv('output_file_sql.csv', sep=';', index=False)
