'''extract the total quantities of each item bought per customer aged 18-35.
- For each customer, get the sum of each item
- Items with no purchase (total quantity=0) should be omitted from the final
list
- No decimal points allowed (The company doesn’t sell half of an item ;) )'''

# using pandas

import pandas as pd

# Load data from CSV file or any other source
df = pd.read_csv('/Users/nitheshreddy/Downloads/Data Engineer_ETL Assignment.csv')

# Filter customers aged 18-35
filtered_df = df[(df['Age'] >= 18) & (df['Age'] <= 35)]

# Calculate total quantities per item per customer
grouped_df = filtered_df.groupby(['Customer_ID', 'Item_Name']).agg({'Quantity': 'sum'}).reset_index()

# Remove rows with total quantity=0
filtered_grouped_df = grouped_df[grouped_df['Quantity'] > 0]

# Store the filtered DataFrame to a CSV file with semicolon delimiter
filtered_grouped_df.to_csv('output_file_pandas.csv', sep=';', index=False)
