#Create a Python script that can connect to the SQLite3 database provided

import sqlite3

# Connect to the SQLite3 database
conn = sqlite3.connect('/Users/nitheshreddy/Downloads/Data Engineer_ETL Assignment.db.db')
cursor = conn.cursor()

# Close the database connection
conn.close()
