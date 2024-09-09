import mysql.connector
import pandas as pd

# Database configuration
db_config = {
        'user': 'root',      # Replace with your MySQL username
        'password': 'admin',  # Replace with your MySQL password
        'host': 'localhost',          # Replace with your MySQL host
        'database': 'commoditydataanaylsis'   # Replace with your MySQL database name
    }

# Path to the CSV file
csv_file_path = r'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\unique_markets_with_ids.csv'

# Read the CSV file into a DataFrame
final_df = pd.read_csv(csv_file_path)

# Establish database connection
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Table name
table_name = 'dim_marketdetails'

# Generate the dynamic SQL insert query
insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'

# Convert the DataFrame to a list of tuples
data = final_df.values.tolist()

# Execute the insert query for each row in the DataFrame
cursor.executemany(insert_query, data)
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Data successfully inserted into the unique_markets table.")
