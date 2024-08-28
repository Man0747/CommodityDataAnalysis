import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import mysql.connector

# Function to get dates from the MySQL tables
def get_dates_from_db():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="commoditydataanaylsis"
    )
    cursor = connection.cursor()

    # Get the last processed date
    cursor.execute("SELECT run_date FROM LastProcessed ORDER BY id DESC LIMIT 1")
    start_date = cursor.fetchone()[0]

    # Get the last run date
    cursor.execute("SELECT run_date FROM Lastrun ORDER BY id DESC LIMIT 1")
    end_date = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return start_date, end_date

# Function to process files within a date range
def process_files(start_date, end_date):
    path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject'
    
    # Increment start_date by 1 day before starting the processing
    start_date += timedelta(days=1)

    # Loop through each date in the range
    date = start_date
    while date <= end_date:
        year = date.strftime("%Y")
        month = str(date.month)
        day = str(date.day)
        
        input_path = os.path.join(path, 'Bronze', year, month, day)
        output_path = os.path.join(path, 'Silver', year, month, day)
        
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        csv_files = glob.glob(input_path + "/*.csv")

        for file in csv_files:
            df = pd.read_csv(file)

            if 'Arrival_Date' in df.columns:
                df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%d/%m/%Y')
                df['Arrival_Date_String'] = df['Arrival_Date'].dt.strftime('%d-%b-%Y')
                df['Arrival_Date_Key'] = df['Arrival_Date'].dt.strftime('%Y%m%d').astype(int)

            if 'Commodity_Code' in df.columns:
                df_cleaned = df.drop(columns=['Commodity_Code'])
            else:
                df_cleaned = df

            column_mapping = {
                'Min_x0020_Price': 'Min_Price',
                'Max_x0020_Price': 'Max_Price',
                'Modal_x0020_Price': 'Modal_Price',
                'Min_Price': 'Min_Price',
                'Max_Price': 'Max_Price',
                'Modal_Price': 'Modal_Price'
            }
            df.rename(columns=column_mapping, inplace=True)

            original_filename = os.path.basename(file)
            new_filename = f"Silver_{original_filename}"
            output_file = os.path.join(output_path, new_filename)

            df_cleaned.to_csv(output_file, index=False)

        print(f"Processed {len(csv_files)} files and saved to {output_path}")

        # Move to the next day
        date += timedelta(days=1)

# Get dates from the database
start_date, end_date = get_dates_from_db()

# Process files between the incremented start date and the end date
process_files(start_date, end_date)
