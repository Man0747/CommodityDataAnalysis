import pandas as pd
import os
import glob
from datetime import datetime, timedelta

# Function to process files within a date range
def process_files(start_date, end_date):
    
    path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject'
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Loop through each date in the range
    date = start_date
    while date <= end_date:
        # Extract year, month, and day as strings without leading zeros
        year = date.strftime("%Y")
        month = str(date.month)  # Remove leading zero
        day = str(date.day)      # Remove leading zero
        
        # Construct the full input path
        input_path = os.path.join(path, 'Bronze', year, month, day)
        # print(f"Input path: {input_path}")  # Debugging line

        # Define the output path (Silver layer)
        output_path = os.path.join(path, 'Silver', year, month, day)
        # print(f"Output path: {output_path}")  # Debugging line

        # Check if the output directory exists, and create it if not
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        # Get all CSV files from the input directory
        csv_files = glob.glob(input_path + "/*.csv")

        # Process each CSV file (for example, cleaning the data)
        for file in csv_files:
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file)

            # Convert the 'date' column to the desired format if it exists
            if 'Arrival_Date' in df.columns:
                # df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
                # First, ensure the format is recognized correctly
                df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%d/%m/%Y')
                df['Arrival_Date'] = pd.to_datetime(df.Arrival_Date)
                # print(f"{df['Arrival_Date'].head()}")

                df['Arrival_Date_String'] = df['Arrival_Date'].dt.strftime('%d-%b-%Y')
                
                # df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%Y-%m-%d').dt.strftime('%Y%m%d').astype(int)

                df['Arrival_Date_Key'] = df['Arrival_Date'].dt.strftime('%Y%m%d').astype(int)
# Then convert it to the desired format
                # df['Arrival_Date'] = df['Arrival_Date'].dt.strftime('%-d-%b-%y')
                

            # print(f"Date after conversion: {df['Arrival_Date_String'].head()}")  # Debugging line
            # print(f"Date after conversion: {df['Arrival_Date_Key'].head()}")  # Debugging line

            # Perfo rm any necessary data cleaning here
            if 'Commodity_Code' in df.columns:
                df_cleaned = df.drop(columns=['Commodity_Code'])  # Example cleaning step
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

            # Rename the columns in the DataFrame
            df.rename(columns=column_mapping, inplace=True)
            # print(df.columns)  # Debugging line
            original_filename = os.path.basename(file)
            new_filename = f"Silver_{original_filename}"
            output_file = os.path.join(output_path, new_filename)
            # print(f"Output file path: {output_file}")  # Debugging line
           

            # Save the cleaned data to the output directory
            df_cleaned.to_csv(output_file, index=False)

        print(f"Processed {len(csv_files)} files and saved to {output_path}")
        
        # Move to the next day
        date += timedelta(days=1)

# Input: start date and end date (format: "YYYY-MM-DD")
start_date = "2002-01-01"
end_date = "2024-08-14"

# Process files between the start and end dates
process_files(start_date, end_date)
