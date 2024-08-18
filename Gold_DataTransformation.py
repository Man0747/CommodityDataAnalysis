import os
import pandas as pd
import glob

# Base directory where the CSV files are stored
base_directory = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject'

# Define the years you want to process
years = ['2022', '2021','2020' ]

# List to store DataFrames
dataframes = []

# Loop through the specified years
for year in years:
    # Construct the path for the year folder
    year_folder_path = os.path.join(base_directory, 'Silver', year)
    
    # Recursively find all CSV files in the year folder
    csv_files = glob.glob(os.path.join(year_folder_path, '**', '*.csv'), recursive=True)
    
    # Read and store each CSV into the DataFrames list
    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)

# Concatenate all DataFrames into one DataFrame
data = pd.concat(dataframes, ignore_index=True)

# Ensure 'Arrival_Date' column is in datetime format
data['Arrival_Date'] = pd.to_datetime(data['Arrival_Date'])

# Load the calendar CSV file
calendar_path = os.path.join(base_directory, 'calendar.csv')
calendar_df = pd.read_csv(calendar_path)

# Ensure 'date' column in calendar_df is in datetime format
# calendar_df['date'] = pd.to_datetime(calendar_df['date'])

# Merge with calendar data to get the correct week numbers
merged_data = pd.merge(data, calendar_df,left_on='Arrival_Date_Key', right_on='Date_Key', how='left')

# Group by year, week, State, District, Market, Commodity, and Variety
weekly_data = merged_data.groupby(['year', 'week', 'State', 'District', 'Market', 'Commodity', 'Variety']).agg({
    'Min_Price': 'mean',
    'Max_Price': 'mean',
    'Modal_Price': 'mean'
}).reset_index()

# Define the output filename for the single aggregated CSV file
output_filename = 'aggregated_weekly_data.csv'
output_path = os.path.join(base_directory, output_filename)

# Write the aggregated data to the CSV file
weekly_data.to_csv(output_path, index=False)

print("Aggregated CSV file generated successfully.")
