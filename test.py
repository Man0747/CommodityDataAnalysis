import pandas as pd
import numpy as np

# Load the CSV file
csv_file = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\WeatherDelhi.csv'  # Replace with the actual path
df = pd.read_csv(csv_file)

# Convert timestamp column to datetime format
df['Timestamp'] = pd.to_datetime(df['Local time in New Delhi / Safdarjung (airport)'], format='%d.%m.%Y %H:%M')

# Extract the date
df['Date'] = df['Timestamp'].dt.date

# Handle missing values (e.g., filling NaN with mean or specific value)
df.fillna(method='ffill', inplace=True)  # Forward fill as an example
# You can customize this method as per your requirement (e.g., `bfill`, `fillna(0)`)

# Aggregate data by date (example: taking mean of numerical columns)
aggregated_data = df.groupby('Date').agg({
    'T': 'mean',       # Average Temperature
    'P0': 'mean',      # Average P0
    'P': 'mean',       # Average Pressure
    'U': 'mean',       # Average Humidity
    'Ff': 'mean',      # Average Wind Speed
    'VV': 'mean',      # Average Visibility
    'Td': 'mean',      # Average Dew Point
    # For non-numerical data, you can use custom functions, like taking the most frequent value
    'DD': lambda x: x.mode()[0] if not x.mode().empty else np.nan,  # Most common wind direction
    'WW': lambda x: x.mode()[0] if not x.mode().empty else np.nan,  # Most common weather condition
})

# Reset index to make 'Date' a column again
aggregated_data.reset_index(inplace=True)

# Save the result to a new CSV
aggregated_data.to_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\aggregated_weather_data.csv', index=False)

print(aggregated_data)
