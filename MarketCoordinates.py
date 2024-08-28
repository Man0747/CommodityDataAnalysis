import pandas as pd
import requests
import time

# Load the dataset
df = pd.read_csv('F://Education//COLLEGE//PROGRAMING//Python//PROJECTS//CommodityDataAnalysisProject//aggregated_weekly_data.csv')  # Replace with the actual path to your CSV

# Define a function to get coordinates using the OpenCage API
def get_coordinates(district, market, api_key):
    query = f"{market}, {district}"
    url = f"https://api.opencagedata.com/geocode/v1/json?q={query}&key={api_key}"
    response = requests.get(url).json()
    if response['results']:
        lat = response['results'][0]['geometry']['lat']
        lng = response['results'][0]['geometry']['lng']
        return lat, lng
    else:
        return None, None

# Your OpenCage API key (replace with your actual key)
api_key = "fcbce23804ab4179a5b32ef5a7969101"  # Replace with your OpenCage API key

# Get unique combinations of District and Market
unique_markets = df[['District', 'Market']].drop_duplicates().reset_index(drop=True)

# Print the count of unique markets
print(f"Count of unique markets: {len(unique_markets)}")

# Initialize columns for Latitude and Longitude
unique_markets['Latitude'] = None
unique_markets['Longitude'] = None

# Define batch size and total number of batches
batch_size = 100  # Adjust batch size as needed
total_batches = len(unique_markets) // batch_size + (1 if len(unique_markets) % batch_size != 0 else 0)

# Process data in batches
for batch_num in range(total_batches):
    start_index = batch_num * batch_size
    end_index = min((batch_num + 1) * batch_size, len(unique_markets))
    batch = unique_markets.iloc[start_index:end_index]

    print(f"Processing batch {batch_num + 1}/{total_batches}...")

    for i, row in batch.iterrows():
        lat, lng = get_coordinates(row['District'], row['Market'], api_key)
        unique_markets.at[i, 'Latitude'] = lat
        unique_markets.at[i, 'Longitude'] = lng

    # Save progress after each batch to the same CSV
    unique_markets.dropna().to_csv('unique_markets_with_coordinates.csv', mode='w', index=False)

    # Delay between batches to prevent hitting API limits
    time.sleep(2)  # Adjust the delay as needed

print("Script has completed successfully. The unique markets with coordinates are saved in 'unique_markets_with_coordinates.csv'.")
