import requests
import pandas as pd
import time

def get_coordinates(market_name):
    base_url = "https://geocode.maps.co/search"
    params = {"q": market_name, "api_key": "66d2270352b57895083413hcwfd7859"}
    response = requests.get(base_url, params=params)
    
    # Check for successful response
    if response.status_code == 200:
        try:
            data = response.json()
            if data:
                # Choose the first result or apply logic to select the best one
                location = data[0]
                lat = float(location['lat'])
                lon = float(location['lon'])
                return lat, lon
            else:
                print(f"No results found for market: {market_name}")
                return None, None
        except ValueError:
            print(f"Error decoding JSON for market: {market_name}")
            return None, None
    else:
        print(f"Error: Received status code {response.status_code} for market: {market_name}")
        print(f"Response content: {response.content}")
        return None, None

# Load the CSV file
df = pd.read_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\unique_markets_with_ids.csv')

# Apply the function with rate limiting and print coordinates
def get_coordinates_with_delay(row):
    market_name = row['market_name']
    lat, lon = get_coordinates(market_name)
    if lat is not None and lon is not None:
        print(f"Market: {market_name} | Latitude: {lat} | Longitude: {lon}")
    else:
        print(f"Market: {market_name} | Latitude and Longitude: Not Found")
    time.sleep(1)  # Add a delay of 1 second between requests
    return pd.Series([lat, lon])

df[['latitude', 'longitude']] = df.apply(get_coordinates_with_delay, axis=1)

# Save the updated DataFrame to a new CSV file
df.to_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\market_data_with_coordinates.csv', index=False)

print("Coordinates added and CSV file saved successfully.")
