import pandas as pd

# Load the dataset
df = pd.read_csv('F://Education//COLLEGE//PROGRAMING//Python//PROJECTS//CommodityDataAnalysisProject//aggregated_weekly_data.csv')  # Replace with the actual path to your CSV

# Get unique combinations of State, District, and Market
unique_markets = df[['State', 'District', 'Market']].drop_duplicates().reset_index(drop=True)

# Rename the columns
unique_markets.rename(columns={
    'State': 'market_state',
    'District': 'market_district',
    'Market': 'market_name'
}, inplace=True)

# Add a unique market_id column
unique_markets['market_id'] = range(1, len(unique_markets) + 1)

# Reorder the columns to have market_id first
unique_markets = unique_markets[['market_id', 'market_name', 'market_district', 'market_state']]

# Print the count of unique markets
print(f"Count of unique markets: {len(unique_markets)}")

# Save the unique markets to a CSV file
unique_markets.to_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\unique_markets_with_ids.csv', index=False)

print("Script has completed successfully. The unique markets with IDs are saved in 'unique_markets_with_ids.csv'.")
