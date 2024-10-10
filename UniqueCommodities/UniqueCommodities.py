import pandas as pd

# Load the dataset
df = pd.read_csv('F://Education//COLLEGE//PROGRAMING//Python//PROJECTS//CommodityDataAnalysisProject//aggregated_weekly_data.csv')  # Replace with the actual path to your CSV

# Get unique combinations of Commodity, Variety, and Grade
unique_commodities = df[['Commodity', 'Variety', 'Grade']].drop_duplicates().reset_index(drop=True)

# Rename the columns
unique_commodities.rename(columns={
    'Commodity': 'commodity_name',
    'Variety': 'commodity_variety',
    'Grade': 'commodity_grade'
}, inplace=True)

# Add a unique commodity_id column
unique_commodities['commodity_id'] = range(1, len(unique_commodities) + 1)

# Reorder the columns to have commodity_id first
unique_commodities = unique_commodities[['commodity_id', 'commodity_name', 'commodity_variety', 'commodity_grade']]

# Print the count of unique commodities
print(f"Count of unique commodities: {len(unique_commodities)}")

# Save the unique commodities to a CSV file
unique_commodities.to_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\unique_commodities_with_ids.csv', index=False)

print("Script has completed successfully. The unique commodities with IDs are saved in 'unique_commodities_with_ids.csv'.")
