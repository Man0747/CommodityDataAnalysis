import pandas as pd

# Load the CSV data into a DataFrame
df = pd.read_csv('F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\calendar.csv')

# Drop the unwanted columns
columns_to_drop = ['month_and_year', 'holiday', 'timezone_id', 'timezone', 'timezone_offset', 'sasdate', 'date_key']
df = df.drop(columns=columns_to_drop)

# Convert 'date' column to datetime format
df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')

# Create the 'Date_Key' column in the desired format
df['Date_Key'] = df['date'].dt.strftime('%Y%m%d').astype(int)

# Save the cleaned DataFrame to a new CSV file
df.to_csv('F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\cleaned_file.csv', index=False)
