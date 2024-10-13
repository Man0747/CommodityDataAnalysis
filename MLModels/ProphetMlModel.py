import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy import stats

# Load the onion price data
df = pd.read_csv('/content/aggregated_daily_data_Azadpur_Tomato_commodity2023-2018.csv')
df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%Y-%m-%d')

# Load and prepare weather data
weather_df = pd.read_csv('/content/aggregated_weather_data.csv')  # Assuming you've saved the weather data in a CSV file
weather_df['Date'] = pd.to_datetime(weather_df['Date'])
weather_df = weather_df.rename(columns={'Date': 'ds'})

# Select relevant weather features (you may adjust this based on your analysis)
weather_features = ['T', 'P0', 'U', 'Ff', 'VV', 'Td']

# Merge onion price data with weather data
prophet_df = df[['Arrival_Date', 'Modal_Price']].rename(columns={'Arrival_Date': 'ds', 'Modal_Price': 'y'})
prophet_df = prophet_df.merge(weather_df[['ds'] + weather_features], on='ds', how='left')

# Handle missing weather data (if any)
for feature in weather_features:
    prophet_df[feature].fillna(prophet_df[feature].mean(), inplace=True)

# Create inflation data (as before)
inflation_data = {
    'date': ['2018-12-31', '2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31'],
    'inflation_rate': [3.9388, 3.7295, 6.6234, 5.1314, 6.699]
}
inflation_df = pd.DataFrame(inflation_data)
inflation_df['date'] = pd.to_datetime(inflation_df['date'])

# Function to extrapolate inflation rates (as before)
def extrapolate_inflation(start_date, end_date, inflation_df):
    x = mdates.date2num(inflation_df['date'])
    y = inflation_df['inflation_rate']
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    x_extrapolate = mdates.date2num(date_range)
    y_extrapolate = slope * x_extrapolate + intercept

    extrapolated_df = pd.DataFrame({
        'ds': date_range,
        'inflation_rate': y_extrapolate
    })

    return extrapolated_df

# Extrapolate inflation rates
start_date = prophet_df['ds'].min()
end_date = pd.to_datetime('2024-09-13')
extrapolated_inflation = extrapolate_inflation(start_date, end_date, inflation_df)

# Merge inflation data with our price and weather data
prophet_df = prophet_df.merge(extrapolated_inflation, on='ds', how='left')

# Create and fit the model
model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=False)
model.add_regressor('inflation_rate')
for feature in weather_features:
    model.add_regressor(feature)

model.fit(prophet_df)

# Create future dates for prediction
future_dates = pd.date_range(start='2024-01-01', end='2024-09-13')
future_df = pd.DataFrame({'ds': future_dates})

# Add projected inflation rates to future dataframe
future_df = future_df.merge(extrapolated_inflation, on='ds', how='left')

# Add weather features to future dataframe (using historical averages)
for feature in weather_features:
    future_df[feature] = prophet_df[feature].mean()

# Make predictions
forecast = model.predict(future_df)

# Plot the forecast
fig, ax = plt.subplots(figsize=(12, 6))
model.plot(forecast, ax=ax)
plt.title('Onion Price Forecast (Including Inflation and Weather)')
plt.xlabel('Date')
plt.ylabel('Price')
plt.savefig('onion_price_forecast_with_inflation_and_weather.png')
plt.close()

# Print the forecasted prices
print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'inflation_rate'] + weather_features].tail())

# Create final forecast DataFrame
final_forecast = forecast[['ds', 'yhat']].rename(columns={'ds': 'date', 'yhat': 'predicted_modal_price'})
final_forecast['predicted_modal_price'] = final_forecast['predicted_modal_price'].round(2)

# Save to CSV
final_forecast.to_csv('tomato_price_forecast.csv', index=False)

print("Forecast completed and saved to 'onion_price_forecast.csv'")
print("Forecast plot saved as 'onion_price_forecast_with_inflation_and_weather.png'")

# Plot the impact of inflation and a key weather feature (e.g., temperature)
fig, ax1 = plt.subplots(figsize=(12, 6))
ax1.set_xlabel('Date')
ax1.set_ylabel('Price', color='tab:blue')
ax1.plot(forecast['ds'], forecast['yhat'], color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')

ax2 = ax1.twinx()
ax2.set_ylabel('Inflation Rate (%)', color='tab:orange')
ax2.plot(forecast['ds'], forecast['inflation_rate'], color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')

ax3 = ax1.twinx()
ax3.spines["right"].set_position(("axes", 1.1))
ax3.set_ylabel('Temperature (°C)', color='tab:green')
ax3.plot(forecast['ds'], forecast['T'], color='tab:green')
ax3.tick_params(axis='y', labelcolor='tab:green')

plt.title('Onion Price Forecast vs Inflation Rate and Temperature')
fig.tight_layout()
plt.savefig('onion_price_vs_inflation_and_temperature.png')
plt.close()

print("Price vs Inflation and Temperature plot saved as 'onion_price_vs_inflation_and_temperature.png'")