import pandas as pd
import requests
import os
from datetime import date, timedelta
import mysql.connector
import sys

def get_start_date():
    # Establish connection to MySQL database
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='commoditydataanaylsis'
    )
    cursor = connection.cursor()

    # Fetch the start_date from the LastRun table
    cursor.execute("SELECT run_date FROM LastRun ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    connection.close()

    if result:
        return result[0]
    else:
        return None

def update_start_date(new_date):
    # Establish connection to MySQL database
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='commoditydataanaylsis'
    )
    cursor = connection.cursor()

    # Update the LastRun table with the new date
    cursor.execute("UPDATE LastRun SET run_date = %s WHERE id = 1", (new_date,))
    connection.commit()
    connection.close()

def getData(start_date, end_date):
    # Define the output path
    output_path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject/Bronze'
    
    # API URLs
    base_url_past = 'https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24'
    base_url_today = 'https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070'
    api_key = '579b464db66ec23bdd000001c448725136334a8c46b2f7e597535cc1'
    
    # Initialize date
    current_date = start_date
    
    while current_date <= end_date:
        formatted_date = current_date.strftime('%d/%m/%Y')
        
        # Choose the correct API based on whether the date is today or not
        if current_date == date.today():
            url = f'{base_url_today}?api-key={api_key}&format=csv&limit=100000'
        else:
            url = f'{base_url_past}?api-key={api_key}&format=csv&limit=10000&filters%5BArrival_Date%5D={formatted_date}'
        
        output_directory = os.path.join(output_path, str(current_date.year), str(current_date.month), str(current_date.day))
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        # Fetch and save data
        try:
            with requests.Session() as session:
                response = session.get(url)
                response.raise_for_status()  # Check if the request was successful
                csv_content = response.content
                
                # Generate the filename with date and time
                filename = f'commoditydata_{formatted_date.replace("/", "")}.csv'
                file_path = os.path.join(output_directory, filename)
                
                with open(file_path, 'wb') as file:
                    file.write(csv_content)
                    
                print(f"Data saved to {file_path}")
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        
        # Move to the next day
        current_date += timedelta(days=1)
    
    # Update the start_date in the LastRun table
    update_start_date(end_date)

# Fetch start date from the LastRun table
start_date = get_start_date()

# If no start_date is found, exit the program
if not start_date:
    print("No start date found in the database. Exiting the program.")
    sys.exit()

# Increment start date by 1 day
start_date += timedelta(days=1)

# Set the end date to today's date
end_date = date.today()

getData(start_date, end_date)

