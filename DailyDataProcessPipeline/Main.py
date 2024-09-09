from datetime import datetime, timedelta,date
import mysql.connector
from Gold_DataTransformation import process_silver_to_gold_with_joins
from Silver_DataCleansing import process_files
from SQL_IncrementalLoadDataTransfer import load_data_to_mysql
def get_dates_from_db():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="commoditydataanaylsis"
    )
    cursor = connection.cursor()

    # Get the last processed date
    cursor.execute("SELECT run_date FROM LastProcessed ORDER BY id DESC LIMIT 1")
    start_date = cursor.fetchone()[0]

    # Get the last run date
    cursor.execute("SELECT run_date FROM Lastrun ORDER BY id DESC LIMIT 1")
    end_date = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return start_date, end_date

def update_run_date_in_db(date, table_name):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="commoditydataanaylsis"
    )
    cursor = connection.cursor()

    # Update the run_date in the specified table
    cursor.execute(f"UPDATE {table_name} SET run_date = %s ORDER BY id DESC LIMIT 1", (date,))
    connection.commit()

    cursor.close()
    connection.close()


start_date, end_date = get_dates_from_db()
# start_date = date(2024, 8, 28)
# end_date = date(2024, 9, 1)
start_date += timedelta(days=1)
date = start_date
while date <= end_date:
    process_files(date)
    process_silver_to_gold_with_joins(date)
    # load_data_to_mysql(date)
    date += timedelta(days=1)

date -= timedelta(days=1)
# update_run_date_in_db(date, "LastProcessed")
