
import pandas as pd
import glob
import csv
import requests
import os
from datetime import datetime

class Silver:

    def Datacleansing() :

        path ='F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject'

        input_path = path + '\Bronze'
        # # year = str(datetime.now().year)
        # month = str(datetime.now().month)
        # day = str(datetime.now().day-1)
        year = "2021" 
        month = "01"
        day = "1"
        input_path = input_path + "/" + year + "/" + month + "/" + day
        output_path = path + '/Silver' + "/" + year + "/" + month + "/" + day
        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)


        csv_files = glob.glob(input_path + "/*.csv")

        df_list = (pd.read_csv(file) for file in csv_files)

        combined_df = pd.concat(df_list, ignore_index=True)

        print(combined_df.head(10))

   							

        combined_df.rename(columns={"state": "State", "district": "District","market": "Market","arrival_date":"Arrival_Date","commodity": "Commodity","variety": "Variety","min_price": "Min_Price","max_price": "Max_Price","modal_price": "Modal_Price"}, inplace=True)

        final_df = combined_df.groupby(["State", "District",  "Market", "Commodity","Variety","Arrival_Date"]).agg({"Min_Price": "min", "Max_Price": "max", "Modal_Price": "mean"}).reset_index()
        final_df["Min_Price"] = final_df["Min_Price"].round(2)
        final_df["Max_Price"] = final_df["Max_Price"].round(2)
        final_df["Modal_Price"] = final_df["Modal_Price"].round(2)

        # final_df["Pollutant_Data"] _= final_df.apply(lambda row: row["Pollutant_Max"] if row["Pollutant_Type"] in ["OZONE", "CO"] else row["Pollutant_Avg"], axis=1)
        # final_df = combined_df.groupby(["Country","State", "City", "Station", "Date", "Pollutant_Type"])

        # final_df = final_df.agg({"Pollutant_Avg": "mean"}).reset_index()
        # final_df = final_df.agg({"Pollutant_Max": "max"}).reset_index()
        # final_df["Pollutant_Avg"] = final_df["Pollutant_Avg"].round(2)
        # final_df["Pollutant_Max"] = final_df["Pollutant_Max"].round(2)

        # current_datetime = datetime.now().strftime('%Y%m%d')
        output_file_path = output_path + '/Silver_CommodityData.csv'
        final_df.to_csv(output_file_path, index=False)

Silver.Datacleansing()