import pandas as pd
import requests
import os
from datetime import datetime

def getData():
    
    API_URL = 'https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key=579b464db66ec23bdd000001c448725136334a8c46b2f7e597535cc1&format=csv&offset=0&limit=100000'

    output_Path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\Bronze'
    year = str(datetime.now().year)
    month = str(datetime.now().month)
    day = str(datetime.now().day)

    output_Path = output_Path + "/" + year + "/" + month + "/" + day

    isExist = os.path.exists(output_Path)

    if not isExist:
        os.makedirs(output_Path)
        
    with requests.Session() as s:
        download = s.get(API_URL)
        decoded_content = download.content

        # Generate the new filename with date and time separated by hyphens
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M')
        new_filename = output_Path + f'/commoditydata_{current_datetime}.csv'

        csv_file = open(new_filename, 'wb')
        csv_file.write(decoded_content)
        csv_file.close()

getData()