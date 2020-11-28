import requests
import shutil
import argparse
import os

DEBUG = False

class ProcessHistOccupancyData(object):

    def __init__(self, file_name):
        self.file_name = file_name

    def download_hist_data(self):
        url = "https://data.seattle.gov/api/views/"+self.file_name+"/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
        downloaded_file_name="PaidParkingOccupancy.csv"
        
        r = requests.get(url, verify=False,stream=True)
        if r.status_code!=200:
            print("Failure!!")
            exit()
        else:
            r.raw.decode_content = True
            with open(downloaded_file_name, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            print(downloaded_file_name+ " successfully downloaded")

    def run(self):
        csv_df = self.download_hist_data()
        print("Batch process finished.")
    
def run():
    # Used ArgumentParser to pass arguments debug or file in command line

    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="debug mode, loads small test file.", action="store_true")
    parser.add_argument("--file", help="file name to process")
    args = parser.parse_args()
    
    file_name = args.file if args.file else "wtpb-jp8d" # Code to download the 2020 Paid Parking Dataset
    if args.debug:
        DEBUG = True 
        file_name = "hiyf-7edq" # Code to download Last 48 hours smaller file
    proc = ProcessHistOccupancyData(file_name)
    proc.run()


run()

     