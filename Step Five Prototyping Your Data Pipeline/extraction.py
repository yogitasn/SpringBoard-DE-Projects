import requests
import shutil
import argparse
import os
from time import time
from multiprocessing.pool import ThreadPool


DEBUG = False
    
def get_dynamic_url


def run():
    urls = [("LearnPython.pdf", "https://drive.google.com/file/d/0B2Y-n6IlHYliSXZxMk0xT0NSY1E/preview"),
            ("Blockface.csv", "https://data-seattlecitygis.opendata.arcgis.com/datasets/a1458ad1abca41869b81f7c0db0cd777_0.csv")]

    start = time()
   
    for url in urls:

        path, url = url

        r = requests.get(url, stream = True)

        with open(path, 'wb') as f:

            for ch in r:

                f.write(ch)

    print(f"Time to download: {time() - start}")

    today = datetime.datetime.now()
    current_day, current_month, current_year = today.day, today.month, today.year

    for year in range(2012, current_year + 1):
        sync_files(year)

run()

     