import requests
import shutil
import argparse
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time
from selenium.webdriver.support.select import Select
import datetime

DEBUG = False


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

    # function to take care of downloading file
def get_parkingOccupancy_file_code(year):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--verbose')

    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')

    # initialize driver object and change the <path_to_chrome_driver> depending on your directory where your chromedriver should be
    driver = webdriver.Chrome(chrome_options=chrome_options, executable_path="..\ChromeDriver\chromedriver.exe")

    # get request to target the site selenium is active on
    driver.get("https://data.seattle.gov/")
    time.sleep(2)

    # initialize an object to the location on the html page and click on it to download
    search_input = driver.find_element_by_xpath("(//input[@type='search'])[2]")
    search_input.send_keys("{} Paid Parking".format(year))
    time.sleep(4)
    #WebElement select = driver.findElement(By.id("gender"));
    print(driver.find_element_by_xpath("//div[@id='react-autowhatever-1']").text)
    driver.find_element_by_xpath("//div[@id='react-autowhatever-1']").click()


    time.sleep(10)

    url=driver.find_element_by_xpath('(//a[@class="browse2-result-name-link"])[1]').get_attribute("href")

    urls=url.split("/")
    print(urls)

    code=urls[5]

    return code

    # This function returns a concatenated string: base_url+year+"-"+formatted_month+".csv"
def _get_parkingOcc_url_by_year(code):
    
    url1="https://data.seattle.gov/api/views/"
    url2="/rows.csv?accessType=DOWNLOAD&bom=true&format=true"

    return "{}{}{}".format(url1,code,url2)



# Retrieves data from list of urls
def _process_url(url, filename):
    print(f"Starting URL processing for {url} and {filename}") # print statement to show
    datasource = urllib.request.urlopen(url)
    file_exists = os.path.isfile(filename)
    write_mode = "a" if file_exists else "w"
    with open(filename, write_mode) as f, open(SUCCESS_LOG, "a") as success_f:
        lines_from_datasource = [datasource.readline() for i in range(SAMPLE_LINES)]
        for i, line in enumerate(lines_from_datasource):
            if not line:
                continue

            if i == 0 and file_exists:
                # if downloaded file exists skip header
                # as you already have it from previous file
                continue

            f.write(line.decode("utf8").rstrip())
            f.write("\n")
           
            if SAMPLING and i > SAMPLE_LINES:
                break
        success_f.write(f"{url}\n")


def _print_neat_error(err, month, year, url):
    with open(FAILURE_LOG, "a") as failure_f:
        failure_f.write(
            f"""
            {err}\nThe above error occured for the following:\n
            URL: {url}
            Month: {month}
            Year: {year}
            {_line_separator()}\n
        """
        )
    print(
        f"""
        {err}\nThe above error occured for the following:\n
        URL: {url}
        Month: {month}
        Year: {year}
        {_line_separator()}
    """
    )


def sync_files(year):
    urls = [_get_project_url(base_url, year) for base_url in BASE_URLS]
    for url, file in zip(urls, FILENAMES):
        try:
            _process_url(url, file)
        except Exception as e:
            _print_neat_error(e, month, year, url)


def _line_separator():
    return "=" * 50


def _banner():
    return "%s\nWhole Data Overview\n%s" % ((_line_separator(),) * 2)


def _greeting(day, month, year):
    return f"Starting program for {month}-{year}-{day}\n" + _line_separator()


def run():
    today = datetime.datetime.now()
    current_day, current_month, current_year = today.day, today.month, today.year

    print(_greeting(current_day, current_month, current_year))
    print(_banner())
    year_codes={}

    for year in range(2020, current_year + 1):
        #sync_files(month, year)
        year_codes[year]=get_parkingOccupancy_file_code(year)

    print(year_codes)
run()
    

     