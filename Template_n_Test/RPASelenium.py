import pandas as pd
import time
import os
import re
import sys
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

###############
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# Set format for displaying path
path = sys.argv[1] if len(sys.argv) > 1 else "."

# Initialize logging event handler
event_handler = LoggingEventHandler()

# Initialize Observer
observer = Observer()
observer.schedule(event_handler, path, recursive=True)
# Define ChromeDriver service
service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed
download_path = "/home/nhdminh/airflow/"

# Set Chrome options for headless mode and download directory
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.binary_location = "/usr/bin/google-chrome"  # Replace with the path to the Edge binary

# Set the download directory
chrome_options.add_experimental_option("prefs", {"download.default_directory": download_path})

# Start the WebDriver
browser = webdriver.Chrome(service=service, options=chrome_options)


# Example: Open a webpage and print its title
browser.get("https://dpm.dmsone.vn/login")

print("Page Title 1:", browser.title)


# Perform your Selenium actions here...
def download_wait(path_to_downloads):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 20:
        dl_wait = False
        for fname in os.listdir(path_to_downloads):
            # if fname.endswith(".xlsx"):
            #     dl_wait = False
            #     break

            if fname.endswith("crdownload"):
                dl_wait = True
                break
        seconds += 2
        time.sleep(2)

        print("Wait", seconds)
    return seconds


# Login to DMS
try:
    elem = browser.find_element(By.NAME, "username")  # Find the Username
    elem.send_keys("madmin")
    elem = browser.find_element(By.NAME, "password")  # Find the Password
    elem.send_keys("PVFCCo@2023" + Keys.RETURN)
    print("Page Title 2:", browser.title)

except Exception as e:
    print(e)
    pass

print("Page Title 3:", browser.title)
time.sleep(2)

# Export file Du_lieu_khach_hang
_URL = "https://dpm.dmsone.vn/catalog_customer_mng/info"
id = "btnExportExcel"
pattern = re.compile(".Du_lieu_danh_muc_khachhang.xlsx")
dir = download_path

try:
    browser.get(_URL)
    exportBtn = browser.find_element(By.ID, id)
    time.sleep(2)
    exportBtn.click()
    print("Wait to download")
    time.sleep(5)

    download_wait(download_path)
except Exception as e:
    print(e)
    pass

##########
# https://dpm.dmsone.vn/catalog/unit-tree/info
_URL = "https://dpm.dmsone.vn/catalog/unit-tree/info"


# Close the browser

print("quit")
browser.quit()
