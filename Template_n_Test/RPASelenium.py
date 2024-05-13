import pandas as pd
import time
import os
import re
import sys
import logging


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

###############
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# Set format for displaying path
path = sys.argv[1] if len(sys.argv) > 1 else "."


download_path = "/home/nhdminh/airflow/"


service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed

# Define ChromeDriver service
service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed
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



def login(browser):
    browser.get("https://dpm.dmsone.vn/login")
    # Login to DMS
    print("Login to DMS")
    try:
        elem = browser.find_element(By.NAME, "username")  # Find the Username
        elem.send_keys("madmin")
        elem = browser.find_element(By.NAME, "password")  # Find the Password
        elem.send_keys("PVFCCo@2023" + Keys.RETURN)
        print("Page Title", browser.title)
        time.sleep(5)
        elem = browser.find_element(By.ID, "spHeaderShopInf")
        print("Page Title", elem.text)
        return 1
    except Exception as e:
        print(e)
        print("quit")
        browser.quit()
        return -1

def  duyetKHAll(browser):
    _URL = 'https://dpm.dmsone.vn/catalog_customer_mng/info'
    browser.get(_URL)
    print(browser.title)
    status =  browser.find_element(By.ID,'status')
    select = Select(status)
    select.select_by_visible_text('Dự thảo')
    time.sleep(1)
    btnSearch = browser.find_element(By.ID,'btnSearch')
    btnSearch.click()
    time.sleep(1)

    checkAll = browser.find_element(By.ID,'checkAll')
    checkAll.click()
    time.sleep(1)

    btnAgree = browser.find_element(By.ID,'group_insert_btnAgree')
    btnAgree.click()
    time.sleep(1)

    btnAgree = browser.find_element(By.XPATH,'/html/body/div[22]/div[2]/div[4]/a[1]/span')
    print(btnAgree.text)
    btnAgree.click()

    time.sleep(1)

    errMsg = browser.find_element(By.ID,'errMsg')
    return errMsg.text


browser = webdriver.Chrome(service=service, options=chrome_options)
login(browser)
duyetKHAll(browser)