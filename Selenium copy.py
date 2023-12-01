import pandas as pd
import time
import datetime as dt
import os
import pytz

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Define ChromeDriver service
service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed
download_path = "/home/nhdminh/airflow/DMSdownload"
noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")
print(noww)
try:
    os.mkdir(download_path + "/" + noww)
except Exception as e:
    print(e)
    pass
download_path = download_path + "/" + noww
# Set Chrome options for headless mode and download directory
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.binary_location = (
    "/usr/bin/google-chrome"  # Replace with the path to the Edge binary
)

# Set the download directory
chrome_options.add_experimental_option(
    "prefs", {"download.default_directory": download_path}
)

# Start the WebDriver
browser = webdriver.Chrome(service=service, options=chrome_options)


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


def downloadUnits(browser, download_path):
    _URL = "https://dpm.dmsone.vn/catalog/unit-tree/info"
    # ResetList FixFloat BreadcrumbList
    browser.get(_URL)
    time.sleep(2)
    print(
        browser.find_element(By.CSS_SELECTOR, ".ResetList.FixFloat.BreadcrumbList").text
    )
    exportBtn = browser.find_element(By.ID, "collapseTab")
    exportBtn.click()
    print("click collapseTab")
    time.sleep(2)

    exportBtn = browser.find_element(By.ID, "tabActive1")
    exportBtn.click()
    print("click tabActive1")
    time.sleep(2)

    exportBtn = browser.find_element(By.ID, "btnSearchShop")
    exportBtn.click()
    print("click btnSearchShop")
    time.sleep(2)
    parentElement = browser.find_element(By.ID, "container1")
    exportBtn = parentElement.find_element(By.ID, "btnExport")
    exportBtn.click()
    print("click btnExport")
    time.sleep(5)

    exportBtn = browser.find_element(By.ID, "tabActive2")
    exportBtn.click()
    print("click tabActive2")
    time.sleep(2)

    exportBtn = browser.find_element(By.ID, "btnSearchStaff")
    exportBtn.click()
    print("click btnSearchStaff")
    time.sleep(2)
    parentElement = browser.find_element(By.ID, "container2")
    exportBtn = parentElement.find_element(By.ID, "btnExport")
    exportBtn.click()
    print("click btnExport")
    time.sleep(5)

    download_wait(download_path)


def exportKHC2(browser, download_path):
    # Export file Du_lieu_khach_hang
    _URL = "https://dpm.dmsone.vn/catalog_customer_mng/info"
    id = "btnExportExcel"

    try:
        browser.get(_URL)
        exportBtn = browser.find_element(By.ID, id)
        time.sleep(2)
        exportBtn.click()
        print("Click ID btnExportExcel")
        print("Wait to download")
        time.sleep(5)

        download_wait(download_path)
    except Exception as e:
        print(e)
        pass


def exportBC(browser, download_path):
    # Export file Du_lieu_khach_hang
    _URL = "https://dpm.dmsone.vn/report/list"
    id = "btnExportExcel"
    # #NVTT -> .jstree-icon ->.BCCTVTKHC2
    # #fromDate -> #toDate
    # #btnReport
    try:
        browser.get(_URL)
        print(browser.title)
    except Exception as e:
        print(e)
        return -1
        pass
    expandMenu = browser.find_element(By.CSS_SELECTOR, "#NVTT")
    print(expandMenu.text)
    expandMenu.click


def main():
    browser.get("https://dpm.dmsone.vn/login")
    # Login to DMS
    try:
        elem = browser.find_element(By.NAME, "username")  # Find the Username
        elem.send_keys("madmin")
        elem = browser.find_element(By.NAME, "password")  # Find the Password
        elem.send_keys("PVFCCo@2023" + Keys.RETURN)
        print("Page Title", browser.title)
        time.sleep(5)
        elem = browser.find_element(By.ID, "spHeaderShopInf")
        print("Page Title", elem.text)

    except Exception as e:
        print(e)
        print("quit")
        browser.quit()
        pass

    # exportKHC2(browser, download_path)
    # downloadUnits(browser, download_path)
    exportBC(browser, download_path)
    print("quit")
    browser.quit()


# Using the special variable
# __name__
if __name__ == "__main__":
    main()
