import datetime as dt
import os
import shutil
import time

import pandas as pd
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


download_path = f"{os.path.abspath('')}/DMSdownload"
noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")


download_path = download_path + "/" + noww
try:
    os.mkdir(download_path)
except:
    pass

end = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%d/%m/%Y")
begin = (
    dt.datetime.now(pytz.timezone("Asia/Bangkok")).date() - dt.timedelta(days=7)
).strftime(format="%d/%m/%Y")

# Define ChromeDriver service
service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed
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


def download_wait(path_to_downloads):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 20:
        dl_wait = False
        for fname in os.listdir(path_to_downloads):
            if fname.endswith("crdownload"):
                dl_wait = True
                break
        seconds += 2
        time.sleep(2)

        print("Wait", seconds)
    return seconds


# %%
def exportMaster(_URL, browser):
    # Export file Du_lieu_khach_hang/Product
    try:
        browser.get(_URL)

        try:
            exportBtn = browser.find_element(By.ID, "btnExport")
            exportBtn.click()
        except:
            exportBtn = browser.find_element(By.ID, "btnExportExcel")
            exportBtn.click()
        time.sleep(2)

        print("Click ID btnExportExcel")
        print("Wait to download")
        try:
            exportBtn.find_element(
                By.XPATH, "/html/body/div[17]/div[2]/div[4]/a[1]/span"
            ).click()
        except:
            pass
        time.sleep(5)
    except Exception as e:
        print(e)
        pass
    download_wait(download_path)


# %%
def exportBC(browser, ID_BC, sub_ID_BC, fromDate, toDate, ID_btn_click):
    # Export file
    _URL = "https://dpm.dmsone.vn/report/list"
    id = "btnExportExcel"
    try:
        browser.get(_URL)
        print(browser.title)
        time.sleep(1)

    except Exception as e:
        print(e)
        return -1
        pass
    BCMenu = browser.find_element(By.ID, ID_BC)
    BCMenu.find_element(By.TAG_NAME, "ins").click()
    time.sleep(1)

    print(BCMenu.find_element(By.ID, sub_ID_BC).text)
    BCMenu.find_element(By.ID, sub_ID_BC).click()
    time.sleep(2)

    ReportCtnSection = browser.find_element(By.CLASS_NAME, "ReportCtnSection")

    fromdate = ReportCtnSection.find_element(By.ID, fromDate)
    fromdate.click()
    fromdate.send_keys(begin)

    time.sleep(1)

    todate = ReportCtnSection.find_element(By.ID, toDate)
    todate.click()
    todate.send_keys(end)
    time.sleep(1)

    btnReport = ReportCtnSection.find_element(By.ID, ID_btn_click)
    btnReport.click()
    print(btnReport.text)
    time.sleep(5)


def exportBC_index(browser, Lv1, Lv2):
    dsBaocao = pd.read_csv("/home/nhdminh/DMS/dags/DSBaocao.csv")
    filterBC1 = dsBaocao["Lv1"] == Lv1
    filterBC2 = dsBaocao["Lv2"] == Lv2
    BC = dsBaocao[filterBC1 * filterBC2].reset_index(drop=True).to_dict()
    exportBC(
        browser,
        BC["ID_BC"][0],
        BC["sub_ID_BC"][0],
        BC["fromDate"][0],
        BC["toDate"][0],
        BC["ID_btn_click"][0],
    )
    # filecontent
    # C:\Users\nhducminh\Downloads\
    print(BC["filecontent"][0])
    download_wait(download_path)


# %%
def exportUnits(browser):
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


def login(browser):
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
        return 1
    except Exception as e:
        print(e)
        print("quit")
        browser.quit()
        return -1


def main():
    # Start the WebDriver
    browser = webdriver.Chrome(options=chrome_options)

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
        # elem.text()
    except Exception as e:
        print(e)
        print("quit")
        browser.quit()
        return
        pass

    exportUnits(browser)
    exportMaster("https://dpm.dmsone.vn/catalog_customer_mng/info", browser)
    exportMaster("https://dpm.dmsone.vn/catalog/product/infoindex", browser)
    exportBC_index(browser, 10, 1)
    exportBC_index(browser, 7, 3)
    time.sleep(1)
    print("quit")
    browser.quit()


# Using the special variable
# __name__
if __name__ == "__main__":
    main()
