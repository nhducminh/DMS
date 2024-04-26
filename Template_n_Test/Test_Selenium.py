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


def exportBC(browser, download_path, ID_BC, sub_ID_BC, ID_fromDate, ID_toDate, ID_btn_click):
    # Export file
    print(f'exportBC {download_path}, {ID_BC}, {sub_ID_BC}, {ID_fromDate}, {ID_toDate}, {ID_btn_click}')
    _URL = "https://dpm.dmsone.vn/report/list"
    id = "btnExportExcel"

    try:
        browser.get(_URL)
        time.sleep(1)

    except Exception as e:
        print(e)
        return -1
        pass

    trytofind = True
    count = 0
    while trytofind:
        try:
            BCMenu = browser.find_element(By.ID, ID_BC)
            print(BCMenu.text)
            BCMenu.find_element(By.TAG_NAME, "ins").click()
            time.sleep(1)
            BCMenu.find_element(By.ID, sub_ID_BC).click()
            print(BCMenu.find_element(By.ID, sub_ID_BC).text)
            time.sleep(2)
            trytofind = False
        except:
            time.sleep(2)
            count += 1
            print(f"try {count}")
            if count == 10:
                trytofind = False

    ReportCtnSection = browser.find_element(By.CLASS_NAME, "ReportCtnSection")
    print(f"{begin}=>{end}")
    # 3,1,REPORT_PROMOTION_PROGRAM,REPORT_PROMOTION_PROGRAM_5_1,3.1 Tổng kết khuyến mãi
    
    if sub_ID_BC != 'BCCTTL' or  sub_ID_BC !=  'CBTDTTCTTLTB' or sub_ID_BC !='sub_ID_BC':
        print("no date")
    else:
        fromdate = ReportCtnSection.find_element(By.ID, ID_fromDate)
        fromdate.click()
        fromdate.send_keys(begin)

        time.sleep(1)

        todate = ReportCtnSection.find_element(By.ID, ID_toDate)
        todate.click()
        todate.send_keys(end)
        time.sleep(1)
    
    
    if sub_ID_BC == 'REPORT_PROMOTION_PROGRAM_5_1':
        btnReport = ReportCtnSection.find_element(By.ID, "btnReportXNT1D1")
    elif sub_ID_BC == 'CBTDTTCTTLTB' or sub_ID_BC=='CBTDTTCTTLTB_NEW':
        btnReport = ReportCtnSection.find_element(By.ID, "btnSearch")
    else:
        btnReport = ReportCtnSection.find_element(By.ID, ID_btn_click)
        


        
    btnReport.click()
    time.sleep(10)
    download_wait(download_path)

    btnerrMsg = browser.find_element(By.ID,"errMsg")
    print(btnerrMsg.text)
    return len(btnerrMsg.text)
    


def exportBC_index(browser, Lv1, Lv2):
    
    print(f'exportBC_index {Lv1}, {Lv2}:')    
    
    dsBaocao = pd.read_csv("/home/nhdminh/DMS/dags/DSBaoCao.csv")
    filterBC1 = dsBaocao["Lv1"] == Lv1
    filterBC2 = dsBaocao["Lv2"] == Lv2
    BC = dsBaocao[filterBC1 * filterBC2].reset_index(drop=True).to_dict()
    
    print(BC)
    try:
        for f in os.listdir(download_path):
            if BC["file_name"][0] in f:
                print(f"đã tồn tại file {f} => pass")
                return 1
    except: pass

    exportBC(
        browser,
        download_path,
        BC["ID_BC"][0],
        BC["sub_ID_BC"][0],
        "fromDate",
        "toDate",
        "btnReport",
        )
    time.sleep(5)

    try:
        for f in os.listdir(download_path):
            if BC["file_name"][0] in f:
                print(f"Đã tải thành công {BC['ID_BC'][0]} {BC['sub_ID_BC'][0]} {f}")
                return 1
    except:
        pass
    # not downloaded
    print(dt.datetime.now())
    return 0



# %%
def exportUnits(browser):
    print("exportUnits")
    _URL = "https://dpm.dmsone.vn/catalog/unit-tree/info"
    # ResetList FixFloat BreadcrumbList
    browser.get(_URL)
    time.sleep(2)
    print(browser.find_element(By.CSS_SELECTOR, ".ResetList.FixFloat.BreadcrumbList").text)
    exportBtn = browser.find_element(By.ID, "collapseTab")
    exportBtn.click()
    print("click collapseTab")
    time.sleep(2)
    print("Export Units")
    exportBtn = browser.find_element(By.ID, "tabActive1")
    exportBtn.click()
    print(f"click  {exportBtn.text}")
    time.sleep(2)

    exportBtn = browser.find_element(By.ID, "btnSearchShop")
    exportBtn.click()
    print("click btnSearchShop => tim kiem don vi")

    time.sleep(2)
    parentElement = browser.find_element(By.ID, "container1")
    exportBtn = parentElement.find_element(By.ID, "btnExportShop")
    exportBtn.click()
    print("click btnExportShop => export don vi")
    time.sleep(5)

    print("Export NVTT")
    exportBtn = browser.find_element(By.ID, "tabActive2")
    exportBtn.click()
    print(f"click  {exportBtn.text}")
    time.sleep(2)

    exportBtn = browser.find_element(By.ID, "btnSearchStaff")
    exportBtn.click()
    print("click btnSearchStaff => tim kiem nhan vien")
    time.sleep(2)
    parentElement = browser.find_element(By.ID, "container2")
    exportBtn = parentElement.find_element(By.ID, "btnExport")
    exportBtn.click()
    print("click btnExport => export nhan vien")
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
        

    # exportUnits(browser)
    # exportMaster("https://dpm.dmsone.vn/catalog_customer_mng/info", browser)
    # exportMaster("https://dpm.dmsone.vn/catalog/product/infoindex", browser)
    
    exportBC_index(browser, 5, 1)
    exportBC_index(browser, 6, 1)
    exportBC_index(browser, 6, 2)
    exportBC_index(browser, 7, 4)
    time.sleep(1)
    print("quit")
    browser.quit()


# Using the special variable
# __name__
if __name__ == "__main__":
    main()
