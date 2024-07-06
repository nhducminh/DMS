from __future__ import annotations

import datetime as dt
import getpass

import datetime as dt
import getpass
import logging
import os
import sys
import tempfile
import time
from pprint import pprint
from typing import List

import pandas as pd
import pendulum
import pysharepoint as ps
import pytz
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

from sqlalchemy import create_engine
from sqlalchemy import create_engine

parent_path = os.path.abspath(os.path.join(os.path.abspath(""), os.pardir))
download_path = f"{os.path.abspath('')}/DMS/DMS_daily"
noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")

download_path = "/home/nhdminh/DMS/DMS_daily/" + noww

try:
    os.mkdir(download_path)
except:
    pass

end = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%d/%m/%Y")
begin = (dt.datetime.now(pytz.timezone("Asia/Bangkok")).date() - dt.timedelta(days=7)).strftime(format="%d/%m/%Y")


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


##############################################################
# This will be the URL that points to your sharepoint site.
# Make sure you change only the parts of the link that start with "Your"
sharepoint_base_url = "https://pvfcco.sharepoint.com"
url_shrpt = "https://pvfcco.sharepoint.com/sites/DMSTNB"
username_shrpt = "nhdminh@pvfcco.com.vn"
password_shrpt = "a3671c389"
folder_url_shrpt = "/sites/DMSTNB/Shared%20Documents"
subfolder_url_shrpt = "Báo cáo - DMS/DMS_daily"

noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")
localPath = "/home/nhdminh/DMS/DMS_daily/" + noww

##############################################################
###Authentication###For authenticating into your sharepoint site###
ctx_auth = AuthenticationContext(url_shrpt)
if ctx_auth.acquire_token_for_user(username_shrpt, password_shrpt):
    ctx = ClientContext(url_shrpt, ctx_auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print("Authenticated into sharepoint as: ", web.properties["Title"])

else:
    print(ctx_auth.get_last_error())

site = ps.SPInterface(sharepoint_base_url, username_shrpt, password_shrpt)

#############################


def download_wait(path_to_downloads):
    pre_download = len(os.listdir(path_to_downloads))
    print(pre_download)
    waittime = 0
    maxtime = 30

    while 1:
        after_download = len(os.listdir(path_to_downloads))
        if pre_download < after_download:
            print("downloaded")
            print(after_download)
            break
        else:
            time.sleep(2)
            waittime += 2
            
        if waittime > maxtime:
            print("download faileddddd")
            break
    return waittime


# %% def exportMaster(_URL,browser, download_path):


def exportMaster(_URL, browser):
    print("exportMaster")
    # Export file Du_lieu_khach_hang/Product
    if _URL=='https://dpm.dmsone.vn/catalog_customer_mng/info':
        for f in os.listdir(download_path):
            if "Danh_sach_nhan_vien" in f:
                print(f"Đã tồn tại file {f}")
                return 0
    #Danh_sach_nhan_vien
    if _URL=='https://dpm.dmsone.vn/catalog/product/infoindex':
        for f in os.listdir(download_path):
            if "Thong_Tin_San_Pham" in f:
                print(f"Đã tồn tại file {f}")
                return 0
    #Thong_Tin_San_Pham
    try:
        browser.get(_URL)
        try:
            exportBtn = browser.find_element(By.ID, "btnExport")
            exportBtn.click()
        except:
            exportBtn = browser.find_element(By.ID, "btnExportExcel")
            exportBtn.click()
        time.sleep(2)

        print("Click ID exportBtn.text")
        print("Wait to download")
        try:
            exportBtn.find_element(By.XPATH, "/html/body/div[17]/div[2]/div[4]/a[1]/span").click()
        except:
            pass
        time.sleep(5)
    except Exception as e:
        print(e)
        pass
    download_wait(download_path)


# %% def exportUnits(browser, download_path):


def exportUnits(browser):
    print("exportUnits")
    for f in os.listdir(download_path):
        if "Units" in f:
            print(f"Đã tồn tại file {f}")
            return 0
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


# %% def login(browser)


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



def exportBC(browser, download_path, ID_BC, sub_ID_BC, ID_fromDate, ID_toDate, ID_btn_click):
    # Export file
    print(f'\nexportBC {download_path}, {ID_BC}, {sub_ID_BC}, {ID_fromDate}, {ID_toDate}, {ID_btn_click}')
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
    # print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
    print(f"{begin}=>{end}")
    try:
        print('fDate')
        fromdate = ReportCtnSection.find_element(By.ID, 'fDate')
        todate = ReportCtnSection.find_element(By.ID, 'tDate')
        result = 2
    except Exception as e:
        # print(e)
    # finally:
        print('fromDate')
        fromdate = ReportCtnSection.find_element(By.ID, ID_fromDate)
        todate = ReportCtnSection.find_element(By.ID, ID_toDate)
        result = 3

        
    fromdate.click()
    fromdate.send_keys(begin)
    time.sleep(1)
    todate.click()
    todate.send_keys(end)
    time.sleep(1)

    try:
        print('btnSearch')
        btnReport = ReportCtnSection.find_element(By.ID, 'btnSearch')
        result = 4
    except Exception as e:
        # print(e)        
    # finally:
        print('btn_click')
        btnReport = ReportCtnSection.find_element(By.ID, ID_btn_click)
        result = 5
    btnReport.click()
    time.sleep(10)
    download_wait(download_path)

    btnerrMsg = browser.find_element(By.ID,"errMsg")
    print(btnerrMsg.text)
    print(result)
    return len(btnerrMsg.text)
    


def exportBC_index(browser, Lv1, Lv2):
    
    print(f'exportBC_index {Lv1}, {Lv2}:')    
    
    dsBaocao = pd.read_csv("/home/nhdminh/DMS/dags/DSBaoCao.csv")
    filterBC1 = dsBaocao["Lv1"] == Lv1
    filterBC2 = dsBaocao["Lv2"] == Lv2
    BC = dsBaocao[filterBC1 * filterBC2].reset_index(drop=True).to_dict()
    
    print(f"\n{BC}")
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

def  duyetKHAll(browser):
    _URL = 'https://dpm.dmsone.vn/catalog_customer_mng/info'
    browser.get(_URL)
    print(browser.title)
    status =  browser.find_element(By.ID,'status')
    select = Select(status)
    select.select_by_visible_text('Dự thảo')
    time.sleep(1)
    print('btnSearch')
    btnSearch = browser.find_element(By.ID,'btnSearch')
    btnSearch.click()
    time.sleep(1)
    print('checkAll')
    checkAll = browser.find_element(By.ID,'checkAll')
    checkAll.click()
    time.sleep(1)
    print('btnDuyet')
    btnDuyet = browser.find_element(By.ID,'group_insert_btnAgree')
    btnDuyet.click()
    time.sleep(1)
    print('btnAgree')
    errMsg = 'Success'
    try:
        btnAgree = browser.find_element(By.XPATH,'/html/body/div[22]/div[2]/div[4]/a[1]/span/span')
        print(btnAgree.text)
        btnAgree.click()
    except:
        time.sleep(1)
        errMsg = browser.find_element(By.ID,'errMsg').text
    return errMsg
# %%
########################
# begein DAG
with DAG(
    dag_id="DMS_export_daily",
    schedule_interval="10 9,21 * * *",
    # schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, tz="Asia/Bangkok"),
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    printLog = print_context()
    # with TaskGroup("section_0", tooltip="Tasks for Load Data") as section_0:
    #         RunPythonScript = BashOperator(
    #             task_id="Remove_Exist_File",
    #             bash_command=f"ls {download_path}; exit 0;",
    #             # bash_command=f"dir   {download_path}",
    #         )
            
            

    with TaskGroup("section_1", tooltip="Tasks for Load Data") as section_1:
        # Task 1
        @task(task_id=f"duyetKH")
        def duyetKH():
            browser = webdriver.Chrome(service=service, options=chrome_options)
            login(browser)
            result = duyetKHAll(browser)
            print(f'duyetKH result {result}')
            browser.quit()
            time.sleep(10)
        duyetKH = duyetKH()
        
        @task(task_id=f"DMS_export_daily")
        def DMS_export_daily():
            print(f"{begin} => {end}")
            print(download_path)
            browser = webdriver.Chrome(service=service, options=chrome_options)
            login(browser)

            exportUnits(browser)
            exportMaster("https://dpm.dmsone.vn/catalog_customer_mng/info", browser)
            #Danh_sach_nhan_vien
        
            exportMaster("https://dpm.dmsone.vn/catalog/product/infoindex", browser)
            #Thong_Tin_San_Pham

            n = 3
            
            dsBaocao = pd.read_csv("/home/nhdminh/DMS/dags/DSBaoCao.csv")
            dsBaocao = dsBaocao[~dsBaocao['file_name'].isna()]
            for index,row in dsBaocao.iterrows():
                try:
                    print(row)
                    for i in range(0, n):
                        result = exportBC_index(browser, row["Lv1"], row["Lv2"])
                        if result==1:break
                        if i ==n:
                            print("failed")
                except: 
                    print(f"Lỗi xuất báo cáo {row['Lv1']}, {row['Lv2']} ")
                    pass    


                    
            for f in (os.listdir(download_path)):
                print(f)
        DMS_export_daily = DMS_export_daily()
        

        
        @task(task_id=f"Upload_report")
        def DMS_upload_report():
            for f in os.listdir(localPath):
                site.upload_file_sharepoint(
                    localPath,
                    f"{folder_url_shrpt}/{subfolder_url_shrpt}/{noww}",
                    f,
                    url_shrpt,
                )

        DMS_upload_report = DMS_upload_report()
        duyetKH >> DMS_export_daily >> DMS_upload_report

    start >> printLog >>  section_1
#9.3.Bao_cao_theo_doi_khac_phuc