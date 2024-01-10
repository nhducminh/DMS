from __future__ import annotations
import logging
import os
import sys
import tempfile
import time
import datetime as dt
from pprint import pprint
from typing import List

import pandas as pd
import pendulum
import pysharepoint as ps
import datetime as dt
import pytz

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from sqlalchemy import create_engine

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

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


parent_path = os.path.abspath(os.path.join(os.path.abspath(""), os.pardir))
download_path = f"{os.path.abspath('')}/DMS/DMS_daily"


noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")
download_path = download_path + "/" + noww
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


def download_wait(path_to_downloads):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 30:
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


# %% def exportMaster(_URL,browser, download_path):


def exportMaster(_URL, browser):
    print("exportMaster")
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


# %% def exportBC(browser, download_path,ID_BC,sub_ID_BC,toDate,ID_toDate,ID_btn_click):


def exportBC(browser, download_path, ID_BC, sub_ID_BC, toDate, ID_toDate, ID_btn_click):
    # Export file
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
    fromdate = ReportCtnSection.find_element(By.ID, toDate)
    fromdate.click()
    fromdate.send_keys(begin)

    time.sleep(1)

    todate = ReportCtnSection.find_element(By.ID, ID_toDate)
    todate.click()
    todate.send_keys(end)
    time.sleep(1)

    btnReport = ReportCtnSection.find_element(By.ID, ID_btn_click)
    btnReport.click()
    time.sleep(10)

    download_wait(download_path)


# %% def exportUnits(browser, download_path):


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


def exportBC_index(browser, Lv1, Lv2):
    dsBaocao = pd.read_csv("/home/nhdminh/DMS/dags/DSBaoCao.csv")
    filterBC1 = dsBaocao["Lv1"] == Lv1
    filterBC2 = dsBaocao["Lv2"] == Lv2
    BC = dsBaocao[filterBC1 * filterBC2].reset_index(drop=True).to_dict()
    print(BC)
    exportBC(
        browser,
        download_path,
        BC["ID_BC"][0],
        BC["sub_ID_BC"][0],
        "fromDate",
        "toDate",
        "btnReport",
    )
    # print((f"{download_path}/{BC['file_name'][0]}"))
    for f in os.listdir(download_path):
        if BC["file_name"][0] in f:
            print(f)
            return 1
    # not downloaded
    print(0)
    print(dt.datetime.now())
    return 0


# %%
########################
# begein DAG
with DAG(
    dag_id="DMS_export_daily_Dags_home",
    schedule_interval="30 23 * * *",
    # schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, tz="UTC"),
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
    with TaskGroup("section_0", tooltip="Tasks for Load Data") as section_0:
        RunPythonScript = BashOperator(
            task_id="Remove_Exist_File",
            bash_command=f"rm -r {download_path}",
            # bash_command=f"dir   {download_path}",
        )
    with TaskGroup("section_1", tooltip="Tasks for Load Data") as section_1:
        # Task 1
        @task(task_id=f"DMS_export_daily")
        def DMS_export_daily():
            print(f"{begin} => {end}")
            print(download_path)
            browser = webdriver.Chrome(service=service, options=chrome_options)
            login(browser)
            exportUnits(browser)
            exportMaster("https://dpm.dmsone.vn/catalog_customer_mng/info", browser)
            exportMaster("https://dpm.dmsone.vn/catalog/product/infoindex", browser)

            n = 3
            for i in range(0, n):
                result = exportBC_index(browser, 1, 1)
                if result == 1:
                    break

            for i in range(0, n):
                result = exportBC_index(browser, 7, 3)
                if result == 1:
                    break

            for i in range(0, n):
                result = exportBC_index(browser, 10, 1)
                if result == 1:
                    break

        DMS_export_daily = DMS_export_daily()

    start >> printLog >> section_0 >> section_1
