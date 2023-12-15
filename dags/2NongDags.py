from __future__ import annotations
import logging
import os
import sys
import tempfile
import time
import pytz


from pprint import pprint
from typing import List

import pandas as pd
import pendulum
import pysharepoint as ps
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
import requests
import pandas as pd
import datetime as dt
from datetime import datetime, timezone


##############################################################
# def function


##############################################################
# This will be the URL that points to your sharepoint site.
# Make sure you change only the parts of the link that start with "Your"
sharepoint_base_url = "https://pvfcco.sharepoint.com"
url_shrpt = "https://pvfcco.sharepoint.com/sites/DMSTNB"
username_shrpt = "nhdminh@pvfcco.com.vn"
password_shrpt = "a3671c389"
folder_url_shrpt = "/sites/DMSTNB/Shared%20Documents"
# subfolder_url_shrpt = "Báo cáo - DMS/Daily"
subfolder_url_shrpt = "Báo cáo - DMS"
localPath = "/home/nhdminh/airflow/2nong_price"
##############################################################
url = "https://api-production.2nong.vn/v0/products"

noww = datetime.now(pytz.timezone("Asia/Bangkok")).date()
# %%
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

########################
# begein DAG
with DAG(
    dag_id="2NongPrice",
    schedule_interval="0 2 * * *",
    # schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, 0, 0, 0, 0, tz="Asia/Bangkok"),
    catchup=False,
    tags=["2Nong"],
) as dag:
    ###############
    # Begin Task
    start = EmptyOperator(task_id="start")

    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    printLog = print_context()
    #################
    # TaskGroup 1
    with TaskGroup("section_1", tooltip="Tasks for Load Data") as section_1:
        # Task 1
        @task(task_id=f"get2NongPrice")
        def get2NongPrice():
            print(noww)

            # Gia phan bon: special_id = 2

            df = pd.DataFrame()
            for x in range(0, 10):
                params = {
                    "page": x,
                    "is_active": 1,
                    "limit": 20,
                    "special_id": 2,
                    "sort[updated_at]": "desc",
                    "name": "",
                    "city_id": "",
                    "date": "",
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    # Flatten the 'info' field if it contains nested data
                    temp = pd.json_normalize(data["data"], sep="_")
                    df = pd.concat([temp, df])
                    # print(df)  # Displaying the first few rows of the DataFrame
                else:
                    print("Failed to fetch data from the API")
            df = df.reset_index(drop=True)
            print(len(df))
            df.to_csv(f"{noww}_2nong_Product_Price.csv", index=False)
            dfList = df.explode("info").reset_index(drop=True)
            temp = pd.DataFrame(dfList["info"].values.tolist())
            dfList = dfList.drop(columns="info", axis=1).join(temp, rsuffix="_temp")
            print(len(dfList))

            print(f"{localPath}/2nong/{noww}_2nong_Product_Price.csv")
            dfList.to_csv(f"{localPath}/2nong/{noww}_2nong_Product_Price.csv", index=False)
            print(
                f"***\n upload {noww}_2nong_Product_Price.csv: {localPath} -> {folder_url_shrpt}/{subfolder_url_shrpt}"
            )
            site.upload_file_sharepoint(
                f"{localPath}/2nong/",
                f"{folder_url_shrpt}/{subfolder_url_shrpt}/2Nong",
                f"{noww}_2nong_Product_Price.csv",
                url_shrpt,
            )
            ########################
            # Gia nong san: special_id = 1
            df = pd.DataFrame()
            for x in range(0, 10):
                params = {
                    "page": x,
                    "is_active": 1,
                    "limit": 20,
                    "special_id": 1,
                    "sort[updated_at]": "desc",
                    "name": "",
                    "city_id": "",
                    "date": "",
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    # Flatten the 'info' field if it contains nested data
                    temp = pd.json_normalize(data["data"], sep="_")
                    df = pd.concat([temp, df])
                    # print(df)  # Displaying the first few rows of the DataFrame
                else:
                    print("Failed to fetch data from the API")
            df = df.reset_index(drop=True)
            print(len(df))

            df.to_csv(f"{noww}_2nong_Goods_Price.csv", index=False)
            dfList = df.explode("info").reset_index(drop=True)
            temp = pd.DataFrame(dfList["info"].values.tolist())
            dfList = dfList.drop(columns="info", axis=1).join(temp, rsuffix="_temp")
            print(len(dfList))
            print(f"{localPath}/2nong/{noww}_2nong_Goods_Price.csv")
            dfList.to_csv(f"{localPath}/2nong/{noww}_2nong_Goods_Price.csv", index=False)
            print(f"***\n upload {noww}_2nong_Goods_Price.csv: {localPath} -> {folder_url_shrpt}/{subfolder_url_shrpt}")
            site.upload_file_sharepoint(
                f"{localPath}/2nong/",
                f"{folder_url_shrpt}/{subfolder_url_shrpt}/2Nong",
                f"{noww}_2nong_Goods_Price.csv",
                url_shrpt,
            )

        get2NongPrice = get2NongPrice()

        # LoadSharePointFile >>
        [get2NongPrice]
        # MasterData
    end = EmptyOperator(task_id="end")

    start >> printLog >> section_1 >> end
