#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
from __future__ import annotations

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


##############################################################
# def function


def insertDF(df, list):
    df.loc[-1] = list  # adding a row
    df.index = df.index + 1  # shifting index
    df = df.sort_index()  # sorting by index


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
localPath = "/home/nhdminh/airflow"
##############################################################
url = "https://api-production.2nong.vn/v0/products"

noww = dt.date.today()
print(noww)
# %%

########################
# begein DAG
with DAG(
    dag_id="2NongPrice",
    # schedule_interval="0 */6 * * *",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, tz="UTC"),
    catchup=False,
    tags=["example"],
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
            for x in range(0, 5):
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
                    df = pd.json_normalize(data["data"], sep="_")

                    print(df)  # Displaying the first few rows of the DataFrame
                else:
                    print("Failed to fetch data from the API")
            df.to_csv(f"{noww}_2nongPrice.csv", index=False)
            dfList = df.explode("info").reset_index(drop=True)
            # dfList = dfList.explode('info').reset_index(drop=True)
            temp = pd.DataFrame(dfList["info"].values.tolist())
            # temp.to_csv("temp.csv",index=False)
            dfList = dfList.drop(columns="info", axis=1).join(temp, rsuffix="_temp")
            dfList.to_csv(f"2nong/{noww}.csv", index=False)

        get2NongPrice = get2NongPrice()

        # LoadSharePointFile >>
        [get2NongPrice]
        # MasterData
    end = EmptyOperator(task_id="end")

    start >> printLog >> section_1 >> end
