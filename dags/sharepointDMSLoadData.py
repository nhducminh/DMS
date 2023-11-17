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


##############################################################
# def function
def print_folder_contents(ctx, folder_url, type):
    try:
        folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        fold_names = []
        if type == "file":
            sub_folders = (
                folder.files
            )  # Replace files with folders for getting list of folders
        else:
            sub_folders = (
                folder.folders
            )  # Replace folders with files for getting list of folders

        ctx.load(sub_folders)
        ctx.execute_query()

        for s_folder in sub_folders:
            fold_names.append(s_folder.properties["Name"])

        return fold_names

    except Exception as e:
        print("Problem printing out library contents: ", e)


def fileList(folder, ctx):
    result = []

    folderList = print_folder_contents(ctx, folder, "")
    if folderList:
        for subFolder in folderList:
            temp = fileList(folder + "/" + subFolder, ctx)
            for t in temp:
                result.append(t)

    fList = print_folder_contents(ctx, folder, "file")
    if fList:
        for f in fList:
            result.append(folder + "/" + f)

    return result


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

# %%
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


# %%
def cleanVTKHC(dft):
    dft = dft[3:].reset_index(drop=True)
    dft.columns = dft.iloc[0]
    dft = dft[1:].reset_index(drop=True)
    dft = dft.drop(columns="STT")
    return dft


def MasterData1(folder, fileName):
    # Get a list of file paths for files with the specified name pattern in the given directory
    # "Danh_sach_nhan_vien20231106083537",
    # Units20230919133446.xls
    sourcePath = localPath + "/" + folder

    temp = pd.to_datetime("20210101T000000", format="ISO8601")
    for f in os.listdir(sourcePath):
        print(f)
        if f.find("csv") > -1:
            continue
        T = f[len(folder) : len(folder) + 8] + "T" + f[len(folder) + 8 : f.find(".")]
        date_time_obj = pd.to_datetime(T, format="ISO8601")

        if date_time_obj == max(date_time_obj, temp):
            final = f
            temp = date_time_obj
        print(f"final {final}")

        dfUnits = pd.read_excel(sourcePath + "/" + final)
        # Save the cleaned and processed DataFrame to a CSV file
        print(f"save to {localPath}/Summary/{fileName}")
        dfUnits.to_csv(localPath + f"/Summary/{fileName}", index=False)
        print(
            f"***\n upload dfUnits.csv: {localPath} -> {folder_url_shrpt}/{subfolder_url_shrpt}"
        )
        site.upload_file_sharepoint(
            localPath + "/Summary/",
            f"{folder_url_shrpt}/{subfolder_url_shrpt}/Summary",
            fileName,
            url_shrpt,
        )


def MasterData2(folder, fileName):
    # 20231116083834_Thong_Tin_San_Pham
    # 20231113080128_Du_lieu_danh_muc_khachhang
    sourcePath = localPath + "/" + folder

    # Get a list of file paths for files with the specified name pattern in the given directory
    temp = pd.to_datetime("20210101T000000", format="ISO8601")
    for f in os.listdir(sourcePath):
        print(f)
        if f.find("csv") > -1:
            continue
        T = f[0:8] + "T" + f[8:14]

        date_time_obj = pd.to_datetime(T, format="ISO8601")

        if date_time_obj == max(date_time_obj, temp):
            final = f
            temp = date_time_obj
        print(f"final {final}")

        dfUnits = pd.read_excel(sourcePath + "/" + final)
        # Save the cleaned and processed DataFrame to a CSV file
        print(f"save to {localPath}/Summary/{fileName}")
        dfUnits.to_csv(localPath + f"/Summary/{fileName}", index=False)
        print(
            f"***\n upload dfUnits.csv: {localPath} -> {folder_url_shrpt}/{subfolder_url_shrpt}"
        )
        site.upload_file_sharepoint(
            localPath + "/Summary/",
            f"{folder_url_shrpt}/{subfolder_url_shrpt}/Summary",
            fileName,
            url_shrpt,
        )


########################
# begein DAG
with DAG(
    dag_id="SharepointLoadData",
    # schedule_interval="0 */6 * * *",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, tz="UTC"),
    catchup=False,
    tags=["DMS"],
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
        @task(task_id=f"LoadSharePointFile")
        def LoadSharePointFile():
            # dfFolderPath = pd.DataFrame(columns=["Folder", "File"])
            subFolder = folder_url_shrpt + "/" + subfolder_url_shrpt
            # folderlist_shrpt = print_folder_contents(ctx, subFolder, "")
            flist = fileList(folder_url_shrpt + "/" + subfolder_url_shrpt, ctx)

            folderlist = [
                "Bao_cao_chi_tiet_VTKHC2",
                "Bao_cao_du_lieu_don_hang",
                "Units",
                "Du_lieu_danh_muc_khachhang",
                "Danh_sach_nhan_vien",
                "Thong_Tin_San_Pham",
            ]
            for folder in folderlist:
                try:
                    if not os.path.exists(f"{localPath}/{folder}"):
                        os.mkdir(f"{localPath}/{folder}")
                        print(f"create folder {localPath}/{folder}")
                except:
                    pass

                for f in flist:
                    foldername = f[: str.rfind(f, "/")]
                    filename = f[str.rfind(f, "/") + 1 :]

                    if str.find(f, folder) > -1:
                        site.download_file_sharepoint(
                            foldername, f"{localPath}/{folder}", filename, url_shrpt
                        )
                        print(f"{localPath}/{folder}/{filename}")

        LoadSharePointFile = LoadSharePointFile()

        @task(task_id=f"Bao_cao_chi_tiet_VTKHC2")
        def BcVTKHC2():
            subFolder = folder_url_shrpt + "/" + subfolder_url_shrpt
            flist = fileList(folder_url_shrpt + "/" + subfolder_url_shrpt, ctx)

            sourcePath = localPath + "/Bao_cao_chi_tiet_VTKHC2"
            # Create an empty DataFrame for storing the combined data
            dfVTKHC2 = pd.DataFrame()
            # Get a list of file paths for files with the specified name pattern in the given directory
            for f in os.listdir(sourcePath):
                # Read Excel data from the file into a temporary DataFrame
                try:
                    print(f)

                    dftemp = pd.read_excel(sourcePath + "/" + f, engine="openpyxl")
                    # Apply the cleanVTKHC function to the temporary DataFrame
                    dftemp = cleanVTKHC(dftemp)

                    # Concatenate the temporary DataFrame with the main DataFrame dfVTKHC2
                    dfVTKHC2 = pd.concat([dftemp, dfVTKHC2])
                except:
                    pass

            filter = dfVTKHC2["Miền"] == "Vùng đào tạo 1"
            dfVTKHC2 = dfVTKHC2[~filter]

            # Remove duplicate rows based on selected columns ('MÃ NVTT', 'Mã KH', 'Ngày')
            dfVTKHC2 = dfVTKHC2.drop_duplicates(subset=["MÃ NVTT", "Mã KH", "Ngày"])

            # Convert the 'Ngày' column to datetime format using the specified date format
            dfVTKHC2["Ngày"] = pd.to_datetime(dfVTKHC2["Ngày"], format="%d-%m-%Y")

            # Save the cleaned and processed DataFrame to a CSV file
            print(f"save to {localPath} /dfVTKHC2.csv")
            dfVTKHC2.to_csv(localPath + "/Summary/dfVTKHC2.csv", index=False)

            print(
                f"***\n upload dfVTKHC2.csv: {localPath} -> {folder_url_shrpt}/{subfolder_url_shrpt}"
            )
            site.upload_file_sharepoint(
                localPath + "/Summary/",
                f"{folder_url_shrpt}/{subfolder_url_shrpt}/Summary",
                "dfVTKHC2.csv",
                url_shrpt,
            )

        BcVTKHC2 = BcVTKHC2()

        @task(task_id=f"Bao_cao_du_lieu_don_hang")
        def BcDonHang():
            subFolder = folder_url_shrpt + "/" + subfolder_url_shrpt
            flist = fileList(folder_url_shrpt + "/" + subfolder_url_shrpt, ctx)

            sourcePath = localPath + "/Bao_cao_du_lieu_don_hang"
            # Create an empty DataFrame for storing the combined data
            dfDonHang = pd.DataFrame()
            # Get a list of file paths for files with the specified name pattern in the given directory
            for f in os.listdir(sourcePath):
                # Read Excel data from the file into a temporary DataFrame
                try:
                    print(f)

                    dftemp = pd.read_excel(sourcePath + "/" + f, engine="openpyxl")
                    # Apply the cleanVTKHC function to the temporary DataFrame
                    dftemp = cleanVTKHC(dftemp)

                    # Concatenate the temporary DataFrame with the main DataFrame dfVTKHC2
                    dfDonHang = pd.concat([dftemp, dfDonHang])
                except:
                    pass
            # Remove duplicate rows based on selected columns ('Số đơn hàng', 'Trạng Thái')
            dfDonHang = dfDonHang.drop_duplicates(subset=["Số đơn hàng", "Trạng Thái"])

            # Replace specific values in the 'Khu vực' column
            dfDonHang["Khu vực"] = dfDonHang["Khu vực"].str.replace(
                "Khu vực Miền Bắc", "Công ty Miền Bắc"
            )
            dfDonHang["Khu vực"] = dfDonHang["Khu vực"].str.replace(
                "Khu vực Miền Trung", "Công ty Miền Trung"
            )
            dfDonHang["Khu vực"] = dfDonHang["Khu vực"].str.replace(
                "Khu vực Tây Nam Bộ", "Công ty Tây Nam Bộ"
            )
            dfDonHang["Khu vực"] = dfDonHang["Khu vực"].str.replace(
                "Khu vực Đông Nam Bộ", "Công ty Đông Nam Bộ"
            )
            dfDonHang["Khu vực"] = dfDonHang["Khu vực"].str.replace(
                "KV Đào tạo 1", "Vùng đào tạo 1"
            )

            # Filter rows where the 'Khu vực' column is not equal to 'Vùng đào tạo 1'
            filter = dfDonHang["Khu vực"] == "Vùng đào tạo 1"
            dfDonHang = dfDonHang[~filter]

            # Convert the 'Ngày đặt hàng' and 'Ngày giao hàng' columns to datetime format
            dfDonHang["Ngày đặt hàng"] = pd.to_datetime(
                dfDonHang["Ngày đặt hàng"], format="%d/%m/%Y"
            )
            dfDonHang["Ngày giao hàng"] = pd.to_datetime(
                dfDonHang["Ngày giao hàng"], format="%d/%m/%Y"
            )

            # Save the cleaned and processed DataFrame to a CSV file
            print(f"save to {localPath}/Summary/dfDonHang.csv")
            dfDonHang.to_csv(localPath + "/Summary/dfDonHang.csv", index=False)
            print(
                f"***\n upload dfDonHang.csv: {localPath} -> {folder_url_shrpt}/{subfolder_url_shrpt}"
            )
            site.upload_file_sharepoint(
                localPath + "/Summary/",
                f"{folder_url_shrpt}/{subfolder_url_shrpt}/Summary",
                "dfDonHang.csv",
                url_shrpt,
            )

        BcDonHang = BcDonHang()

        @task(task_id=f"MasterData")
        def MasterData():
            subFolder = folder_url_shrpt + "/" + subfolder_url_shrpt
            flist = fileList(folder_url_shrpt + "/" + subfolder_url_shrpt, ctx)
            # %% Get latest Units file
            # Get a list of file paths for files with the specified name pattern in the given directory
            MasterData1("Units", "dfUnits.csv")
            # %% Get latest Danh_sach_nhanVien file
            # Get a list of file paths for files with the specified name pattern in the given directory
            MasterData1("Danh_sach_nhan_vien", "dfDSNV.csv")
            # %% Get latest Danh_sach_nhanVien file
            # Get a list of file paths for files with the specified name pattern in the given directory
            MasterData2("Thong_Tin_San_Pham", "dfDSSP.csv")
            # %% Get latest Danh_sach_nhanVien file
            # Get a list of file paths for files with the specified name pattern in the given directory
            MasterData2("Du_lieu_danh_muc_khachhang", "dfDSKHC2.csv")

        MasterData = MasterData()

        LoadSharePointFile >> [BcVTKHC2, BcDonHang, MasterData]
        # MasterData
    end = EmptyOperator(task_id="end")

    start >> printLog >> section_1 >> end
