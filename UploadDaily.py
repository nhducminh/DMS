from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
import datetime as dt
from pprint import pprint
from typing import List
import pytz
import pandas as pd
import pendulum
import pysharepoint as ps
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version

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
# This will be the URL that points to your sharepoint site.
# Make sure you change only the parts of the link that start with "Your"
sharepoint_base_url = "https://pvfcco.sharepoint.com"
url_shrpt = "https://pvfcco.sharepoint.com/sites/DMSTNB"
username_shrpt = "nhdminh@pvfcco.com.vn"
password_shrpt = "a3671c389"
folder_url_shrpt = "/sites/DMSTNB/Shared%20Documents"
# subfolder_url_shrpt = "Báo cáo - DMS/Daily"
# subfolder_url_shrpt = "UploadDaily"
subfolder_url_shrpt = "Báo cáo - DMS/DMS_daily"

noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")
# download_path = 'download_path' + "/" + noww
localPath = "/home/nhdminh/DMS/DMS_daily" + "/" + noww

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


for f in os.listdir(localPath):
    site.upload_file_sharepoint(
        localPath,
        f"{folder_url_shrpt}/{subfolder_url_shrpt}/{noww}",
        f,
        url_shrpt,
    )
