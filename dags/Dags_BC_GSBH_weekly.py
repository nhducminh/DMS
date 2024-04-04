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
import datetime as dt
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


# %%
########################
# begein DAG
with DAG(
    dag_id="BC_GSBH_weekly",
    schedule_interval="55 23 * * 0",
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

    with TaskGroup("section_2", tooltip="Tasks for Load Data") as section_2:
        RunPythonScript = BashOperator(
            task_id="cleanData",
            bash_command="/bin/python3 '/home/nhdminh/DMS/dags/BC_GSBH.py'",
        )
    start >> printLog >> section_2
    # start >> printLog >> section_1 >> section_2
