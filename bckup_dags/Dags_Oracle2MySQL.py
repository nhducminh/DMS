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
import sys
import tempfile
import time
from pprint import pprint
from typing import List

import cx_Oracle
import pandas as pd
import pendulum
from sqlalchemy import create_engine

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()


fromYEAR = Variable.get("PO_year")

monthDuration = 2


def gen_tbl_cols_sql(df):
    dmap = dtype_mapping()
    sql = "pi_db_uid INT AUTO_INCREMENT PRIMARY KEY"
    df1 = df.rename(columns={"": "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for hl in hdrs_list:
        sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
    return sql


def dtype_mapping():
    return {
        "object": "TEXT",
        "int64": "INT",
        "float64": "FLOAT",
        "datetime64": "DATETIME",
        "bool": "TINYINT",
        "category": "TEXT",
        "timedelta[ns]": "TEXT",
        "datetime64[ns]": "DATETIME",
    }


def create_mysql_tbl_schema(df, conn, db, tbl_name):
    print(tbl_name)
    tbl_cols_sql = gen_tbl_cols_sql(df)
    print(tbl_cols_sql)
    sql = "USE {0}; CREATE TABLE {1} ({2})".format(db, tbl_name, tbl_cols_sql)
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.commit()
    print("done")


def updateTbls(tb):
    tbl_name = tb.replace(".", "_")
    """This is a function that will run within the DAG execution"""
    ###################################
    # get data from Oracle DB
    sept = tb.find(".")
    oracle_table_name = tb[sept + 1 :].upper()
    oracle_owner = tb[:sept].upper()
    print(f"{oracle_owner}.{oracle_table_name}")
    query = f"""
            select COLUMN_NAME
            from sys.all_tab_columns col
            inner join sys.all_tables t on col.owner = t.owner
                                        and col.table_name = t.table_name
            where col.owner = '{oracle_owner}'
            and col.table_name = '{oracle_table_name}'
            order by col.column_id
            """
    columnss = list(pd.read_sql(query, db_Oracle).COLUMN_NAME)
    if "requisition_headers_all" in tb:
        rmcolumn = ["ATTRIBUTE2", "ATTRIBUTE3", "ATTRIBUTE4", "DESCRIPTION"]
        for c in rmcolumn:
            columnss.remove(c)
        list_columns = ""
        for c in columnss:
            list_columns = list_columns + "a." + c + ","
        list_columns = list_columns[: len(list_columns) - 1]
        query = f"""SELECT
                {list_columns},
                TRANSLATE (b.ATTRIBUTE2, 'x'||CHR(10)||CHR(13), 'x') as ATTRIBUTE2,
                TRANSLATE (b.ATTRIBUTE3, 'x'||CHR(10)||CHR(13), 'x') as ATTRIBUTE3,
                TRANSLATE (b.ATTRIBUTE4, 'x'||CHR(10)||CHR(13), 'x') as ATTRIBUTE4,
                TRANSLATE (b.DESCRIPTION, 'x'||CHR(10)||CHR(13), 'x') as DESCRIPTION
            FROM
                po.po_requisition_headers_all a
                INNER JOIN po.po_requisition_headers_all b ON a.requisition_header_id = b.requisition_header_id
            where EXTRACT(YEAR FROM a.creation_date) > {fromYEAR}"""

    elif ("gl_code_combinations" in tb) or ("fnd_user" in tb):
        query = f"""select * from {tb} h"""

    elif "ap_suppliers" in tb:
        columnss.remove("VALIDATION_NUMBER")
        list_columns = ""
        for c in columnss:
            list_columns = list_columns + "a." + c + ","
        list_columns = list_columns[: len(list_columns) - 1]
        query = f"""SELECT
                {list_columns}
                from   {tb}  a
                """
    elif "mtl_system_items_b" in tb:
        query = f"""SELECT a.inventory_item_id,a.description,a.segment1
                from   {tb}  a
                where a.ORGANIZATION_ID = 81
                """
    else:
        query = f"""select * from {tb} h where EXTRACT(YEAR FROM h.creation_date) > {fromYEAR}"""
    print(query)

    df = pd.read_sql(query, db_Oracle)
    rowcount = len(df)
    ####################################
    print(f"Begin Task create_table_SQL; Table name = {tbl_name}")

    # read dataframe from ERP

    print(f"Create table in stgERP")
    database = "stgERP"
    engine = create_engine(
        f"mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    )

    try:
        print(f"Try to Create table in {database} ")
        create_mysql_tbl_schema(df, engine.raw_connection(), database, tbl_name)
        print(f"Create table {database} success, row number {rowcount}")
    except Exception as e:
        print(e)

    try:
        print(f"Try to Copy data {tbl_name} to {database}")
        df.to_sql(tbl_name, engine, if_exists="replace")
        print(f"Try to Copy data {tbl_name} to {database} - Done ")
    except Exception as e:
        print(e)
    print(len(df))
    print("x")


factTableList = [
    "po.po_headers_all",
    "po.po_lines_all",
    "po.po_distributions_all",
    "po.po_requisition_headers_all",
    "po.po_requisition_lines_all",
    "po.po_req_distributions_all",
    "po.rcv_shipment_headers",
    "po.rcv_shipment_lines",
    "po.rcv_transactions",
]
dimTableList = [
    "ap.ap_suppliers",
    "gl.gl_code_combinations",
    "apps.fnd_user",
    "inv.mtl_system_items_b",
]


@task
def fn():
    pass

    # DAG Order
    # Read data from Oracle DB
    # Write stgERP all FactTable
    # ETL from stgERP to Airflow_DB


# Oracle login
Oracle = {
    "username": "dpm_powerbi",
    "password": "powerbi01",
    "encoding": "UTF-8",
    "ip": "192.168.2.33",
    "port": 1523,
    "SID": "DEV",
}

# mysql login
user = "cleandata"
password = "a3671c389"
host = "192.168.10.100"
port = "3306"

with DAG(
    dag_id="Oracle2MySQL",
    schedule_interval="0 */6 * * *",
    # schedule_interval="@hourly",
    start_date=pendulum.datetime(2023, 10, 30, tz="UTC"),
    catchup=False,
    tags=["ERP"],
) as dag:
    # %%
    # [START howto_operator_python]
    # @task(
    #     task_id="log_sql_query",
    #     templates_dict={"query": "sql/sample.sql"},
    #     templates_exts=[".sql"],
    # )
    # def log_sql(**kwargs):
    #     logging.info(
    #         "Python task decorator query: %s", str(kwargs["templates_dict"]["query"])
    #     )
    # %%
    start = EmptyOperator(task_id="start")
    print("hourly")

    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    printLog = print_context()

    with TaskGroup("LoadFactTable", tooltip="Tasks for Load Fact Tables") as section_1:
        dsn_tns = cx_Oracle.makedsn(Oracle["ip"], Oracle["port"], Oracle["SID"])
        db_Oracle = cx_Oracle.connect(Oracle["username"], Oracle["password"], dsn_tns)
        countx = 0
        my_tasks_Fact = []
        my_tasks_Dim = []
        # %%
        for tbs in factTableList:
            #  @task(task_id=f"create_table_SQL_{tbs}")
            @task(task_id=f"create_table_fact_{tbs}")
            def UpdateTable(tb):
                updateTbls(tb)
                ####################################

            # countx = countx + 1
            UpdateERP = UpdateTable(tbs)
            my_tasks_Fact.append(UpdateERP)

            for i in range(0, len(my_tasks_Fact)):
                try:
                    if i % 2 != 1:
                        [my_tasks_Fact[i], my_tasks_Fact[i + 1]]
                    else:
                        [my_tasks_Fact[i] >> my_tasks_Fact[i + 1]]
                except:
                    pass

    with TaskGroup("LoadDimTable", tooltip="Tasks for Load Dim Tables") as section_2:
        for tbs in dimTableList:
            #  @task(task_id=f"create_table_SQL_{tbs}")
            @task(task_id=f"create_table_dim_{tbs}")
            def UpdateTable(tb):
                updateTbls(tb)
                ####################################

            # countx = countx + 1
            UpdateERP = UpdateTable(tbs)

            my_tasks_Dim.append(UpdateERP)

            for i in range(0, len(my_tasks_Dim)):
                try:
                    [my_tasks_Dim[i], my_tasks_Dim[i + 1]]
                except:
                    pass

    # %%
    # with TaskGroup("section_2", tooltip="Tasks for ETL") as section_2:

    #     @task(task_id=f"ETL_PO")
    #     def ETL_PO():
    #         database = "stgERP"
    #         engine = create_engine(
    #             f"mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    #         )
    #         query = f"""SELECT DISTINCT
    #                 d.po_distribution_id AS d_po_distribution_id,
    #                 d.po_line_id AS d_po_line_id,
    #                 d.code_combination_id AS d_code_combination_id,
    #                 d.quantity_ordered AS d_quantity_ordered,
    #                 d.creation_date AS d_creation_date,
    #                 d.created_by AS d_created_by,
    #                 d.quantity_delivered AS d_quantity_delivered,
    #                 d.req_distribution_id AS d_req_distribution_id,
    #                 d.amount_billed AS d_amount_billed,
    #                 d.destination_type_code AS d_destination_type_code,
    #                 d.destination_organization_id AS d_destination_organization_id,
    #                 d.destination_subinventory AS d_destination_subinventory,
    #                 d.recoverable_tax AS d_recoverable_tax,
    #                 d.nonrecoverable_tax AS d_nonrecoverable_tax,
    #                 l.po_line_id AS l_po_line_id,
    #                 l.po_header_id AS l_po_header_id,
    #                 l.line_num AS l_line_num,
    #                 l.creation_date AS l_creation_date,
    #                 l.created_by AS l_created_by,
    #                 l.item_id AS l_item_id,
    #                 l.item_description AS l_item_description,
    #                 l.unit_price AS l_unit_price,
    #                 l.quantity AS l_quantity,
    #                 (l.unit_price * l.quantity) AS l_amount,
    #                 l.from_header_id AS l_from_header_id,
    #                 l.from_line_id AS l_from_line_id,
    #                 h.po_header_id AS h_po_header_id,
    #                 h.segment1 AS po_number,
    #                 h.creation_date AS h_creation_date,
    #                 h.created_by AS h_created_by,
    #                 h.approved_date AS h_approved_date,
    #                 h.comments AS h_comments,
    #                 h.attribute1 AS h_attribute1,
    #                 h.attribute2 AS h_attribute2,
    #                 h.attribute3 AS h_attribute3,
    #                 h.attribute4 AS h_attribute4,
    #                 h.attribute5 AS h_attribute5,
    #                 h.attribute6 AS h_attribute6,
    #                 h.attribute7 AS h_attribute7,
    #                 h.attribute8 AS h_attribute8,
    #                 h.attribute9 AS h_attribute9,
    #                 h.attribute10 AS h_attribute10,
    #                 h.attribute11 AS h_attribute11,
    #                 h.attribute12 AS h_attribute12,
    #                 h.attribute13 AS h_attribute13,
    #                 h.org_id AS h_org_id,
    #                 g.segment1 AS g_segment1,
    #                 g.segment2 AS g_segment2,
    #                 g.segment3 AS g_segment3,
    #                 g.segment4 AS g_segment4,
    #                 g.segment5 AS g_segment5,
    #                 u.user_id AS U_user_id,
    #                 u.user_name AS u_user_name,
    #                 a.VENDOR_NAME AS a_VENDOR_NAME
    #             FROM
    #                 po_po_headers_all h
    #                     LEFT JOIN
    #                 po_po_lines_all l ON h.po_header_id = l.po_header_id
    #                     LEFT JOIN
    #                 po_po_distributions_all d ON d.po_line_id = l.po_line_id
    #                     INNER JOIN
    #                 inv_mtl_system_items_b i ON i.inventory_item_id = l.item_id
    #                     INNER JOIN
    #                 ap_ap_suppliers a ON h.vendor_id = a.vendor_id
    #                     INNER JOIN
    #                 gl_gl_code_combinations g ON g.code_combination_id = d.code_combination_id
    #                     INNER JOIN
    #                 apps_fnd_user u ON u.user_id = h.created_by
    #                 where h.org_id = 81 """
    #         df = pd.read_sql(query, engine)
    #         print(len(df))

    #     ETLPo = ETL_PO()

    #     @task(task_id=f"ETL_PR")
    #     def ETL_PR():
    #         database = "stgERP"
    #         engine = create_engine(
    #             f"mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    #         )
    #         query = f"""SELECT DISTINCT
    #                 d.po_distribution_id AS d_po_distribution_id,
    #                 d.po_line_id AS d_po_line_id,
    #                 d.code_combination_id AS d_code_combination_id,
    #                 d.quantity_ordered AS d_quantity_ordered,
    #                 d.creation_date AS d_creation_date,
    #                 d.created_by AS d_created_by,
    #                 d.quantity_delivered AS d_quantity_delivered,
    #                 d.req_distribution_id AS d_req_distribution_id,
    #                 d.amount_billed AS d_amount_billed,
    #                 d.destination_type_code AS d_destination_type_code,
    #                 d.destination_organization_id AS d_destination_organization_id,
    #                 d.destination_subinventory AS d_destination_subinventory,
    #                 d.recoverable_tax AS d_recoverable_tax,
    #                 d.nonrecoverable_tax AS d_nonrecoverable_tax,
    #                 l.po_line_id AS l_po_line_id,
    #                 l.po_header_id AS l_po_header_id,
    #                 l.line_num AS l_line_num,
    #                 l.creation_date AS l_creation_date,
    #                 l.created_by AS l_created_by,
    #                 l.item_id AS l_item_id,
    #                 l.item_description AS l_item_description,
    #                 l.unit_price AS l_unit_price,
    #                 l.quantity AS l_quantity,
    #                 (l.unit_price * l.quantity) AS l_amount,
    #                 l.from_header_id AS l_from_header_id,
    #                 l.from_line_id AS l_from_line_id,
    #                 h.po_header_id AS h_po_header_id,
    #                 h.segment1 AS po_number,
    #                 h.creation_date AS h_creation_date,
    #                 h.created_by AS h_created_by,
    #                 h.approved_date AS h_approved_date,
    #                 h.comments AS h_comments,
    #                 h.attribute1 AS h_attribute1,
    #                 h.attribute2 AS h_attribute2,
    #                 h.attribute3 AS h_attribute3,
    #                 h.attribute4 AS h_attribute4,
    #                 h.attribute5 AS h_attribute5,
    #                 h.attribute6 AS h_attribute6,
    #                 h.attribute7 AS h_attribute7,
    #                 h.attribute8 AS h_attribute8,
    #                 h.attribute9 AS h_attribute9,
    #                 h.attribute10 AS h_attribute10,
    #                 h.attribute11 AS h_attribute11,
    #                 h.attribute12 AS h_attribute12,
    #                 h.attribute13 AS h_attribute13,
    #                 h.org_id AS h_org_id,
    #                 g.segment1 AS g_segment1,
    #                 g.segment2 AS g_segment2,
    #                 g.segment3 AS g_segment3,
    #                 g.segment4 AS g_segment4,
    #                 g.segment5 AS g_segment5,
    #                 u.user_id AS U_user_id,
    #                 u.user_name AS u_user_name,
    #                 a.VENDOR_NAME AS a_VENDOR_NAME
    #             FROM
    #                 po_po_headers_all h
    #                     LEFT JOIN
    #                 po_po_lines_all l ON h.po_header_id = l.po_header_id
    #                     LEFT JOIN
    #                 po_po_distributions_all d ON d.po_line_id = l.po_line_id
    #                     INNER JOIN
    #                 inv_mtl_system_items_b i ON i.inventory_item_id = l.item_id
    #                     INNER JOIN
    #                 ap_ap_suppliers a ON h.vendor_id = a.vendor_id
    #                     INNER JOIN
    #                 gl_gl_code_combinations g ON g.code_combination_id = d.code_combination_id
    #                     INNER JOIN
    #                 apps_fnd_user u ON u.user_id = h.created_by
    #                 where h.org_id = 81 """
    #         df = pd.read_sql(query, engine)
    #         print(len(df))

    #     ETLPr = ETL_PR()

    #     @task(task_id=f"ETL_RCV")
    #     def ETL_RCV():
    #         database = "stgERP"
    #         engine = create_engine(
    #             f"mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    #         )
    #         query = f"""SELECT DISTINCT
    #                 d.po_distribution_id AS d_po_distribution_id,
    #                 d.po_line_id AS d_po_line_id,
    #                 d.code_combination_id AS d_code_combination_id,
    #                 d.quantity_ordered AS d_quantity_ordered,
    #                 d.creation_date AS d_creation_date,
    #                 d.created_by AS d_created_by,
    #                 d.quantity_delivered AS d_quantity_delivered,
    #                 d.req_distribution_id AS d_req_distribution_id,
    #                 d.amount_billed AS d_amount_billed,
    #                 d.destination_type_code AS d_destination_type_code,
    #                 d.destination_organization_id AS d_destination_organization_id,
    #                 d.destination_subinventory AS d_destination_subinventory,
    #                 d.recoverable_tax AS d_recoverable_tax,
    #                 d.nonrecoverable_tax AS d_nonrecoverable_tax,
    #                 l.po_line_id AS l_po_line_id,
    #                 l.po_header_id AS l_po_header_id,
    #                 l.line_num AS l_line_num,
    #                 l.creation_date AS l_creation_date,
    #                 l.created_by AS l_created_by,
    #                 l.item_id AS l_item_id,
    #                 l.item_description AS l_item_description,
    #                 l.unit_price AS l_unit_price,
    #                 l.quantity AS l_quantity,
    #                 (l.unit_price * l.quantity) AS l_amount,
    #                 l.from_header_id AS l_from_header_id,
    #                 l.from_line_id AS l_from_line_id,
    #                 h.po_header_id AS h_po_header_id,
    #                 h.segment1 AS po_number,
    #                 h.creation_date AS h_creation_date,
    #                 h.created_by AS h_created_by,
    #                 h.approved_date AS h_approved_date,
    #                 h.comments AS h_comments,
    #                 h.attribute1 AS h_attribute1,
    #                 h.attribute2 AS h_attribute2,
    #                 h.attribute3 AS h_attribute3,
    #                 h.attribute4 AS h_attribute4,
    #                 h.attribute5 AS h_attribute5,
    #                 h.attribute6 AS h_attribute6,
    #                 h.attribute7 AS h_attribute7,
    #                 h.attribute8 AS h_attribute8,
    #                 h.attribute9 AS h_attribute9,
    #                 h.attribute10 AS h_attribute10,
    #                 h.attribute11 AS h_attribute11,
    #                 h.attribute12 AS h_attribute12,
    #                 h.attribute13 AS h_attribute13,
    #                 h.org_id AS h_org_id,
    #                 g.segment1 AS g_segment1,
    #                 g.segment2 AS g_segment2,
    #                 g.segment3 AS g_segment3,
    #                 g.segment4 AS g_segment4,
    #                 g.segment5 AS g_segment5,
    #                 u.user_id AS U_user_id,
    #                 u.user_name AS u_user_name,
    #                 a.VENDOR_NAME AS a_VENDOR_NAME
    #             FROM
    #                 po_po_headers_all h
    #                     LEFT JOIN
    #                 po_po_lines_all l ON h.po_header_id = l.po_header_id
    #                     LEFT JOIN
    #                 po_po_distributions_all d ON d.po_line_id = l.po_line_id
    #                     INNER JOIN
    #                 inv_mtl_system_items_b i ON i.inventory_item_id = l.item_id
    #                     INNER JOIN
    #                 ap_ap_suppliers a ON h.vendor_id = a.vendor_id
    #                     INNER JOIN
    #                 gl_gl_code_combinations g ON g.code_combination_id = d.code_combination_id
    #                     INNER JOIN
    #                 apps_fnd_user u ON u.user_id = h.created_by
    #                 where h.org_id = 81 """
    #         df = pd.read_sql(query, engine)
    #         print(len(df))

    #     ETLRcv = ETL_RCV()
    #     # my_tasks[i] >> ETL_db
    #     [ETLPo, ETLPr, ETLRcv]
    # # [END howto_operator_python_kwargs]
    # %%
    end = EmptyOperator(task_id="end")

    start >> printLog >> section_1 >> section_2 >> end
    # start >> printLog >> section_1 >> end
