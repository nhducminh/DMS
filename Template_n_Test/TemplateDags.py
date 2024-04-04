import pendulum
import pandas as pd

from airflow import DAG
from airflow import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

# %%
########################
# begein DAG
with DAG(
    dag_id="DMS_export_daily_Dags",
    # schedule_interval="0 */6 * * *",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    printLog = print_context()
    with TaskGroup("section_1", tooltip="Tasks for Load Data") as section_1:
        # Task 1
        @task(task_id=f"DMS_export_daily")
        def DMS_export_daily():
            print(f"{begin} => {end}")
            browser = webdriver.Chrome(service=service, options=chrome_options)
            login(browser)
            exportUnits(browser, download_path)
            exportMaster(
                "https://dpm.dmsone.vn/catalog_customer_mng/info",
                browser,
                download_path,
            )
            exportMaster(
                "https://dpm.dmsone.vn/catalog/product/infoindex",
                browser,
                download_path,
            )
            exportBC(
                browser,
                download_path,
                "REPORT_BCK",
                "REPORT_SALE_ORDER",
                "fDate",
                "tDate",
                "btnSearch",
            )
            exportBC(
                browser,
                download_path,
                "NVTT",
                "BCCTVTKHC2",
                "fromDate",
                "toDate",
                "btnReport",
            )

        LoadSharePointFile = DMS_export_daily()

    # end = EmptyOperator(task_id="end")
    # start >> printLog >> section_1 >> end
    start >> printLog >> section_1
