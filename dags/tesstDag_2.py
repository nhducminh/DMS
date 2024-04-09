from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup # type: ignore

import pendulum


from selenium import webdriver # type: ignore
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities # type: ignore
from selenium.webdriver.chrome.service import Service # type: ignore
from selenium.webdriver.common.by import By # type: ignore
from selenium.webdriver.common.keys import Keys # type: ignore
from selenium.webdriver.chrome.options import Options # type: ignore


import socket

def get_local_ip():
    try:
        # Create a socket and connect to an external server (e.g., Google's public DNS server)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        return f"Error: {str(e)}"

with DAG(
    dag_id="DAG_Test_02",
    schedule="30 23 * * *",
    # schedule="@daily",
    start_date=pendulum.datetime(2023, 10, 30, tz="Asia/Bangkok"),
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("section_1", tooltip="Tasks for Load Data") as section_1:
        # Task 1
        @task(task_id=f"DMS_export_daily")
        def DMS_export_daily():
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--window-size=1920,1080")
            # Call the function to get the local IP address
            local_ip_address = get_local_ip()

            # Print the local IP address
            print(f"Local IP address: {local_ip_address}")
            print(local_ip_address)
            driver = webdriver.Remote(command_executor=f'http://selenium-hub:4444/wd/hub', options=chrome_options,)
            driver.get("https://vnexpress.net/")
            print(driver.title)
            time_now = driver.find_element(By.CLASS_NAME,"time-now")
            print(time_now.text)

            
        DMS_export_daily = DMS_export_daily()

    start >> section_1
