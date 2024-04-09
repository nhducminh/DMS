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


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

remote_webdriver = 'remote_chromedriver'

print(__file__)
print("DMS_export_daily")
print(f'{remote_webdriver}:4444/wd/hub')
with webdriver.Remote(command_executor='http://localhost:4444/wd/hub', options=chrome_options,) as driver:
    driver.get("https://vnexpress.net/")
    print(driver.title)
pass


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

# Call the function to get the local IP address
local_ip_address = get_local_ip()

# Print the local IP address
print(f"Local IP address: {local_ip_address}")
