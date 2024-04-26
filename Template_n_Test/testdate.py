<<<<<<< HEAD
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup # type: ignore

=======
import os
import re
import time
>>>>>>> d4b59df (update DMS_Daily)
import pendulum
import pandas as pd, numpy as np



dsBC = pd.read_csv('../dags/DSBaoCao.csv')
dsBC = dsBC[~dsBC.file_name.isna()]
print(dsBC)

for index, row in dsBC.iterrows():    
    for f in os.listdir('../DMS_daily/20240417'):
        if row['file_name'] in f:
            print(f'{row["Lv1"]} {row["Lv2"]} {f}')
    
# Bao_cao_thoi_gian_ghe_tham_kh
# Bc_Don_hang_phat_sinh_trong_ngay
# Danh_sach_nhan_vien
# Bc_Log_iPad
# Thong_Tin_San_Pham.xlsx
# Units
# Bao_cao_san_luong_doanh_so_kh
# Bao_cao_chi_tiet_VTKHC2



#  Lỗi xuất báo cáo 3, 1 
#  Lỗi xuất báo cáo 3, 2 
#  Lỗi xuất báo cáo 4, 1 
#  Lỗi xuất báo cáo 4, 3 
#  Lỗi xuất báo cáo 4, 4 
#  Lỗi xuất báo cáo 5, 1 
#  Lỗi xuất báo cáo 6, 1 
#  Lỗi xuất báo cáo 6, 2 
#  Lỗi xuất báo cáo 7, 4 
#  Lỗi xuất báo cáo 8, 1 
#  Lỗi xuất báo cáo 9, 1 


<<<<<<< HEAD
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
=======
>>>>>>> d4b59df (update DMS_Daily)
