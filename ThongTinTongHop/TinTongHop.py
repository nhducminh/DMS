import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Define ChromeDriver service
service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed
download_path = "/home/nhdminh/airflow"

# Set Chrome options for headless mode and download directory
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.binary_location = (
    "/usr/bin/google-chrome"  # Replace with the path to the Edge binary
)

# Set the download directory
chrome_options.add_experimental_option(
    "prefs", {"download.default_directory": download_path}
)

# Start the WebDriver
browser = webdriver.Chrome(service=service, options=chrome_options)

# _URL = "https://vietnambiz.vn/gia-phan-bon-hom-nay-2811-lang-song-tai-khu-vuc-mien-trung-va-tay-nam-bo-20231127118499.htm"
# browser.get(_URL)

# print(browser.title)

# Title = browser.find_element(By.CSS_SELECTOR, ".vnbcb-title")
# print("Title ==============>\n ", Title.text)

# Sapo = browser.find_element(By.CSS_SELECTOR, ".vnbcbc-sapo")
# print("Sapo ==============>\n", Sapo.text)

# Content = browser.find_element(By.CSS_SELECTOR, ".vnbcbc-body.vceditor-content ")
# print("Content ==============>\n ", Content.get_attribute("innerHTML"))


dfURL = pd.read_csv("ThongTinTongHop/ThongTinTongHop.csv")
print(list(dfURL))
["date", "url_list", "Webste", "Title", "Sapo", "Content"]

for index, row in dfURL.iterrows():
    _URL = row["url_list"]
    try:
        classTitle = f".{str.replace(str.strip(row['Title']),' ','.')}"
        classSapo = f".{str.replace(str.strip(row['Sapo']),' ','.')}"
        classContent = f".{str.replace(str.strip(row['Content']),' ','.')}"
        print(_URL)

        browser.get(_URL)
        print(browser.title)

        Title = browser.find_element(By.CSS_SELECTOR, classTitle)
        print("Title ==============>\n ", Title.text)

        Sapo = browser.find_element(By.CSS_SELECTOR, classSapo)
        print("Sapo ===============>\n", Sapo.text)

        # Content = browser.find_element(By.CSS_SELECTOR, classContent)
        # print("Content ==============>\n ", Content.get_attribute("innerHTML"))

        print("\n<==============>\n ")
    except Exception as e:
        print(f"Error\n{classTitle} {classSapo} {classContent}")
        # print(e)
