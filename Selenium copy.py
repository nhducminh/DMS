import pandas as pd
import time
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


# Example: Open a webpage and print its title
browser.get("https://dpm.dmsone.vn/login")

print("Page Title 1:", browser.title)

# Perform your Selenium actions here...

try:
    elem = browser.find_element(By.NAME, "username")  # Find the search box
    elem.send_keys("madmin")
    elem = browser.find_element(By.NAME, "password")  # Find the search box
    elem.send_keys("PVFCCo@2023" + Keys.RETURN)
    print("Page Title 2:", browser.title)

except Exception as e:
    print(e)
    pass

print("Page Title 3:", browser.title)
time.sleep(2)

_URL = "https://dpm.dmsone.vn/catalog_customer_mng/info"
id = "btnExportExcel"

try:
    browser.get(_URL)
    exportBtn = browser.find_element(By.ID, id)
    browser.get(_URL)
    exportBtn = browser.find_element(By.ID, id)
    time.sleep(2)
    exportBtn.click()
    print("Wait to download")
    time.sleep(5)

except Exception as e:
    print(e)
    pass
# Close the browser


browser.quit()
