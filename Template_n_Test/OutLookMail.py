import pandas as pd
import time
import os
import re
import sys
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# %%
_URL = "https://outlook.office365.com/mail/"

# %%
###############
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# Set format for displaying path
path = sys.argv[1] if len(sys.argv) > 1 else "."

# Initialize logging event handler
event_handler = LoggingEventHandler()

# Initialize Observer
observer = Observer()
observer.schedule(event_handler, path, recursive=True)
# Define ChromeDriver service
service = Service("/usr/local/bin/chromedriver")  # Adjust the path if needed
download_path = "/home/nhdminh/airflow/"

# Set Chrome options for headless mode and download directory
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.binary_location = "/usr/bin/google-chrome"  # Replace with the path to the Edge binary
# %%
# Set the download directory
chrome_options.add_experimental_option("prefs", {"download.default_directory": download_path})

# Start the WebDriver
browser = webdriver.Chrome(service=service, options=chrome_options)
# %%
browser.get(_URL)
time.sleep(5)

# %%
# Login
try:
    print("Input Username")
    Xpath = "/html/body/div/form[1]/div/div/div[2]/div[1]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/div/div/div[1]/div/div[1]/div/div[2]/div"
    lbPickAccount = browser.find_element(By.XPATH, Xpath)
    lbPickAccount.click()
    time.sleep(2)

    print("Input Password")
    inputPassword = browser.find_element(By.NAME, "passwd")
    inputPassword.send_keys("a3671c389")
    time.sleep(2)
    inputPassword.send_keys(Keys.ENTER)

    btnNext = browser.find_element(By.ID, "idSIButton9")
    btnNext.click()
    time.sleep(2)

except:
    try:
        print("Input Username NAME loginfmt")
        inputEmail = browser.find_element(By.NAME, "loginfmt")
        inputEmail.clear()
        inputEmail.send_keys("nhdminh@pvfcco.com.vn")
        btnNext = browser.find_element(By.ID, "idSIButton9")
        btnNext.click()
        time.sleep(2)

        print("Input Password NAME passwd")
        inputPassword = browser.find_element(By.NAME, "passwd")
        inputPassword.send_keys("a3671c389")
        time.sleep(2)
        inputPassword.send_keys(Keys.ENTER)

        print("Input Password NAME idSIButton9")
        btnNext = browser.find_element(By.ID, "idSIButton9")
        btnNext.click()
        time.sleep(2)
        print("LoginDone")
        print(browser.title)
    except:
        pass


# %%
def NewMail(mailTo, Subject, content):
    # new mail
    time.sleep(1)
    btnNewMail = browser.find_element(By.CSS_SELECTOR, ".ms-Button-icon.icon-218")
    btnNewMail.click()
    time.sleep(1)
    try:
        # ms-Button-label pivot-header-label label-182
        print("test test xxxxxx")

        test = browser.find_element(By.CSS_SELECTOR(".ms-Button-label.pivot-header-label")).find_element(
            By.XPATH, "//span[contains(.,'Insert')]"
        )
        print("test test", test.text)

    except Exception as e:
        print(e)
        pass
    # mail to
    try:
        print(mailTo)
        print(".T6Va1.VbY1P.EditorClass.aoWYQ")
        txtToEmail = browser.find_element(By.CSS_SELECTOR, ".T6Va1.VbY1P.EditorClass.aoWYQ")
        txtToEmail.clear()
        txtToEmail.send_keys(mailTo)
        time.sleep(1)
    except Exception as e:
        print(e)
        pass
    print("\Done Mail To\n")
    print("\Try Mail Subject\n")

    try:
        # TextField542
        print(".ms-TextField-field")
        txtSubject = browser.find_element(By.CSS_SELECTOR, ".ms-TextField-field")
        txtSubject.clear()
        mailSubject = Subject
        txtSubject.send_keys(mailSubject)
        time.sleep(1)
    except Exception as e:
        print(e)
        pass

    time.sleep(1)
    print("\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n")
    print("\Done Mail Subject\n")
    print("\Try Mail Content\n")
    try:
        print("elementToProof")
        txtContent = browser.find_element(By.XPATH, '//*[@id="editorParent_1"]/div/div[1]')
        mailContent = content
        txtContent.send_keys(mailContent)
        time.sleep(1)
    except Exception as e:
        print(e)
        pass
    time.sleep(1)
    print("\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n")

    try:
        print("ms-Button ms-Button--primary ms-Button--hasMenu root-422")
        btnSendMail = browser.find_element(By.CSS_SELECTOR, ".ms-Button.ms-Button--primary.ms-Button--hasMenu")
        btnSendMail.click()
        time.sleep(1)
    except Exception as e:
        print(e)
        pass


# %%
NewMail("nhdminh@pvfcco.com.vn", "testmail 02 from linux", "testContent")
