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

# Set the download directory
chrome_options.add_experimental_option("prefs", {"download.default_directory": download_path})

# Start the WebDriver
browser = webdriver.Chrome(service=service, options=chrome_options)
