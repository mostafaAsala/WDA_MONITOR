import pandas as pd
import pyperclip
from selenium import webdriver
from selenium.webdriver.edge.options import Options
import time

import pyautogui
pyautogui.FAILSAFE = False

import logging

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler and a stream handler
file_handler = logging.FileHandler('download_matrix.log')
stream_handler = logging.StreamHandler()

# Create a formatter and set it for the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def download_matrix_toFile():
    try:
        # Set up Edge options (optional)
        edge_options = Options()
        edge_options.add_experimental_option("prefs", {
            "download.default_directory": r"D:\Mostafa\WDA_MONITOR\AutomationProcesses",  # Change path
            "download.prompt_for_download": False,  # Disable 'Save As' popup
            "download.directory_upgrade": True
        })
        edge_options.add_argument("--start-maximized")  # Start maximized

        # Initialize Edge WebDriver (Selenium Manager handles driver installation)
        driver = webdriver.Edge(options=edge_options)

        # Open a website
        driver.get("https://arrowelectronics-my.sharepoint.com/:x:/g/personal/heba_moheb_siliconexpert_com/ERC1R--yjsFFsvIPH-o5m1oBvvVoWYwLXMUgO--6AVvPzQ?e=30KIcw&clickparams=eyJBcHBOYW1lIjoiVGVhbXMtRGVza3RvcCIsIkFwcFZlcnNpb24iOiIyNy8yMzExMDIyNDcwNSIsIkhhc0ZlZGVyYXRlZFVzZXIiOmZhbHNlfQ%3D%3D")

        logger.info("Website opened successfully")

        time.sleep(5)

        # Select all data (Ctrl + A)
        pyautogui.hotkey("ctrl","shift", "l")
        time.sleep(1)
        # Select all data (Ctrl + A)
        pyautogui.hotkey("ctrl","shift", "l")
        time.sleep(1)

        # Select all data (Ctrl + A)
        pyautogui.hotkey("ctrl", "a")
        time.sleep(1)
        # Select all data (Ctrl + A)
        pyautogui.hotkey("ctrl", "a")
        time.sleep(1)

        # Copy data (Ctrl + C)
        pyautogui.hotkey("ctrl", "c")
        time.sleep(2)  # Wait for copying to complete

        logger.info("Data copied successfully")

        # Get copied data from clipboard
        data = pyperclip.paste()
        rows = [row.split("\t") for row in data.split("\r\n")]  # Convert to list
        header = rows[0]
        rows = rows[1:]
        # Save as CSV
        df = pd.DataFrame(rows,columns=header)
        print(df)
        print("Download started...")
        if not df.empty and len(df.columns) > 3:
            df = df.dropna(axis=1, how='all')
            df.to_csv(r'Static Data\matrix.csv')

        logger.info("Data saved to CSV file successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.quit()
        logger.info("Driver quit successfully")

if __name__ == "__main__":
    download_matrix_toFile()