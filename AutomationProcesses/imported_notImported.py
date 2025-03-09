from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
# Automatically download and use the latest Chrome WebDriver


def Download_autoImported(from_date, to_date):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    # Open the web application
    driver.get("http://10.199.104.153:3100/WDA/ReportsAutoImport")  # Replace with the actual URL
    try:
        driver.find_element(By.XPATH,'//input[@placeholder="Username"]').send_keys('WDA')
        time.sleep(1)
        driver.find_element(By.XPATH,'//input[@placeholder="Password"]').send_keys('admin')
        time.sleep(1)
        driver.find_element(By.XPATH,'//button[@class="px-4 btn btn-primary"]').click()
        # Wait for the page to load
        time.sleep(2)  # Use WebDriverWait for better handling

        driver.get("http://10.199.104.153:3100/WDA/ReportsAutoImport")  # Replace with the actual URL
    except Exception as e:
        print("not login")
    # Select the process from the dropdown
    time.sleep(2)  # Small delay to let the selection take effect
    # Find the dropdown element by its ID
    driver.find_element(By.XPATH,'(//input[@class="rdp-form-control  form-control"])[1]').send_keys(from_date)
    driver.find_element(By.XPATH,'(//input[@class="rdp-form-control  form-control"])[2]').send_keys(to_date)
    driver.find_element(By.XPATH,'//div[@class="form-check"]//input[@value="found_not_found"]').click()
    # Wait for upload to complete
    time.sleep(1)  # Adjust based on upload time
    driver.find_element(By.XPATH,'//button[@type="button" and contains(.,"Export")]').click()
    # Close the browser
    time.sleep(5)  # Adjust based on upload time
    while len(driver.find_elements(By.XPATH,'//button[@type="button" and contains(.,"Loading")]'))>0:
        time.sleep(5)
    
    driver.find_element(By.XPATH,'//div[@class="form-check"]//input[@value="not_approved"]').click()
    time.sleep(1)  # Adjust based on upload time
    driver.find_element(By.XPATH,'//button[@type="button" and contains(.,"Export")]').click()
    # Close the browser
    time.sleep(5)  # Adjust based on upload time
    while len(driver.find_elements(By.XPATH,'//button[@type="button" and contains(.,"Loading")]'))>0:
        time.sleep(5)
    
    driver.quit()


Download_autoImported('2025-03-03', '2025-03-03')
import zipfile
import os

def extract_file_from_zip(zip_file_path, file_name):
    """Extract a file from a zip file path"""
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extract(file_name, Config.WORK_FOLDER)
    return os.path.join(Config.WORK_FOLDER, file_name)
