from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import time
import pyautogui
from config import Config
import os
# Automatically download and use the latest Chrome WebDriver

def upload_file_to_amazon(df,filename):
    print(df.columns)
    filename = filename.split('.')[0]
    try:
        df.columns = ['part', 'man', 'module'] 
    except Exception as e:
        print(e)
    df = df[['part','module']]
    df.columns = ['Part','Module']
    df['Prty'] = df['Part'].str.contains('http').map({True: 'P0', False: 'P1'})
    current_folder = os.getcwd()
    file_path = os.path.join(current_folder, Config.WORK_FOLDER,filename+'.xlsx')
    print("saving excel file")
    df.to_excel(file_path, index=False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    # Open the web application
    driver.get("http://10.199.104.153:3100/news/importers")  # Replace with the actual URL
    try:
        print("opening amazon upload page")
        driver.find_element(By.XPATH,'//input[@placeholder="Username"]').send_keys('WDA')
        time.sleep(1)
        driver.find_element(By.XPATH,'//input[@placeholder="Password"]').send_keys('admin')
        time.sleep(1)
        driver.find_element(By.XPATH,'//button[@class="px-4 btn btn-primary"]').click()
        # Wait for the page to load
        time.sleep(2)  # Use WebDriverWait for better handling

        driver.get("http://10.199.104.153:3100/news/importers")  # Replace with the actual URL
    except Exception as e:
        print("not login", e)
    try:    
        # Select the process from the dropdown
        time.sleep(2)  # Small delay to let the selection take effect
        # Find the dropdown element by its ID
        dropdown = Select(driver.find_element(By.ID, "select"))

        dropdown.select_by_index(2)
        time.sleep(1)

        print("uploading file")
        # Locate the file input and upload a file
        file_input = driver.find_element(By.XPATH, '//input[@id="exampleFile"]')  # Using XPath
        print(file_input.get_attribute('outerHTML'))
        print("open2")
        #file_input.click()
        time.sleep(3)
        file_input.send_keys(file_path)
        # Locate and click the upload button
        
        time.sleep(3)
        upload_button = driver.find_element(By.XPATH, '//button[@type="button" and contains(.,"Import")]')  # Update with the actual button ID/class
        upload_button.click()

        # Wait for upload to complete
        time.sleep(50)  # Adjust based on upload time
    except Exception as e:
        print(e)
    # Close the browser
    driver.quit()
