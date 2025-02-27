from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import time
import pyautogui
# Automatically download and use the latest Chrome WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)



def main(file_path):
    # Open the web application
    driver.get("http://10.199.104.153:3100/news/importers")  # Replace with the actual URL
    try:
        driver.find_element(By.XPATH,'//input[@placeholder="Username"]').send_keys('WDA')
        time.sleep(1)
        driver.find_element(By.XPATH,'//input[@placeholder="Password"]').send_keys('admin')
        time.sleep(1)
        driver.find_element(By.XPATH,'//button[@class="px-4 btn btn-primary"]').click()
        # Wait for the page to load
        time.sleep(2)  # Use WebDriverWait for better handling

        driver.get("http://10.199.104.153:3100/news/importers")  # Replace with the actual URL
    except Exception as e:
        print("not login")
    # Select the process from the dropdown
    time.sleep(2)  # Small delay to let the selection take effect
    # Find the dropdown element by its ID
    dropdown = Select(driver.find_element(By.ID, "select"))

    dropdown.select_by_index(2)
    time.sleep(1)


    # Locate the file input and upload a file
    file_input = driver.find_element(By.XPATH, '//input[@id="exampleFile"]/parent::div')  # Using XPath
    print(file_input.get_attribute('outerHTML'))

    file_input.click()
    time.sleep(3)
    pyautogui.typewrite(file_path,interval=0.1)  # Provide the absolute file path
    time.sleep(1)
    pyautogui.press("enter")  # Press Enter to select
    time.sleep(1)
    # Locate and click the upload button
    upload_button = driver.find_element(By.XPATH, '//button[@type="button" and contains(.,"Import")]')  # Update with the actual button ID/class
    upload_button.click()

    # Wait for upload to complete
    time.sleep(5)  # Adjust based on upload time

    # Close the browser
    driver.quit()
