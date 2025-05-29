import shutil
import sys
import traceback
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import zipfile
import os
import json
from datetime import datetime, timedelta
import py7zr
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config import Config  # Now this should work

def Download_autoImported(from_date, to_date):
    options = webdriver.ChromeOptions()
    #options.add_argument('headless')
    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service,options=options)
    
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
    feed_hour = datetime.now().hour
    driver.find_element(By.XPATH,'//button[@type="button" and contains(.,"Export")]').click()
    # Close the browser
    time.sleep(5)  # Adjust based on upload time
    while len(driver.find_elements(By.XPATH,'//button[@type="button" and contains(.,"Loading")]'))>0:
        time.sleep(5)
    
    driver.find_element(By.XPATH,'//div[@class="form-check"]//input[@value="not_approved"]').click()
    time.sleep(1)  # Adjust based on upload time
    notApproved_hour = datetime.now().hour
    driver.find_element(By.XPATH,'//button[@type="button" and contains(.,"Export")]').click()
    # Close the browser
    time.sleep(5)  # Adjust based on upload time
    while len(driver.find_elements(By.XPATH,'//button[@type="button" and contains(.,"Loading")]'))>0:
        time.sleep(5)
    
    driver.quit()
    return feed_hour, notApproved_hour

daily_feed_url = Config.daily_feed_url
daily_feed_path = Config.daily_feed_path
# Download the daily feed for the specified date
def check_and_download_missing_dates(start_date, end_date, daily_feed_url, daily_feed_path):
    """
    Checks for missing date files in daily_feed_path and downloads them if they don't exist.
    :param start_date: Start date (YYYY-MM-DD)
    :param end_date: End date (YYYY-MM-DD)
    :param daily_feed_url: URL template for downloading the files
    :param daily_feed_path: Path where files should be stored
    """
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_date <= end_date:
        formatted_date = current_date.strftime("%d-%b-%Y")
        #Priority System Results_Amazon@08-Mar-2025
        file_name = f"Priority System Results_Amazon@{formatted_date}.zip"
        file_path = os.path.join(daily_feed_path, file_name)
        
        if not os.path.exists(file_path):
            print(f"Missing file: {file_name}. Downloading...")
            url = daily_feed_url.format(formatted_date)
            response = requests.get(url, stream=True)
            
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        file.write(chunk)
                print(f"Downloaded: {file_name}")
            else:
                print(f"Failed to download {file_name}. HTTP Status: {response.status_code}")
        else:
            print(f"File exists: {file_name}")
        
        current_date += timedelta(days=1)



def move_exported_files(export_path, work_folder, feed_hour, notApproved_hour):
    """
    Moves exported files matching the pattern to the work folder and handles not approved file specially.
    :param export_path: Path where the exported files are generated
    :param work_folder: Destination folder where files should be moved
    """
    current_date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
    am_pm = 'PM' if feed_hour//12==1 else 'AM'
    feed_hour = feed_hour%12 if feed_hour!=12 else 12
    feed_hour = f"{feed_hour:02d}_{ am_pm}"
    
    am_pm = 'PM' if notApproved_hour//12==1 else 'AM'
    notApproved_hour = notApproved_hour%12 if notApproved_hour!=12 else 12
    notApproved_hour= f"{notApproved_hour:02d}_{ am_pm}"
    
    patterns = [
        f"Export_NotFound_prysysFeed_{current_date}_{feed_hour}.txt",
        f"Export_prysysFeed_{current_date}_{feed_hour}.txt",
        f"Export_prysysFeed_notApproved_{current_date}_{notApproved_hour}.txt"
    ]
    
    for i, pattern in enumerate(patterns):
        file_path = os.path.join(export_path, pattern)
        if os.path.exists(file_path):
            if i == 2:  # Not approved file
                # Create 7z file with generic name
                seven_zip_path = os.path.join(work_folder, 'Latest_Not_Approved.7z')
                
                # Remove old 7z if exists
                if os.path.exists(seven_zip_path):
                    os.remove(seven_zip_path)
                    
                # Create new 7z with the file
                with py7zr.SevenZipFile(seven_zip_path, 'w') as archive:
                    archive.write(file_path, os.path.basename(file_path))
                
                # Remove original file after compression
                #os.remove(file_path)
                print(f"Compressed and moved not approved file to: {seven_zip_path}")
            else:
                destination_path = os.path.join(work_folder, pattern)
                print(file_path,destination_path)
                shutil.copy(file_path, destination_path)
                print(f"Moved: {pattern} to {work_folder}")
        else:
            print(f"File not found: {file_path}")

    return patterns

def extract_part_details(feed_path, work_path,last_done_date,last_date):
    """
    Extracts PartDetailsReport from zip files in feed_path and saves them to work_path.
    :param feed_path: Path where zip files are stored
    :param work_path: Path where extracted files should be saved
    """
    
    current_date = datetime.strptime(last_done_date, "%Y-%m-%d")
    end_date = datetime.strptime(last_date, "%Y-%m-%d")
    
    while current_date <= end_date:
        formatted_date = current_date.strftime("%d-%b-%Y")
    
        zip_file_name = f"Priority System Results_Amazon@{formatted_date}.zip"
        zip_file_path = os.path.join(feed_path, zip_file_name)
        part_details_file = f"PartDetailsReport@{formatted_date}.txt"
        
        if os.path.exists(zip_file_path):
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                if part_details_file in zip_ref.namelist():
                    zip_ref.extract(part_details_file, work_path)
                    print(f"Extracted: {part_details_file} to {work_path}")
                else:
                    print(f"File not found in zip: {part_details_file}")
        else:
            print(f"Zip file not found: {zip_file_path}")
        current_date += timedelta(days=1)






def automate_process(last_done_date=None,last_date=None):
    # Load the last done date from the JSON file
    with open('automate_records/process_status.json', 'r') as f:
        data = json.load(f)
    if not last_done_date:
        last_done_date = data.get("last_done_date")

    # If the last done date is empty, set it to today's date
    if not last_date:
        last_date = (datetime.now() - timedelta(days=1)).date().isoformat()
    # Call the Download_autoImported function
    if not last_done_date:
        last_done_date = last_date
    
    feed_hour,notApproved_hour = Download_autoImported(last_done_date,last_date)
    print(feed_hour,notApproved_hour)
    patterns = move_exported_files(Config.prty_feed_path,'automate_records',feed_hour=feed_hour,notApproved_hour=notApproved_hour)
    print(patterns)
    check_and_download_missing_dates(last_done_date, last_date,daily_feed_url,daily_feed_path)
    extract_part_details(daily_feed_path ,'automate_records' ,last_done_date,last_date)
    # Update the JSON file with the new date
    result = calculate_import_status(f'automate_records\\{patterns[1]}', f'automate_records\\{patterns[0]}',r'automate_records' )
    with open('automate_records/process_status.json', 'w') as f:
            json.dump({"last_done_date": last_date}, f)
    return result


def calculate_import_status(found_feed_path, not_found_feed_path, part_details_folder):
    print("reading found feed")
    found_feed = pd.read_csv(found_feed_path, delimiter='\t',encoding='mbcs',dtype=str)
    found_feed.dropna(subset=['check_date'])
    print("reading not found feed")
    
    not_found_feed = pd.read_csv(not_found_feed_path, delimiter='\t',encoding='mbcs',dtype=str)
    not_found_feed.dropna(subset=['check_date'])
    check_dates = found_feed['check_date'].unique()
    check_dates = pd.to_datetime(check_dates)
    print("read found and not found")
    results = []
    print(check_dates)
    i=0
    for file_name in os.listdir(part_details_folder):
        print(f"checking {file_name}")
        try:
            if file_name.endswith(".txt") and file_name.startswith("PartDetails") and pd.to_datetime(file_name.split('@')[1].split('.')[0]) in check_dates:
                print(f"reading {file_name}")
                part_details_path = os.path.join(part_details_folder, file_name)
                part_details = pd.read_csv(part_details_path, delimiter='\t', encoding='mbcs', dtype=str, on_bad_lines='skip')
                part_details = part_details.dropna(subset=['Priority'])
                part_details.columns = ['mpn', 'man', 'module','Priority','Online Link','Status','Found Part','Sys Date']
                part_details['status'] = part_details['Status'].apply(lambda x: 'found' if str(x).lower() == 'found' else 'not found')
                part_details = part_details.sort_values(by=['mpn', 'man', 'module', 'status'], ascending=[True, True, True, True])
                part_details = part_details.drop_duplicates(subset=['mpn', 'man', 'module'], keep='first')
                print("droped duplicates")
                
                date = pd.to_datetime(part_details['Sys Date'].dropna().unique()[0])
                print(date)
                
                # Process found parts
                found_feed_date = found_feed[pd.to_datetime(found_feed['check_date'])==date]
                found_feed_date = found_feed_date.drop_duplicates(subset=['mpn', 'man', 'module'], keep='first')
                found_feed_date = found_feed_date.dropna(subset=['check_date'])
                found_merge = found_feed_date.merge(part_details[part_details['status'] == 'found'], 
                                                on=['mpn', 'man', 'module'], how='outer', indicator=True)
                
                # Process not found parts
                notfound_feed_date = not_found_feed[pd.to_datetime(not_found_feed['check_date'])==date]
                notfound_feed_date = notfound_feed_date.drop_duplicates(subset=['mpn', 'man', 'module'], keep='first')
                notfound_feed_date = notfound_feed_date.dropna(subset=['check_date'])
                not_found_merge = notfound_feed_date.merge(part_details[part_details['status'] == 'not found'], 
                                                        on=['mpn', 'man', 'module'], how='outer', indicator=True)
                
                # Process auto import status
                found_merge['received'] = found_merge['_merge'].map({'both': 'received', 'right_only': 'not received'})
                not_found_merge['received'] = not_found_merge['_merge'].map({'both': 'received', 'right_only': 'not received'})
                
                found_merge['auto_imp_status'] = found_merge.apply(
                    lambda row: '' if pd.isna(row['auto_imp_status']) or row['received']== 'not received' 
                    else 'In Progress' if row['auto_imp_status'] == 'In Progress' 
                    else 'Not Imported' if 'Not Imported' in str(row['auto_imp_status']) 
                    else 'Imported', axis=1
                )
                
                not_found_merge['auto_imp_status'] = not_found_merge.apply(
                    lambda row: '' if pd.isna(row['auto_imp_status']) or row['received']== 'not received' 
                    else 'In Progress' if row['auto_imp_status'] == 'In Progress' 
                    else 'Imported', axis=1
                )
                
                # Create reports for found parts
                found_report = found_merge[['mpn', 'module', 'Status', 'auto_imp_status', 'received']].copy()
                found_report['auto_imp_status'] = found_report['auto_imp_status'].apply(lambda x: 'NotReceived' if x == '' else x)
                found_report['Category'] = found_report['auto_imp_status'] + ' found'
                found_report.columns = ['Part Number', 'Module Name', 'Status', 'Auto Import Status', 'Received', 'Category']
                
                # Create reports for not found parts
                notfound_report = not_found_merge[['mpn', 'module', 'Status', 'auto_imp_status', 'received']].copy()
                notfound_report['auto_imp_status'] = notfound_report['auto_imp_status'].apply(lambda x: 'NotReceived' if x == '' else x)
                notfound_report['Category'] = notfound_report['auto_imp_status'] + ' NotFound'
                notfound_report.columns = ['Part Number', 'Module Name', 'Status', 'Auto Import Status', 'Received', 'Category']
                
                # Combine all reports
                daily_report = pd.concat([found_report, notfound_report])
                daily_report['date'] = date
                
                # Create Imported_NotImported_parts report
                imported_not_imported = daily_report[
                    (daily_report['Auto Import Status'].isin(['Imported', 'Not Imported'])) & 
                    (daily_report['Received'] == 'received')
                ].copy()
                
                # Create Missed_Inprogress_parts report
                missed_inprogress = daily_report[
                    ((daily_report['Auto Import Status'] == 'In Progress') & (daily_report['Received'] == 'received')) |
                    (daily_report['Received'] == 'not received')
                ].copy()
                
                # Remove unnecessary columns and export
                columns_to_export = ['Part Number', 'Module Name', 'Status', 'Auto Import Status', 'Category', 'date']
                
                # Export Imported_NotImported_parts
                imported_not_imported = imported_not_imported[columns_to_export]
                report_filename = f'Imported_NotImported_parts_{date.strftime("%Y-%m-%d")}.csv'
                report_path = os.path.join(part_details_folder, report_filename)
                imported_not_imported.to_csv(report_path, index=False)
                print(f"Exported imported/not imported report: {report_filename}")
                
                # Export Missed_Inprogress_parts
                missed_inprogress = missed_inprogress[columns_to_export]
                report_filename = f'Missed_Inprogress_parts_{date.strftime("%Y-%m-%d")}.csv'
                report_path = os.path.join(part_details_folder, report_filename)
                missed_inprogress.to_csv(report_path, index=False)
                print(f"Exported missed/in progress report: {report_filename}")
                
                # Continue with existing statistics calculation
                total_parts = len(part_details[part_details['status'] == 'found'])
                received_count = (found_merge['received'] == 'received' ).sum() 
                not_received_count = (found_merge['received'] == 'not received').sum() 
                imported_count = (found_merge['auto_imp_status'] == 'Imported').sum()
                not_imported_count = (found_merge['auto_imp_status'] == 'Not Imported').sum()
                in_progress_count = (found_merge['auto_imp_status'] == 'In Progress').sum()
                
                results.append({
                    'date': date,
                    'table': 'found',
                    'total_parts': total_parts,
                    'received_count': received_count,
                    'not_received_count': not_received_count,
                    'imported_count': imported_count,
                    'not_imported_count': not_imported_count,
                    'in_progress_count': in_progress_count
                })

                total_parts = len(part_details[part_details['status'] == 'not found'])
                received_count = (not_found_merge['received'] == 'received').sum() 
                not_received_count = (not_found_merge['received'] == 'not received').sum() 
                imported_count = (not_found_merge['auto_imp_status'] == 'Imported').sum()
                not_imported_count = (not_found_merge['auto_imp_status'] == 'Not Imported').sum()
                in_progress_count = (not_found_merge['auto_imp_status'] == 'In Progress').sum()
                
                results.append({
                    'date': date,
                    'table': 'notfound',
                    'total_parts': total_parts,
                    'received_count': received_count,
                    'not_received_count': not_received_count,
                    'imported_count': imported_count,
                    'not_imported_count': not_imported_count,
                    'in_progress_count': in_progress_count
                })
        except Exception as e:
            print(traceback.format_exc())
            print(f"Error processing {file_name}: {str(e)}")
            continue
    try:
        results_df = pd.DataFrame(results)
        results_df = results_df[['date','table','total_parts','received_count','imported_count','not_imported_count','in_progress_count','not_received_count']]
        
        #results_df.columns = ['date','table','total_parts','received','imported','not_imported','in_progress','not_received']
        results_df = results_df[results_df['not_received_count'] != results_df['total_parts']]
    except Exception as e:
        print(traceback.format_exc())
        print(f"Error processing results: {str(e)}")
    print(results_df)
    return results_df

if __name__=="__main__":
    calculate_import_status(r'automate_records\Export_prysysFeed_23-Mar-2025_10_AM.txt',r'automate_records\Export_NotFound_prysysFeed_23-Mar-2025_10_AM.txt','automate_records')
    #automate_process()
