import os

class Config:
    # Database Configuration
    SQLALCHEMY_DATABASE_URI = "oracle+oracledb://A161070:MostafaAsalA161070@10.199.104.126/analytics"
    #SQLALCHEMY_DATABASE_URI = "oracle+oracledb://USER:PASSWORD@10.199.104.126/analytics"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = 'your-secret-key'
    DB_URI = 'oracle+oracledb://A161070:MostafaAsalA161070@10.199.104.126/analytics'  # Replace with your actual connection string
    WORK_FOLDER = "Data"
    UPLOAD_FOLDER = "Upload"
    result_path = 'results'
    shared_path = r'\\10.199.104.125\DirectFeed\Mostafa\WDA_MONITOR'
    Date_to_expire = 30
    # Oracle Instant Client Path
    INSTANT_CLIENT = r"instantclient_23_6"  # Updated to absolute path

    # File Upload Folder
    UPLOAD_FOLDER = "instance/uploads"
    EMAIL_FROM = 'mostafa.asal@siliconexpert.com'
    EMAIL_TO =['nader_seliman@siliconexpert.com','mostafa.asal@siliconexpert.com','heba_moheb@siliconexpert.com','abdrabu_ahmed@siliconexpert.com']
    
    # SMTP Configuration
    SMTP_SERVER = 'smtp.office365.com'
    SMTP_PORT = 587
    SMTP_USE_TLS = True
    SMTP_USERNAME = 'mostafa.asal@siliconexpert.com'

    tenent_id='0beb0c35-9cbb-4feb-99e5-589e415c7944'
    client_id='7eadcef8-456d-4611-9480-4fff72b8b9e2'
    access='https://myaccess.microsoft.com/?tenantId=0beb0c35-9cbb-4feb-99e5-589e415c7944&upn=Mostafa.Asal%40siliconexpert.com'

    

    #automate status process
    daily_feed_url = r'https://d3ei71guzcqeu2.cloudfront.net/DailyExporterFeed/Priority%20System%20Results_Amazon%40{}.zip'
    daily_feed_path = r'\\10.199.104.106\update_sys'
    prty_feed_path = r'\\10.199.104.114\ShareName\UPDATE_SYS'
# Ensure the upload folder exists
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
