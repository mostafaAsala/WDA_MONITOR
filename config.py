import os

class Config:
    # Database Configuration
    SQLALCHEMY_DATABASE_URI = "oracle+oracledb://A161070:MostafaAsalA161070@10.199.104.126/analytics"
    #SQLALCHEMY_DATABASE_URI = "oracle+oracledb://USER:PASSWORD@10.199.104.126/analytics"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = 'your-secret-key'
    DB_URI = 'oracle+oracledb://A161070:MostafaAsalA161070@10.199.104.126/analytics'  # Replace with your actual connection string
    
    WORK_FOLDER = "Data"
    result_path = 'results'
    shared_path = r'\\10.199.104.125\DirectFeed\Mostafa\WDA_MONITOR'
    Date_to_expire = 30
    # Oracle Instant Client Path
    INSTANT_CLIENT = r"instantclient_23_6"  # Updated to absolute path

    # File Upload Folder
    UPLOAD_FOLDER = "instance/uploads"

# Ensure the upload folder exists
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
