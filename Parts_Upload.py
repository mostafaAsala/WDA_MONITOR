import os
import csv
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Date, Integer, MetaData, select, insert ,delete
import logging
from config import Config
from logging import Formatter
import chardet
from datetime import datetime
import oracledb

# Import enhanced CSV logging system from check_status
try:
    from check_status import setup_csv_logger, process_loggers, log_process_start, log_process_end, log_step, log_error_with_context
    ENHANCED_LOGGING_AVAILABLE = True
except ImportError:
    ENHANCED_LOGGING_AVAILABLE = False
# Database configuration: Replace "your_database_url_here" with your actual database connection string
DATABASE_URL = Config.DB_URI


# Setup logging for each function
def setup_logger(name, log_file):
    """
    Sets up a logger with a specific name and log file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create file handler
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    
    # Add date and time format to logs
    formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger



# Update Oracle Client initialization
try:
    instant_client_path = os.path.abspath(Config.INSTANT_CLIENT)
    os.environ["PATH"] = instant_client_path + os.pathsep + os.environ["PATH"]
    os.environ["ORACLE_HOME"] = instant_client_path
    oracledb.init_oracle_client(lib_dir=instant_client_path)
except Exception as e:
    print(f"Error initializing Oracle Client: {e}")
    raise

# Setup logging for each function
logging.basicConfig(level=logging.INFO)

# Loggers for individual functionalities - use enhanced CSV logging if available
if ENHANCED_LOGGING_AVAILABLE:
    upload_logger = setup_csv_logger("file_upload", logging.INFO)
    delete_logger = setup_csv_logger("file_delete", logging.INFO)
    file_scan_logger = setup_csv_logger("file_scan", logging.INFO)
else:
    # Fallback to basic logging
    upload_logger = setup_logger("upload_parts", "upload_parts.log")
    delete_logger = setup_logger("delete_file", "delete_file.log")
    file_scan_logger = setup_logger("scan_files", "scan_files.log")


# Function to set up the database engine and session
def get_engine_and_session(database_url,):
    """
    Create the SQLAlchemy engine to connect to the database.
    """
    return create_engine(database_url,echo = False)

# Define the tables in the database
def define_tables(metadata):
    """
    Define schema for 'parts' and 'uploaded_files' tables.
    """
    parts_table = Table(
        'parts', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('part', String),
        Column('man', String),
        Column('module', String),
        Column('file_id', Integer),
        Column('STATUS', String, nullable=True),
        Column('CM_DATE', String, nullable=True),
        Column('LAST_RUN_DATE', String, nullable=True),
        Column('TABLE_NAME', String, nullable=True),
        Column('Features', String, nullable=True),
        Column('monitor features', String, nullable=True)
    )

    uploaded_files_table = Table(
        'uploaded_files', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('file_name', String(255), unique=True),  # Specify length for String
        Column('UPLOAD_DATE', Date),
        Column('LAST_CHECK_DATE', Date),  
    )

    return parts_table, uploaded_files_table

# Function to retrieve all files from a folder
def get_files_from_folder(folder_path):
    """
    Get a list of all files in the specified folder.
    """
    try:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        file_scan_logger.info(f"Scanned folder '{folder_path}', found files: {files}")
        return files
    except Exception as e:
        file_scan_logger.error(f"Error scanning folder '{folder_path}': {e}")
        raise


# Function to extract parts data from a CSV file
def extract_parts_from_file(file_path):
    """
    Parse a CSV file and extract part details using pandas.
    """
    required_columns = [ 'STATUS', 'CM_DATE', 'LAST_RUN_DATE', 'TABLE_NAME', 'Features', 'monitor features']
    parts = []

    try:
        

        with open(file_path, "rb") as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            print(result)  # {'encoding': 'utf-8', 'confidence': 0.99, 'language': ''}
        # Load CSV into a DataFrame
        if result['encoding'] is None:
            result['encoding'] = 'utf-8'
        df = pd.read_csv(file_path,delimiter='\t',encoding=result['encoding'])
        for column in required_columns:
            if column not in df.columns:
                df[column]=None

        print(df.columns)
        df.columns = ['part', 'man', 'module'] + required_columns



        # Extract relevant columns and convert to a list of dictionaries
        parts = df.to_dict(orient='records')

        upload_logger.info(f"Extracted {len(parts)} parts from file '{file_path}' with ensured columns: {required_columns}.")
        return parts
    except Exception as e:
        upload_logger.error(f"Error extracting parts from file '{file_path}': {e}")
        raise

# Function to check if a file is already uploaded
def is_file_uploaded(session, uploaded_files_table, file_name):
    """
    Check if a file is already uploaded.
    """
    if file_name =='AmazonUpload.xlsx':
        return False
    try:
        query = select(uploaded_files_table).where(uploaded_files_table.c.file_name == file_name)
        result = session.execute(query).fetchone()
        upload_logger.info(f"Checked if file '{file_name}' is uploaded: {'Yes' if result else 'No'}.")
        return result is not None
    except Exception as e:
        upload_logger.error(f"Error checking file upload status for '{file_name}': {e}")
        raise

# Function to upload parts to the database
def upload_parts(session, parts_table, uploaded_files_table, file_path, parts):
    """
    Upload parts to the database and log the file.
    """
    file_name = os.path.basename(file_path)
    try:
        # Insert file into uploaded_files table
        result = session.execute(insert(uploaded_files_table).values(file_name=file_name,UPLOAD_DATE=datetime.now(),LAST_CHECK_DATE=None))
        file_id = result.inserted_primary_key[0]

        # Insert parts into parts table
        for part in parts:
            part['file_id'] = file_id
            session.execute(insert(parts_table).values(**part))
        
        session.commit()
        upload_logger.info(f"Uploaded file '{file_name}' with {len(parts)} parts to database.")
    except Exception as e:
        upload_logger.error(f"Error uploading file '{file_name}' to database: {e}")
        session.rollback()
        raise

# Function to delete a file and its associated data
def delete_file(session, parts_table, uploaded_files_table, folder_path, file_name):
    """
    Delete the file and all its associated data from the database and folder.
    """
    try:
        # Fetch file ID
        query = select(uploaded_files_table).where(uploaded_files_table.c.file_name == file_name)
        file_record = session.execute(query).fetchone()

        if not file_record:
            delete_logger.warning(f"File '{file_name}' not found in database. Skipping deletion.")
        else:
            #print("---------------------------------------------")
            #print(file_record)
            
            #print("---------------------------------------------")
            file_id = file_record[0]

            # Delete parts associated with file
            session.execute(delete(parts_table).where(parts_table.c.file_id == file_id))

            # Delete file record
            session.execute(delete(uploaded_files_table).where(uploaded_files_table.c.id == file_id))
            session.commit()

        # Delete the physical file
        file_path = os.path.join(folder_path, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            delete_logger.info(f"Deleted file '{file_name}' and its content from the database and folder.")
        else:
            delete_logger.warning(f"File '{file_name}' not found in folder. Database entry removed.")
    except Exception as e:
        delete_logger.error(f"Error deleting file '{file_name}': {e}")
        session.rollback()
        raise

# Main function to upload parts from all files in a folder
def main_upload_parts(folder_path):
    """
    Process all files in the folder and upload new parts to the database.
    """
    if ENHANCED_LOGGING_AVAILABLE:
        log_process_start(upload_logger, "main_upload_parts", folder_path=folder_path)

    try:
        engine = get_engine_and_session(DATABASE_URL)
        metadata = MetaData()

        parts_table, uploaded_files_table = define_tables(metadata)
        metadata.create_all(engine)

        if ENHANCED_LOGGING_AVAILABLE:
            log_step(upload_logger, "Scan folder for files to upload", subprocess_name="folder_scan")

        with engine.connect() as session:
            files = get_files_from_folder(folder_path)

            if ENHANCED_LOGGING_AVAILABLE:
                log_step(upload_logger, f"Process {len(files)} files for upload", subprocess_name="file_processing_init")

            files_processed = 0
            files_skipped = 0

            for file_index, file_path in enumerate(files, 1):
                file_name = os.path.basename(file_path)

                if is_file_uploaded(session, uploaded_files_table, file_name):
                    if ENHANCED_LOGGING_AVAILABLE:
                        record = upload_logger.makeRecord(
                            upload_logger.name, logging.INFO, '', 0,
                            f"File '{file_name}' already uploaded. Skipping.",
                            (), None, func='file_check'
                        )
                        record.subprocess = 'file_check'
                        record.details = f'file_status=already_uploaded'
                        upload_logger.handle(record)
                    else:
                        upload_logger.info(f"File '{file_name}' already uploaded. Skipping.")
                    files_skipped += 1
                    continue

                if ENHANCED_LOGGING_AVAILABLE:
                    record = upload_logger.makeRecord(
                        upload_logger.name, logging.INFO, '', 0,
                        f"Processing file {file_index}/{len(files)}: {file_name}",
                        (), None, func='file_processing'
                    )
                    record.subprocess = 'file_processing'
                    record.details = f'file_index={file_index},total_files={len(files)}'
                    upload_logger.handle(record)
                else:
                    upload_logger.info(f"Processing file {file_index}/{len(files)}: {file_name}")

                parts = extract_parts_from_file(file_path)
                upload_parts(session, parts_table, uploaded_files_table, file_path, parts)
                files_processed += 1

        if ENHANCED_LOGGING_AVAILABLE:
            log_process_end(upload_logger, "main_upload_parts", success=True,
                           files_processed=files_processed, files_skipped=files_skipped)

    except Exception as e:
        if ENHANCED_LOGGING_AVAILABLE:
            log_process_end(upload_logger, "main_upload_parts", success=False, error=str(e))
            log_error_with_context(upload_logger, e, f"Upload parts from folder: {folder_path}", subprocess_name="error_handling")
        else:
            upload_logger.error(f"Error in main_upload_parts: {str(e)}")
        raise

# Main function to delete a specific file and its content
def main_delete_file(folder_path, file_name):
    """
    Deletes a specified file and its associated database entries.
    """
    print("deleting : ",file_name)
    engine = get_engine_and_session(DATABASE_URL)
    metadata = MetaData()
    parts_table, uploaded_files_table = define_tables(metadata)

    with engine.connect() as session:
        delete_file(session, parts_table, uploaded_files_table, folder_path, file_name)

# Entry point
if __name__ == "__main__":
    folder_path = "Data"  # Replace with the path to your folder
    action = 'upload' #input("Enter action (upload/delete): ").strip().lower()
    
    if action == "upload":
        main_upload_parts(folder_path)
    elif action == "delete":
        file_name = 'Test_parts_2.txt' #input("Enter the name of the file to delete: ").strip()
        main_delete_file(folder_path, file_name)
    else:
        print("Invalid action. Use 'upload' or 'delete'.")