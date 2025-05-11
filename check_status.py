import os
import logging
from datetime import datetime
from typing import Text
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import oracledb
from config import Config,SQLQueries
import traceback
from sqlalchemy.pool import QueuePool
import tempfile
import shutil
import filelock

import win32com.client as win32
from tabulate import tabulate

import winreg

def set_redemption_registry():
    """Ensure Redemption license agreement is suppressed via registry."""
    try:
        # Open or create the Redemption registry key
        key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"instance\Redemption")
        # Set RDOAcceptMessages to 1 to suppress the license dialog
        winreg.SetValueEx(key, "RDOAcceptMessages", 0, winreg.REG_DWORD, 1)
        winreg.CloseKey(key)
        print("Registry key set successfully. Redemption license dialog suppressed.")
    except Exception as e:
        print(f"Error setting Redemption registry key: {e}")

# Call this function before initializing Redemption
#set_redemption_registry()


#session = win32.Dispatch("Redemption.RDOSession")
#session.RDOAcceptMessages = True
    


# Setup logging
def setup_logger():
    logger = logging.getLogger('status_logger')
    logger.setLevel(logging.INFO)
    
    # Create logs directory if it doesn't exist
    log_filename = os.path.join(Config.shared_path, f'status_log_{datetime.now().strftime("%Y-%m-%d")}.log')
    
    # Create file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(file_handler)
    
    return logger

# Initialize logger
status_logger = setup_logger()

# Create a file lock for results.csv
results_lock = filelock.FileLock(os.path.join(Config.result_path, "results.csv.lock"))
# Update Oracle Client initialization
try:
    instant_client_path = os.path.abspath(Config.INSTANT_CLIENT)
    os.environ["PATH"] = instant_client_path + os.pathsep + os.environ["PATH"]
    os.environ["ORACLE_HOME"] = instant_client_path
    oracledb.init_oracle_client(lib_dir=instant_client_path)
except Exception as e:
    print(f"Error initializing Oracle Client: {e}")
    raise

# Database configuration with connection pooling
def create_db_engine():
    return create_engine(
        Config.DB_URI,
        poolclass=QueuePool,
        pool_size=20,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )


def get_files():
    engine = create_db_engine()
    try:
        get_files_string =text( "select * from uploaded_files")
        with engine.begin() as connection:
            try:
                print("in progress...")
                result = connection.execute(get_files_string)
                data = result.fetchall()  # Fetch all rows
                columns = result.keys()   # Get column names
                df = pd.DataFrame(data, columns=columns)
                return df
            except Exception as e:
                print(e)
                return None
        
    except Exception as e:
        print(e)
    
    pass

def safe_write_csv(df, filepath):
    """Thread-safe CSV writing with direct file access"""
    try:
        # Use file lock when writing
        with results_lock:
            # Write directly to the target file
            df.to_csv(filepath, index=False, mode='w')
            
            # Ensure file is fully written and synced
            with open(filepath, 'a') as f:
                f.flush()
                os.fsync(f.fileno())
                
    except Exception as e:
        print(f"Error in safe_write_csv: {str(e)}")
        raise




def get_status_statistics(df):
    """Get status statistics from DataFrame"""

    status_stats = df['status'].value_counts().to_dict()
    return status_stats

def check_valid_file(files_list=None, ignore_date=True):
    """Check if parts in files are valid"""
    files_string , daily_export = Get_status(files_list , ignore_date)
    df = fetch_results_from_database(files_string , daily_export)
    

def Get_status(files_list=None, ignore_date=True, daily_export=False):
    engine = create_db_engine()
    try:
        status_logger.info("Starting Get_status function")
        if files_list is None:
            files_list = os.listdir(Config.WORK_FOLDER)
            status_logger.info(f"No files provided, using all files in work folder: {len(files_list)} files")
        else:
            status_logger.info(f"Processing specific files: {files_list}")
            
        files_string = str(tuple(files_list))
        if len(files_list)==1:
            files_string = files_string[0:-2]+')'
        
        status_logger.info(f"Ignore date: {ignore_date}, Daily export: {daily_export}")
        
        if ignore_date:
            check_date = ""
            status_logger.info("Date check disabled")
        else:
            check_date = "and NVL(TO_CHAR(last_check_date,'DD-MON-YYYY'), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) < TO_CHAR(sysdate,'DD-MON-YYYY')"
            check_date = SQLQueries.q_add_check_date
            status_logger.info("Using date check condition")

        if daily_export:
            date_condition = "and NVL(stop_monitor_date, TO_DATE('01-JAN-2999', 'DD-MON-YYYY')) > sysdate"
            date_condition = SQLQueries.q_stop_monitor
            status_logger.info("Using daily export condition")
        else:
            date_condition = ""
            date_condition = "and NVL(stop_monitor_date, TO_DATE('01-JAN-2999', 'DD-MON-YYYY')) > sysdate"
            date_condition = SQLQueries.q_stop_monitor
        status_logger.info(f"Calculating Status for files: {files_string}")
        
        query = text(f"""

            MERGE INTO parts p
            USING (
                WITH files_id as (select id from uploaded_files  where file_name  in {str(files_string)} {check_date} {date_condition} )
                ,parts_id AS (
                    SELECT 
                        p.ID AS PART_ID, 
                        p.PART, 
                        p.MAN, 
                        p.MODULE, 
                        p.FILE_ID, 
                        CASE 
                            WHEN m.module_name IS NULL THEN 'not_valid' 
                            ELSE p.STATUS 
                        END AS STATUS,
                        p.CM_DATE, 
                        p.LAST_RUN_DATE, 
                        p.TABLE_NAME, 
                        p."monitor features", 
                        m.MAN_ID, 
                        m.MODULE_ID
                    FROM parts p
                    LEFT JOIN updatesys.tbl_man_modules@new3_n m 
                    ON p.module = m.module_name
                    where p.file_id  IN (
                        SELECT id FROM files_id
                    )
                ),
                found_notfound_table AS (
                    SELECT 
                        pm.PART_ID,
                        pm.PART,
                        pm.MAN,
                        pm.MODULE,
                        pm.FILE_ID,
                        pm.LAST_RUN_DATE,
                        pm.TABLE_NAME,
                        pm.CM_DATE,
                        pm."monitor features",
                        pm.MAN_ID,
                        pm.MODULE_ID,
                        CASE 
                            WHEN pm.STATUS = 'not_valid' THEN 'not_valid'
                            WHEN NOT EXISTS (
                                SELECT 1 
                                FROM updatesys.TBL_Prty_pns_@NEW3_N st 
                                WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID
                            ) THEN 'not SE part'
                            WHEN EXISTS (
                                SELECT 1 
                                FROM updatesys.TBL_Prty_pns_@NEW3_N st 
                                WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID AND st.mod_id = pm.MODULE_ID 
                                AND st.LRD is null and st.V_NOTFOUND_DAT is null
                            ) THEN 'not run'
                            WHEN EXISTS (
                                SELECT 1 
                                FROM updatesys.TBL_Prty_pns_@NEW3_N st 
                                WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID AND st.mod_id = pm.MODULE_ID 
                                AND NVL(cm.XLP_RELEASEDATE_FUNCTION_D(st.LRD), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) >
                                NVL(st.V_NOTFOUND_DAT, TO_DATE('01-JAN-1900', 'DD-MON-YYYY'))
                            ) THEN 'found'
                            ELSE 'not found'
                        END AS STATUS,
                        (SELECT 
                            GREATEST(NVL(cm.XLP_RELEASEDATE_FUNCTION_D(st.LRD), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')), 
                            NVL(st.V_NOTFOUND_DAT, TO_DATE('01-JAN-1900', 'DD-MON-YYYY')))
                        FROM updatesys.TBL_Prty_pns_@NEW3_N st 
                        WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID AND st.mod_id = pm.MODULE_ID
                        FETCH FIRST 1 ROWS ONLY
                        ) AS LRD_V_NOTFOUND_MAX,
                        (SELECT 
                            prty
                        FROM updatesys.TBL_Prty_pns_@NEW3_N st 
                        WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID AND st.mod_id = pm.MODULE_ID
                        FETCH FIRST 1 ROWS ONLY
                        ) AS prty
                    FROM parts_id pm
                ),
                not_f AS (
                    SELECT 
                        fnf_1.*,
                        nf.check_date,
                        nf.status AS status_2
                    FROM found_notfound_table fnf_1 
                    LEFT JOIN webspider.TBL_PRSYS_FEED_NOTFOUND@NEW3_N nf 
                    ON nf.mpn = fnf_1.PART AND nf.man_id = fnf_1.MAN_ID AND nf.mod_id = fnf_1.MODULE_ID
                )
                SELECT DISTINCT
                    PART_ID, 
                    PART, 
                    MAN, 
                    MODULE, 
                    FILE_ID,
                    CM_DATE, 
                    CASE 
                        WHEN STATUS <> 'found' THEN STATUS_2 
                        ELSE NULL 
                    END AS STATUS,
                    CASE 
                        WHEN LAST_RUN_DATE > TO_DATE('01-JAN-2001', 'DD-MON-YYYY') THEN LAST_RUN_DATE 
                        ELSE NULL 
                    END AS LAST_RUN_DATE,
                    STATUS AS TABLE_NAME, 
                    "monitor features", 
                    MAN_ID, 
                    MODULE_ID,
                    prty
                FROM (
                    SELECT 
                        fnf_1.PART_ID, 
                        fnf_1.PART, 
                        fnf_1.MAN, 
                        fnf_1.MODULE, 
                        fnf_1.FILE_ID, 
                        fnf_1.CM_DATE,  
                        fnf_1.TABLE_NAME, 
                        fnf_1."monitor features", 
                        fnf_1.MAN_ID, 
                        fnf_1.MODULE_ID, 
                        fnf_1.prty,
                        fnf_1.STATUS,
                        fnf_1.STATUS_2,
                        GREATEST(
                            NVL(fnf_1.check_date, TO_DATE('01-JAN-1900', 'DD-MON-YYYY')),
                            NVL(fnf_1.LRD_V_NOTFOUND_MAX, TO_DATE('01-JAN-1900', 'DD-MON-YYYY'))
                        ) AS LAST_RUN_DATE, 
                        ROW_NUMBER() OVER (
                            PARTITION BY fnf_1.PART_ID 
                            ORDER BY NVL(fnf_1.check_date, TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) DESC
                        ) AS rn
                    FROM not_f fnf_1
                ) ranked
                WHERE rn = 1
            ) query_results
            ON (p.ID = query_results.PART_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    p.STATUS = query_results.STATUS,
                    p.LAST_RUN_DATE = query_results.LAST_RUN_DATE,
                    p.TABLE_NAME = query_results.TABLE_NAME,
                    p."monitor features" = query_results."monitor features",
                    p.prty = query_results.prty,
                    p.man_id = query_results.man_id,
                    p.module_id = query_results.module_id
                    

    """)
    

        query =  SQLQueries.q_overall_status.format(files_string=str(files_string), check_date=check_date, date_condition=date_condition)
        update_files_query = f"""
                    UPDATE uploaded_files
                    SET 
                        LAST_CHECK_DATE = TO_DATE('{datetime.now().date().strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
                    WHERE file_name in {files_string}
                    
            """
        
        # Execute queries within a transaction 
        with engine.begin() as connection:
            try:
                status_logger.info("Starting Not Found status update query execution...")
                connection.execute(text(query))
                status_logger.info("Status calculation completed successfully")
                
                status_logger.info("Updating last check dates...")
                connection.execute(text(update_files_query))
                status_logger.info("Last check dates updated successfully")
                
            except Exception as e:
                error_msg = f"Error in database transaction: {str(e)}"
                status_logger.error(error_msg)
                status_logger.error(traceback.format_exc())
                raise Exception(error_msg)
        
        status_logger.info("Starting Found_parts_Status processing")
        Found_parts_Status(engine=engine, files_string=files_string)
        status_logger.info("Found_parts_Status processing completed successfully")
        
        status_logger.info("Get_status function completed successfully")
        return files_string, daily_export

    except Exception as e:
        error_msg = f"Error in Get_status: {str(e)}"
        status_logger.error(error_msg)
        status_logger.error(traceback.format_exc())
        raise
    finally:
        engine.dispose()
        status_logger.info("Database connection disposed")


def Found_parts_Status(engine, files_string):
    try:
        status_logger.info("Starting Found_parts_Status function")
        with engine.begin() as connection:
            file_id_query = text(f"""
                SELECT id 
                FROM uploaded_files 
                WHERE file_name IN {files_string}
            """)
            
            status_logger.info("Fetching file IDs from database")
            file_ids = connection.execute(file_id_query).fetchall()
            file_ids = [row[0] for row in file_ids]
            
            if not file_ids:
                status_logger.warning("No matching file IDs found for the given file names.")
                return

            status_logger.info(f"Processing {len(file_ids)} files")
            for file_id in file_ids:
                try:
                    status_logger.info(f"Processing file ID: {file_id}")
                    with engine.begin() as file_connection:
                        part_query = text(f"""
                            SELECT DISTINCT
                                m.module_name,
                                m.MAN_ID, 
                                m.MODULE_ID
                            FROM 
                                (SELECT * FROM parts) p
                            LEFT JOIN 
                                updatesys.tbl_man_modules@new3_n m 
                            ON 
                                p.MODULE = m.MODULE_NAME
                            WHERE 
                                m.MAN_ID IS NOT NULL
                                AND m.MODULE_ID IS NOT NULL
                                AND p.file_id = :file_id
                        """)
                        
                        status_logger.info(f"Fetching modules for file ID: {file_id}")
                        result = file_connection.execute(part_query, {"file_id": file_id}).fetchall()
                        status_logger.info(f"Found {len(result)} modules to process for file ID: {file_id}")
                        
                        percent = 0
                        for rec in result:
                            try:
                                module_name = rec[0]
                                man_id = rec[1]
                                module_id = rec[2]
                                table_name = f"updatesys.tbl_{man_id}_{module_id}@new3_n"
                                
                                status_logger.info(f"Processing module: {module_name}, table: {table_name}, progress: {percent/len(result)*100:.2f}%")
                                percent += 1
                                
                                v_sql = f"""
                                    MERGE INTO parts tgt
                                    USING (
                                        WITH RankedFeatures AS (
                                            SELECT 
                                                p.ID AS PART_ID,
                                                p.PART,
                                                cm.XLP_RELEASEDATE_FUNCTION_D(tbl.DAT) AS NEW_LAST_RUN_DATE,
                                                tbl.FEATURE_NAME,
                                                RANK() OVER (PARTITION BY p.ID, p.PART ORDER BY tbl.DAT DESC) AS rnk
                                            FROM 
                                                (SELECT * FROM parts WHERE module = '{module_name}') p
                                            JOIN 
                                                (SELECT DISTINCT PART, FEATURE_NAME, DAT
                                                FROM {table_name}) tbl
                                            ON 
                                                p.PART = tbl.PART
                                        )
                                        SELECT 
                                            PART_ID,
                                            PART,
                                            TO_CLOB(
                                                XMLAGG(
                                                    XMLELEMENT(e, FEATURE_NAME || '|').EXTRACT('//text()')
                                                ).GetClobVal()
                                            ) AS FEATURES,
                                            TO_DATE(NEW_LAST_RUN_DATE, 'DD-MON-YY') as NEW_LAST_RUN_DATE
                                        FROM 
                                            RankedFeatures
                                        WHERE 
                                            rnk = 1
                                        GROUP BY 
                                            PART_ID,
                                            PART,
                                            NEW_LAST_RUN_DATE
                                    ) src
                                    ON (tgt.ID = src.PART_ID AND tgt.PART = src.PART)
                                    WHEN MATCHED THEN
                                        UPDATE 
                                        SET 
                                            tgt."Features" = FEATURES,
                                            tgt.LAST_RUN_DATE = CASE 
                                                WHEN src.NEW_LAST_RUN_DATE IS NOT NULL AND (tgt.LAST_RUN_DATE IS NULL OR src.NEW_LAST_RUN_DATE > tgt.LAST_RUN_DATE) 
                                                THEN TO_DATE(src.NEW_LAST_RUN_DATE, 'DD-MON-YY')
                                                ELSE TO_DATE(tgt.LAST_RUN_DATE, 'DD-MON-YY')
                                            END,
                                            tgt.status = CASE 
                                                WHEN src.NEW_LAST_RUN_DATE IS NOT NULL AND (tgt.LAST_RUN_DATE IS NULL OR src.NEW_LAST_RUN_DATE >= tgt.LAST_RUN_DATE) 
                                                THEN 'found' 
                                                ELSE tgt.status 
                                            END,
                                            tgt.TABLE_NAME = CASE 
                                                WHEN src.NEW_LAST_RUN_DATE IS NOT NULL AND (tgt.LAST_RUN_DATE IS NULL OR src.NEW_LAST_RUN_DATE >= tgt.LAST_RUN_DATE) and tgt.TABLE_NAME <> 'not SE part'
                                                THEN 'found' 
                                                ELSE tgt.TABLE_NAME 
                                            END
                                """
                                # Second dynamic SQL query (v_sql2)
                                v_sql2 = f"""
                                    MERGE INTO parts tgt
                                    USING (
                                        WITH RankedFeatures AS (
                                            SELECT 
                                                p.ID AS PART_ID,
                                                p.PART,
                                                cm.XLP_RELEASEDATE_FUNCTION_D(tbl.DAT) AS NEW_LAST_RUN_DATE,
                                                RANK() OVER (PARTITION BY p.ID, p.PART ORDER BY tbl.DAT DESC) AS rnk
                                            FROM 
                                                (SELECT * FROM parts WHERE module = '{module_name}') p
                                            JOIN 
                                                (SELECT DISTINCT PART, DAT
                                                FROM {table_name}) tbl
                                            ON 
                                                p.PART = tbl.PART
                                        )
                                        SELECT DISTINCT
                                            PART_ID,
                                            PART,
                                            TO_DATE(NEW_LAST_RUN_DATE, 'DD-MON-YY') as NEW_LAST_RUN_DATE
                                        FROM 
                                            RankedFeatures
                                        WHERE 
                                            rnk = 1
                                    ) src
                                    ON (tgt.ID = src.PART_ID AND tgt.PART = src.PART)
                                    WHEN MATCHED THEN
                                        UPDATE 
                                        SET 
                                            tgt.LAST_RUN_DATE = CASE 
                                                WHEN src.NEW_LAST_RUN_DATE IS NOT NULL AND (tgt.LAST_RUN_DATE IS NULL OR src.NEW_LAST_RUN_DATE > tgt.LAST_RUN_DATE) 
                                                THEN TO_DATE(src.NEW_LAST_RUN_DATE, 'DD-MON-YY')
                                                ELSE TO_DATE(tgt.LAST_RUN_DATE, 'DD-MON-YY')
                                            END,
                                            tgt.status = CASE 
                                                WHEN src.NEW_LAST_RUN_DATE IS NOT NULL AND (tgt.LAST_RUN_DATE IS NULL OR src.NEW_LAST_RUN_DATE >= tgt.LAST_RUN_DATE) 
                                                THEN 'found' 
                                                ELSE tgt.status 
                                            END,
                                            tgt.TABLE_NAME = CASE 
                                                WHEN src.NEW_LAST_RUN_DATE IS NOT NULL AND (tgt.LAST_RUN_DATE IS NULL OR src.NEW_LAST_RUN_DATE >= tgt.LAST_RUN_DATE) and tgt.TABLE_NAME <> 'not SE part'
                                                THEN 'found' 
                                                ELSE tgt.TABLE_NAME 
                                            END
                                """
                                
                                try:
                                    status_logger.info(f"Executing v_sql for module {module_name}")
                                    file_connection.execute(text(v_sql))
                                    status_logger.info(f"Successfully executed v_sql for module {module_name}")
                                except Exception as e:
                                    status_logger.error(f"Error executing v_sql for module {module_name}, table {table_name}: {e}")
                                    status_logger.info("Attempting to execute v_sql2...")
                                    try:
                                        file_connection.execute(text(v_sql2))
                                        status_logger.info(f"Successfully executed v_sql2 for module {module_name}")
                                    except Exception as e2:
                                        status_logger.error(f"Error executing v_sql2 for module {module_name}, table {table_name}: {e2}")
                                        raise
                            except Exception as e:
                                status_logger.error(f"Error processing record: {str(e)}")
                                continue
                                
                    status_logger.info(f"Completed processing file ID: {file_id}")
                except Exception as e:
                    status_logger.error(f"Error processing file ID {file_id}: {str(e)}")
                    continue
                    
        status_logger.info("Found_parts_Status function completed successfully")
    except Exception as e:
        status_logger.error(f"Error in Found_parts_Status: {str(e)}")
        status_logger.error(traceback.format_exc())
        raise

def fetch_results_from_database(files_list = None, daily_export=False):
    status_logger.info("Starting fetch_results_from_database function")
    engine = create_db_engine()
    
    try:
        if files_list is None:
            status_logger.info("No files specified, fetching all results")
            result_query = text("select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.* ,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id  )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name ")
        else:
            files_string = files_list if isinstance(files_list, str) else str(tuple(files_list))
            if isinstance(files_list, list) and len(files_list)==1:
                files_string = files_string[0:-2]+')'
            status_logger.info(f"Fetching results for files: {files_string}")
            result_query = text(f"""select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.* ,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string} )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name """)
        
        with engine.connect() as connection:
            status_logger.info("Executing database query")
            result = connection.execute(result_query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        status_logger.info(f"Retrieved {len(df)} records from database")
        df['last_run_date'] = pd.to_datetime(df['last_run_date'], errors='coerce')
        df['is_expired'] = (df['last_run_date'].isna()) | (df['last_run_date'] < datetime.now() - timedelta(days=Config.Date_to_expire))
        status_logger.info("Data processing completed")
        
        return df
    except Exception as e:
        status_logger.error(f"Error in fetch_results_from_database: {str(e)}")
        status_logger.error(traceback.format_exc())
        raise
    finally:
        engine.dispose()
        status_logger.info("Database connection disposed")


def Download_results(files_list = None, daily_export=False):
    status_logger.info("Starting Download_results function")
    engine = create_db_engine()
    
    try:
        if files_list is None:
            status_logger.info("No files specified, downloading all results")
            result_query = text("select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.* ,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name ")
        else:
            files_string = files_list if isinstance(files_list, str) else str(tuple(files_list))
            if isinstance(files_list, list) and len(files_list)==1:
                files_string = files_string[0:-2]+')'
            status_logger.info(f"Downloading results for files: {files_string}")
            result_query = text(f"""select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.*,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string} )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name""")
        
        with engine.connect() as connection:
            status_logger.info("Executing database query")
            result = connection.execute(result_query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        status_logger.info(f"Retrieved {len(df)} records from database")
        df['last_run_date'] = pd.to_datetime(df['last_run_date'], errors='coerce')
        df['is_expired'] = (df['last_run_date'].isna()) | (df['last_run_date'] < datetime.now() - timedelta(days=Config.Date_to_expire))
        
        # Save results using thread-safe method
        results_csv = os.path.join(Config.result_path, 'results.csv')
        status_logger.info(f"Saving results to {results_csv}")
        safe_write_csv(df, results_csv)
        status_logger.info("Results saved successfully")

        if daily_export:
            file_name = f'Monitor_Parts_Status@{datetime.now().strftime("%Y-%m-%d__%H_%M_%S")}.txt'
            file_path = os.path.join(Config.result_path, file_name)
            shared_path = os.path.join(Config.shared_path, file_name)
            status_logger.info(f"Performing daily export to {shared_path}")
            safe_write_csv(df, file_path)
            safe_write_csv(df, shared_path)
            status_logger.info("Daily export completed successfully")
        else:
            file_name = 'results.csv'
        
        status_logger.info("Download_results function completed successfully")
        return df, file_name
    
    except Exception as e:
        status_logger.error(f"Error in Download_results: {str(e)}")
        status_logger.error(traceback.format_exc())
        raise
    finally:
        engine.dispose()
        status_logger.info("Database connection disposed")


def Get_file_stats(global_df):
    # Calculate file statistics
    global_df['status'] = global_df['status'].apply(lambda x: 
			'Proxy' if '403' in str(x) else
			'Error' if any(err in str(x) for err in ['Error', 'Exception', 'Incomplete']) else
			x
		)
		
    global_df['status'] = global_df['status'].fillna('-')
    global_df['prty'] = global_df['prty'].fillna('-')
    global_df['table_name'] = global_df['table_name'].fillna('-')
    
    df = global_df.groupby(
        ['man', 'module', 'file_id', 'status', 'last_run_date', 
        'table_name', 'prty', 'file_name', 'is_expired'], dropna=False
    ).agg(
        count=('part', 'size'),  # Count the number of rows
        last_check_date=('last_check_date', 'first'),  # First value of last_check_date
        upload_date = ('upload_date','first'),
        stop_monitor_date=('stop_monitor_date', 'first'),  # First value of stop_monitor_date
        man_id=('man_id', 'first'),  # First value of man_id
        module_id=('module_id', 'first'),  # First value of module_id
        wda_flag=('wda_flag', 'first')  # First value of wda_flag
    ).reset_index()
    file_stats = []
    for file_name in df['file_name'].unique():
        file_df = df[df['file_name'] == file_name]
        total_count = file_df['count'].sum()
        error_count = file_df[file_df['status'].isin(['Error','Exception', 'Proxy','Incomplete'])]['count'].sum()
        notFound_count = file_df[file_df['status']== 'Not Found']['count'].sum()
        found_count = file_df[file_df['status'] == 'found']['count'].sum()
        Done_parts = file_df[file_df['last_run_date']>=file_df['upload_date']]['count'].sum()
        error_Done_parts = file_df[(file_df['last_run_date']>=file_df['upload_date']) & file_df['status'].isin(['Error','Exception', 'Proxy','Incomplete'])]['count'].sum()
        file_stats.append({
            'file': file_name,
            'total_count': int(total_count),
            'error_count': int(error_count),
            'NotFound_count': int(notFound_count),
            'error_percentage': round((error_count / total_count) * 100, 2) if total_count > 0 else 0,
            'found_count': int(found_count),
            'found_percentage': round((found_count / total_count) * 100, 2) if total_count > 0 else 0,
            'done_percentage': round((Done_parts / total_count) * 100, 2) if total_count > 0 else 0,
            'Error_done_percentage': round((error_Done_parts / Done_parts) * 100, 2) if Done_parts > 0 else 0
        })

    # Sort by total count descending
    file_stats.sort(key=lambda x: x['total_count'], reverse=True)
    return file_stats



# ... rest of the imports ...

def send_status_email(file_stats):
    """Send email with file processing statistics using Outlook"""
    try:
        # Create table from file_stats
        headers = ['File', 'Total', 'Errors', 'Not Found', 'Error %', 'Found', 'Found %', 'Done %', 'Error Done %']
        table_data = [[
            stat['file'],
            stat['total_count'],
            stat['error_count'],
            stat['NotFound_count'],
            f"{stat['error_percentage']}%",
            stat['found_count'],
            f"{stat['found_percentage']}%",
            f"{stat['done_percentage']}%",
            f"{stat['Error_done_percentage']}%"
        ] for stat in file_stats]
        
        table = tabulate(table_data, headers=headers, tablefmt='html')
        # Helper function to determine color based on percentage
        def get_color(percentage):
            if percentage < 40:
                return "red"
            elif 40 <= percentage < 70:
                return "orange"
            else:
                return "green"
        # Create HTML content
        # Create HTML content with improved styling
        html_body = f"""
            <html>
                <head>
                    <style>
                        table {{
                            width: 100%;
                            border: 1px solid black;
                            border-collapse: collapse;
                            font-family: Arial, sans-serif;
                        }}
                        th {{
                            background-color: #4CAF50;
                            color: white;
                            padding: 10px;
                        }}
                        td {{
                            padding: 8px;
                            text-align: center;
                            border: 1px solid #ddd;
                        }}
                        tr:nth-child(even) {{ background-color: #f2f2f2; }}
                        tr:hover {{ background-color: #ddd; }}
                    </style>
                </head>
                <body>
                    <h2 class="alert">⚠️ This is an automated email. ⚠️</h2>
                    <h2>Daily Export Status Report</h2>
                    <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <table>
                        <thead>
                            <tr>
                                {"".join(f"<th>{header}</th>" for header in headers)}
                            </tr>
                        </thead>
                        <tbody>
                            {"".join(
                                f"<tr>"
                                f"<td>{stat['file']}</td>"
                                f"<td>{stat['total_count']}</td>"
                                f"<td>{stat['error_count']}</td>"
                                f"<td>{stat['NotFound_count']}</td>"
                                f"<td style='color: {get_color(100-stat['error_percentage'])};'>{stat['error_percentage']}%</td>"
                                f"<td>{stat['found_count']}</td>"
                                f"<td style='color: {get_color(stat['found_percentage'])};'>{stat['found_percentage']}%</td>"
                                f"<td style='color: {get_color(stat['done_percentage'])};'>{stat['done_percentage']}%</td>"
                                f"<td style='color: {get_color(100-stat['Error_done_percentage'])};'>{stat['Error_done_percentage']}%</td>"
                                f"</tr>"
                                for stat in file_stats
                            )}
                        </tbody>
                    </table>
                </body>
            </html>
        """

        

        
        # Initialize Outlook and Redemption
        outlook = win32.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)  # 0 represents olMailItem

        # Use Redemption to bypass restrictions
        safe_mail = win32.Dispatch('Redemption.SafeMailItem')  # Redemption SafeMailItem
        safe_mail.Item = mail  # Assign the Outlook mail item to Redemption

        # Set email properties using Redemption
        safe_mail.Subject = f"Daily Export Status Report - {datetime.now().strftime('%Y-%m-%d')}"
        safe_mail.HTMLBody = html_body
        recipients = Config.EMAIL_TO
        for recipient in recipients:
            mail.Recipients.Add(recipient)
        # Send the email
        safe_mail.Send()

        status_logger.info("Status email sent successfully using Redemption")
    except Exception as e:
        status_logger.error(f"Error sending status email: {str(e)}")
        status_logger.error(traceback.format_exc())
        print("error", e)


def daily_check_all():
    
    files_string , daily_export = Get_status(ignore_date = False,daily_export=True)
    df, file_name = Download_results(files_string, daily_export)
    #df = pd.read_csv(r'results\results.csv')
    if True:
        file_stats = Get_file_stats(df)
        #send_status_email(file_stats)

def daily_check_all2():
    
    files_string , daily_export = Get_status(ignore_date = False,daily_export=True)
    df, file_name = Download_results(files_string, daily_export)
    df = pd.read_csv(r'results\results.csv')
    if True:
        file_stats = Get_file_stats(df)
        send_status_email(file_stats)
if __name__=="__main__":

    df = pd.read_csv(r'results\results.csv')
    if True:
        file_stats = Get_file_stats(df)
        send_status_email(file_stats)
