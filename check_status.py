import os
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import oracledb
from config import Config
import traceback
from sqlalchemy.pool import QueuePool
import tempfile
import shutil
import filelock

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
                data = connection.execute(get_files_string).fetchall()
                print(data)
            except:
                pass
        
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
        if files_list is None:
            files_list = os.listdir(Config.WORK_FOLDER)
        files_string = str(tuple(files_list))
        if len(files_list)==1:
            files_string = files_string[0:-2]+')'
        
        if ignore_date:
            check_date = ""
        else:
            check_date = "and NVL(TO_CHAR(last_check_date,'DD-MON-YYYY'), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) < TO_CHAR(sysdate,'DD-MON-YYYY')"

        print("Calculating Status for: ", files_string)
        
        query = text(f"""

            MERGE INTO parts p
            USING (
                WITH files_id as (select id from uploaded_files  where file_name  in {str(files_string)} {check_date} )
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
                    p.prty = query_results.prty

    """)
    
        update_files_query = f"""
                    UPDATE uploaded_files
                    SET 
                        LAST_CHECK_DATE = TO_DATE('{datetime.now().date().strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
                    WHERE file_name in {files_string}
                    
            """
        
        # Execute queries within a transaction 
        with engine.begin() as connection:
            try:
                print("in progress...")
                connection.execute(query)
                print("calculating Status Done...")
                connection.execute(text(update_files_query))
                print("updating files Status..")
                # Transaction will automatically commit if no exceptions occur
                print("finalizing calculation...")
            except Exception as e:
                # Transaction will automatically rollback on exception
                print(f"Error in database transaction: {str(e)}")
                print(traceback.format_exc())
                raise
        
        print("found parts begin")
        Found_parts_Status(engine=engine, files_string=files_string)
        print("found parts done")
        return files_string , daily_export
        #return Download_results(files_string, daily_export)

    except Exception as e:
        print(f"Error in Get_status: {str(e)}")
        print(traceback.format_exc())
        raise
    finally:
        engine.dispose()


def Found_parts_Status(engine, files_string):
    """
    Process parts by executing dynamic SQL queries.
    Use file names to fetch corresponding file IDs and process the data.

    Args:
        engine: SQLAlchemy engine connected to the database.
        file_names: List of file names to filter on.
    """
    try:
        with engine.begin() as connection:
            # Query to fetch file IDs based on file names
            file_id_query = text(f"""
                SELECT id 
                FROM uploaded_files 
                WHERE file_name IN {files_string}
            """)
            
            file_ids = connection.execute(file_id_query).fetchall()
            file_ids = [row[0] for row in file_ids]
            
            if not file_ids:
                print("No matching file IDs found for the given file names.")
                return

            for file_id in file_ids:
                try:
                    # Process each file within its own transaction
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
                        
                        result = file_connection.execute(part_query, {"file_id": file_id}).fetchall()
                        percent = 0
                        for rec in result:
                            try:
                                module_name = rec[0]
                                
                                man_id = rec[1]
                                module_id = rec[2]
                                table_name = f"updatesys.tbl_{man_id}_{module_id}@new3_n"
                                print("fetching found parts for \nmodule: ",module_name,"\ttable_name: ",table_name,"\nfinished: %",percent/len(result)*100)
                                percent+=1
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
                        
                                # Execute v_sql with proper error handling
                                try:
                                    file_connection.execute(text(v_sql))
                                except Exception as e:
                                    print(f"Error executing v_sql for module {module_name}, table {table_name}: {e}")
                                    print("Attempting to execute v_sql2...")
                                    try:
                                        file_connection.execute(text(v_sql2))
                                    except Exception as e2:
                                        print(f"Error executing v_sql2 for module {module_name}, table {table_name}: {e2}")
                                        raise
                            except Exception as e:
                                print(f"Error processing record: {str(e)}")
                                continue
                except Exception as e:
                    print(f"Error processing file ID {file_id}: {str(e)}")
                    continue
    except Exception as e:
        print(f"Error in Found_parts_Status: {str(e)}")
        print(traceback.format_exc())
        raise

def fetch_results_from_database(files_list = None, daily_export=False):
    print("fetching_result...")
    engine = create_db_engine()
    
    try:
        if files_list is None:
            result_query = text("select x.* , y.file_name from parts x join uploaded_files y on x.file_id = y.id ")
        else:
            files_string = files_list if isinstance(files_list, str) else str(tuple(files_list))
            if isinstance(files_list, list) and len(files_list)==1:
                files_string = files_string[0:-2]+')'
        
            result_query = text(f"""select x.* , y.file_name from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string}""")
        
        with engine.connect() as connection:
            result = connection.execute(result_query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        print("fetching finished Exporting files...")
        df['last_run_date'] = pd.to_datetime(df['last_run_date'], errors='coerce')
        df['is_expired'] = (df['last_run_date'].isna()) | (df['last_run_date'] < datetime.now() - timedelta(days=Config.Date_to_expire))
        
        return df
    finally:
        engine.dispose()


def Download_results(files_list = None, daily_export=False):
    print("fetching_result...")
    engine = create_db_engine()
    
    try:
        if files_list is None:
            result_query = text("select x.* , y.file_name from parts x join uploaded_files y on x.file_id = y.id ")
        else:
            files_string = files_list if isinstance(files_list, str) else str(tuple(files_list))
            if isinstance(files_list, list) and len(files_list)==1:
                files_string = files_string[0:-2]+')'
        
            result_query = text(f"""select x.* , y.file_name from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string}""")
        
        with engine.connect() as connection:
            result = connection.execute(result_query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        print("fetching finished Exporting files...")
        df['last_run_date'] = pd.to_datetime(df['last_run_date'], errors='coerce')
        df['is_expired'] = (df['last_run_date'].isna()) | (df['last_run_date'] < datetime.now() - timedelta(days=Config.Date_to_expire))
        
        # Save results using thread-safe method
        results_csv = os.path.join(Config.result_path, 'results.csv')
        safe_write_csv(df, results_csv)
        print("Results saved to results.csv")

        if daily_export:
            file_name = f'Monitor_Parts_Status@{datetime.now().strftime("%Y-%m-%d__%H_%M_%S")}.txt'
            file_path = os.path.join(Config.result_path, file_name)
            shared_path = os.path.join(Config.shared_path, file_name)
            safe_write_csv(df, file_path)
            safe_write_csv(df, shared_path)
            print("Data Exported successfully to: ", shared_path)
        else:
            file_name = 'results.csv'
        
        print("Saving Status....")
        return df, file_name
    
    finally:
        engine.dispose()

if __name__=="__main__":

    
    files_string , daily_export = Get_status(ignore_date = False,daily_export=True)
    df, file_name = Download_results(files_string, daily_export)
    