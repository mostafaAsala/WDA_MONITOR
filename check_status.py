import os
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import oracledb
from config import Config
import traceback
# Database configuration: Replace "your_database_url_here" with your actual database connection string
DATABASE_URL = Config.DB_URI


# Update Oracle Client initialization
try:
    instant_client_path = os.path.abspath(Config.INSTANT_CLIENT)
    os.environ["PATH"] = instant_client_path + os.pathsep + os.environ["PATH"]
    os.environ["ORACLE_HOME"] = instant_client_path
    oracledb.init_oracle_client(lib_dir=instant_client_path)
except Exception as e:
    print(f"Error initializing Oracle Client: {e}")
    raise
def get_status_statistics(df):
    """Get status statistics from DataFrame"""

    status_stats = df['status'].value_counts().to_dict()
    return status_stats

def Get_status(files_list=None, ignore_date=True, daily_export=False):
    # Create the SQLAlchemy engine
    
    if files_list is None:
        files_list = os.listdir(Config.WORK_FOLDER)
    files_string = str(tuple(files_list))
    if len(files_list)==1:
        files_string = files_string[0:-2]+')'
    
    if ignore_date:
        check_date = ""
    else:
        check_date = "and NVL(TO_CHAR(last_check_date,'DD-MON-YYYY'), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) < TO_CHAR(sysdate,'DD-MON-YYYY')"
    engine = create_engine(DATABASE_URL)

    print("Calculating Status for: ", files_string)
        
    query = """
        WITH parts_id AS (
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
                p."Features", 
                p."monitor features", 
                m.MAN_ID, 
                m.MODULE_ID
            FROM parts p
            LEFT JOIN updatesys.tbl_man_modules@new3_n m 
            ON p.module = m.module_name
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
            pm."Features",
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
                    AND NVL(cm.XLP_RELEASEDATE_FUNCTION_D(st.LRD), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) > NVL(st.V_NOTFOUND_DAT, TO_DATE('01-JAN-1900', 'DD-MON-YYYY'))
                ) THEN 'found'
                ELSE 'not found'
            END AS STATUS,
            (SELECT 
                GREATEST(NVL(cm.XLP_RELEASEDATE_FUNCTION_D(st.LRD), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')), NVL(st.V_NOTFOUND_DAT, TO_DATE('01-JAN-1900', 'DD-MON-YYYY')))
            FROM updatesys.TBL_Prty_pns_@NEW3_N st 
            WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID AND st.mod_id = pm.MODULE_ID
            FETCH FIRST 1 ROWS ONLY -- Ensures only one result is returned
            ) AS LRD_V_NOTFOUND_MAX,
            (SELECT 
                prty
            FROM updatesys.TBL_Prty_pns_@NEW3_N st 
            WHERE st.pn = pm.PART AND st.man_id = pm.MAN_ID AND st.mod_id = pm.MODULE_ID
            FETCH FIRST 1 ROWS ONLY -- Ensures only one result is returned
            ) AS prty
        FROM parts_id pm

        ),
        not_f As
        (
            select 
                fnf_1.* ,
                nf.check_date,
                nf.status as status_2
            from found_notfound_table fnf_1 
            left join  webspider.TBL_PRSYS_FEED_NOTFOUND@NEW3_N nf on 
                nf.mpn = fnf_1.PART AND 
                nf.man_id = fnf_1.MAN_ID AND 
                nf.mod_id = fnf_1.MODULE_ID

        ),
        found AS(select 1 from dual)


        SELECT DISTINCT
            PART_ID, 
            PART, 
            MAN, 
            MODULE, 
            FILE_ID, 
            CM_DATE, 
            LAST_RUN_DATE, 
            TABLE_NAME, 
            "Features", 
            "monitor features", 
            MAN_ID, 
            MODULE_ID, 
            STATUS,
            prty,
            STATUS_2,
            LRD_V_NOTFOUND_MAX,
            check_date AS max_check_date
        FROM (
            SELECT 
                fnf_1.PART_ID, 
                fnf_1.PART, 
                fnf_1.MAN, 
                fnf_1.MODULE, 
                fnf_1.FILE_ID, 
                fnf_1.CM_DATE, 
                fnf_1.LAST_RUN_DATE, 
                fnf_1.TABLE_NAME, 
                fnf_1."Features", 
                fnf_1."monitor features", 
                fnf_1.MAN_ID, 
                fnf_1.MODULE_ID, 
                fnf_1.prty,
                fnf_1.STATUS,
                fnf_1.STATUS_2,
                fnf_1.check_date,
                fnf_1.LRD_V_NOTFOUND_MAX,
                ROW_NUMBER() OVER (PARTITION BY fnf_1.PART_ID ORDER BY NVL(fnf_1.check_date, TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) DESC) AS rn
            FROM not_f fnf_1
        ) ranked
        WHERE rn = 1
    """
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
                    p."Features", 
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
                    pm."Features",
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
                "Features", 
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
                    fnf_1."Features", 
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
                p."Features" = query_results."Features",
                p."monitor features" = query_results."monitor features",
                p.prty = query_results.prty

    """)
    
    update_files_query = f"""
                UPDATE uploaded_files
                SET 
                    LAST_CHECK_DATE = TO_DATE('{datetime.now().date().strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
                WHERE file_name in {files_string}
                
        """
    
    
    try:
        with engine.begin() as connection:
            print("in progress...")
            connection.execute(query)
            print("calculating Status Done...")
            connection.execute(text(update_files_query))
            print("updating files Status..")
            connection.commit()
            print("finalizing calculation...")
            
    except Exception as e:
        print(traceback.format_exc())
    
    
    return Download_results(files_string,daily_export)

def Download_results(files_list = None,daily_export=False):
    print("fetching_result...")
    engine = create_engine(DATABASE_URL)
    if files_list is None:
        result_query = text("select x.* , y.file_name from parts x join uploaded_files y on x.file_id = y.id ")
    else:
        files_string = files_list

        if not(isinstance(files_list,str)):
            files_string = str(tuple(files_list))
            if len(files_list)==1:
                files_string = files_string[0:-2]+')'
    
        result_query = text(f"""select x.* , y.file_name from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string}""")
    
    with engine.connect() as connection:
        result = connection.execute(result_query)
    print("fetching finished Exporting files...")
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df['last_run_date'] = pd.to_datetime(df['last_run_date'], errors='coerce')

    # Calculate 'is_expired' column
    df['is_expired'] = (df['last_run_date'].isna()) | (df['last_run_date'] < datetime.now() - timedelta(days=Config.Date_to_expire))
    
    # Always save to results.csv
    results_csv = os.path.join(Config.result_path, 'results.csv')
    df.to_csv(results_csv, index=False)
    print("Results saved to results.csv")

    # Save timestamped file if daily_export is True
    if daily_export:
        file_name = f'Monitor_Parts_Status@{datetime.now().strftime("%Y-%m-%d__%H_%M_%S")}.txt'
        file_path = os.path.join(Config.result_path, file_name)
        shared_path = os.path.join(Config.shared_path, file_name)
        df.to_csv(file_path)
        df.to_csv(shared_path)
        print("Data Exported successfully to: ", shared_path)
    else:
        file_name = 'results.csv'
    print("Saving Status....")
    return df, file_name

if __name__=="__main__":
    Get_status(ignore_date = False,daily_export=True)