import os
import dotenv

dotenv.load_dotenv()

class Config:
    # Database Configuration
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = 'wda-monitor-secure-key-2024-change-in-production'
    DB_URI = os.getenv("DB_URI")
    SQLALCHEMY_DATABASE_URI = os.getenv("DB_URI")
    print("--------------------------------------------------------")
    print(SQLALCHEMY_DATABASE_URI,DB_URI,list(os.environ))
    print("--------------------------------------------------------")
    WORK_FOLDER = "Data"
    AMAZON_FOLDER = 'Data_amazon'
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


class SQLQueries:
    # Define your SQL queries here
    q_add_check_date = "and NVL(TO_CHAR(last_check_date,'DD-MON-YYYY'), TO_DATE('01-JAN-1900', 'DD-MON-YYYY')) < TO_CHAR(sysdate,'DD-MON-YYYY')"
    q_stop_monitor = "and NVL(stop_monitor_date, TO_DATE('01-JAN-2999', 'DD-MON-YYYY')) > sysdate"

    q_overall_status = """

            MERGE INTO parts p
            USING (
                WITH files_id as (select id from uploaded_files  where file_name  in {files_string} {check_date} {date_condition} )
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


    """

    q_update_files_table = """
                    UPDATE uploaded_files
                    SET
                        LAST_CHECK_DATE = TO_DATE('{datetime.now().date().strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
                    WHERE file_name in {files_string}

            """


    #found Queries
    q_file_id = """
                SELECT id
                FROM uploaded_files
                WHERE file_name IN {files_string}
            """

    q_module_table_name = """
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
                        """

    q_get_table_name = """updatesys.tbl_{man_id}_{module_id}@new3_n"""

    q_found_status = """
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

    q_found_status_alternate = """
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


    q_get_all_files_data = """select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.* ,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id  )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name """
    q_get_specific_files_data = """select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.* ,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string} )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name """

    q_download_all_files = """select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.* ,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name """
    q_download_specific_files_data = """select par.*, md.man_id, md.module_id, md.WDA_FLAG from (select x.*,y.file_name , y.UPLOAD_DATE, y.LAST_CHECK_DATE, y.STOP_MONITOR_DATE from parts x join uploaded_files y on x.file_id = y.id where y.file_name in {files_string} )par join updatesys.tbl_man_modules@new3_n md on par.module = md.module_name"""
# Ensure the upload folder exists
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
