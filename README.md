CREATE VOLATILE TABLE transformed_data AS (
    SELECT 
        a.wh_acc_no,
        a.acc_no,
        a.scv_id,
        a.bic_cde,
        a.iban_no,
        a.br_language_descr,
        a.prod_descr,
        a.prod_grp_descr,
        a.dgs_cust_owner_cnt,
        a.euro_bal_amt,
        a.euro_accrd_int_amt,
        a.orig_crncy_bal_amt,
        a.orig_crncy_accrd_int_amt,
        a.iso_crncy_cde,
        a.sap_fx_daily_rte,
        a.short_name,
        a.designation_1_txt,
        a.designation_2_txt,
        -- First deposit status code condition
        CASE 
            WHEN b.stp_ind = 1 THEN 'STP'
            WHEN b.ben_ind = 1 THEN 'BEN'
            WHEN b.dec_ind = 1 THEN 'DEC'
            WHEN b.ga_ind = 1 THEN 'GA'
            WHEN b.brp_ind = 1 THEN 'BRP'
            WHEN b.frd_ind = 1 THEN 'FRD'
            WHEN b.mlo_ind = 1 THEN 'MLO'
            WHEN b.san_ind = 1 THEN 'SAN'
            WHEN b.ldis_ind = 1 THEN 'LDIS'
            WHEN b.blk_ind = 1 THEN 'BLK'
            ELSE NULL 
        END AS deposit_status_code1,
        -- Second deposit status code condition
        CASE 
            WHEN b.ben_ind = 1 AND b.stp_ind <> 1 THEN 'BEN'
            WHEN b.dec_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 THEN 'DEC'
            WHEN b.ga_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 THEN 'GA'
            WHEN b.brp_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 AND b.ga_ind <> 1 THEN 'BRP'
            WHEN b.frd_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 AND b.ga_ind <> 1 AND b.brp_ind <> 1 THEN 'FRD'
            WHEN b.mlo_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 AND b.ga_ind <> 1 AND b.brp_ind <> 1 AND b.frd_ind <> 1 THEN 'MLO'
            WHEN b.san_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 AND b.ga_ind <> 1 AND b.brp_ind <> 1 AND b.frd_ind <> 1 AND b.mlo_ind <> 1 THEN 'SAN'
            WHEN b.ldis_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 AND b.ga_ind <> 1 AND b.brp_ind <> 1 AND b.frd_ind <> 1 AND b.mlo_ind <> 1 AND b.san_ind <> 1 THEN 'LDIS'
            WHEN b.blk_ind = 1 AND b.stp_ind <> 1 AND b.ben_ind <> 1 AND b.dec_ind <> 1 AND b.ga_ind <> 1 AND b.brp_ind <> 1 AND b.frd_ind <> 1 AND b.mlo_ind <> 1 AND b.san_ind <> 1 AND b.ldis_ind <> 1 THEN 'BLK'
            ELSE NULL
        END AS deposit_status_code2,
        -- Third deposit status code condition
        CASE 
            WHEN b.ga_ind = 1 AND NOT (b.stp_ind = 1 OR b.ben_ind = 1 OR b.dec_ind = 1 OR b.brp_ind = 1 OR b.frd_ind = 1 OR b.mlo_ind = 1 OR b.san_ind = 1 OR b.ldis_ind = 1 OR b.blk_ind = 1) THEN 'GA'
            WHEN b.brp_ind = 1 AND NOT (b.stp_ind = 1 OR b.ben_ind = 1 OR b.dec_ind = 1 OR b.ga_ind = 1 OR b.frd_ind = 1 OR b.mlo_ind = 1 OR b.san_ind = 1 OR b.ldis_ind = 1 OR b.blk_ind = 1) THEN 'BRP'
            WHEN b.frd_ind = 1 AND NOT (b.stp_ind = 1 OR b.ben_ind = 1 OR b.dec_ind = 1 OR b.ga_ind = 1 OR b.brp_ind = 1 OR b.mlo_ind = 1 OR b.san_ind = 1 OR b.ldis_ind = 1 OR b.blk_ind = 1) THEN 'FRD'
            ELSE NULL
        END AS deposit_status_code3
    FROM v_dgs_account_detail a
    JOIN formatted_dgs_deposit_status_detail b
    ON a.scv_id = b.scv_id
) WITH DATA PRIMARY INDEX (scv_id, wh_acc_no) ON COMMIT PRESERVE ROWS;

------------
After completing Step 2, which involves transforming your data based on multiple conditions, we move on to sorting, deduplication, and final output stages to complete the ETL process. This workflow ensures that the data is ready for further analysis or storage in a more permanent data warehouse system.

Step 3: Sorting
Sorting the data ensures that the records are in a specific order, which can be crucial for the next steps like deduplication and for achieving performance efficiency in database operations.

sql
Copy code
CREATE VOLATILE TABLE sorted_data AS (
    SELECT *
    FROM transformed_data
    ORDER BY scv_id, wh_acc_no, acc_no
) WITH DATA PRIMARY INDEX (scv_id, wh_acc_no, acc_no) ON COMMIT PRESERVE ROWS;
Step 4: Deduplication
Deduplication removes duplicate records based on certain keys, ensuring each record in the output is unique according to the specified attributes.

sql
Copy code
CREATE VOLATILE TABLE deduped_data AS (
    SELECT DISTINCT *
    FROM sorted_data
) WITH DATA PRIMARY INDEX (scv_id, wh_acc_no, acc_no) ON COMMIT PRESERVE ROWS;
Step 5: Replication
This step involves replicating the output to serve multiple purposes, such as feeding into different systems or tables for various business needs. This step may involve additional transformations or simply formatting the data for output.

sql
Copy code
-- Example of splitting output into multiple paths; the actual implementation will depend on your system's capabilities
CREATE VOLATILE TABLE replication_path1 AS (
    SELECT * FROM deduped_data
) WITH DATA PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE replication_path2 AS (
    SELECT * FROM deduped_data
) WITH DATA PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;
Step 6: Reformatting
This step changes the format of your data to fit the requirements of the output or downstream applications. It might involve adding new fields, dropping unnecessary ones, or transforming values.

sql
Copy code
CREATE VOLATILE TABLE reformatted_data AS (
    SELECT 
        scv_id,
        wh_acc_no,
        acc_no,
        deposit_status_code1 AS dep_sta_1_cde,
        deposit_status_code2 AS dep_sta_2_cde,
        deposit_status_code3 AS dep_sta_3_cde,
        short_name AS dep_title_txt,
        bic_cde,
        iban_no,
        br_language_descr,
        CASE WHEN prod_grp_descr IS NOT NULL THEN prod_grp_descr ELSE prod_descr END AS prod_typ,
        euro_bal_amt AS acc_bal_euro_amt,
        orig_crncy_bal_amt AS acc_bal_orig_crncy_amt,
        euro_accrd_int_amt AS accrd_int_euro_amt,
        orig_crncy_accrd_int_amt AS accrd_int_orig_crncy_amt,
        iso_crncy_cde AS orig_crncy_cde,
        sap_fx_daily_rte AS exch_rte,
        '36' AS srce_sys,
        '1' AS srce_inst,
        '\n' AS new_line
    FROM replication_path1
) WITH DATA PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;
Step 7: Output
Finally, the data is output to a file or loaded into an Enterprise Data Warehouse (EDW). This often involves writing to a file system or directly inserting into database tables.

sql
Copy code
-- Writing to an output file (hypothetical command, specific syntax will vary)
EXPORT TO DATA FILE 'output_path/dgs_scv_deposits.dat' 
SELECT * FROM reformatted_data;

-- Inserting into an EDW table
INSERT INTO edw_table
SELECT *
FROM reformatted_data;
This series of SQL statements aims to replicate the ETL process as outlined in your scenario, ensuring data is processed efficiently and is ready for the final storage or analysis steps. Adjust the SQL commands according to the specifics of your system and the tools you are using.
-----------


Step-by-Step SQL for Creating and Inserting Data into Permanent Tables
Step 1: Create Tables for Staging and Processing
These tables will store intermediate results from each stage of the ETL process. If they already exist, you might consider clearing them before inserting new data or appending as necessary based on your use case.

sql
Copy code
-- Assuming database schema 'etl' is used for simplicity.
-- Create or replace table logic could be adapted based on your environment's needs.

-- Staging Tables for Input Data
CREATE MULTISET TABLE etl.stg_account_detail AS (
    SELECT * FROM warehouse.dgs_account_detail
) WITH DATA PRIMARY INDEX (wh_acc_no);

CREATE MULTISET TABLE etl.stg_customer_account_rel AS (
    SELECT * FROM warehouse.dgs_customer_account_rel
) WITH DATA PRIMARY INDEX (wh_acc_no);

CREATE MULTISET TABLE etl.stg_deposit_status_detail AS (
    SELECT * FROM warehouse.dgs_deposit_status_detail
) WITH DATA PRIMARY INDEX (scv_id);

CREATE MULTISET TABLE etl.stg_scv_master_customer AS (
    SELECT * FROM warehouse.scv_master_customer
) WITH DATA PRIMARY INDEX (scv_id);

-- Processing Tables
CREATE MULTISET TABLE etl.joined_data (
    -- Define columns based on output from the join step
) PRIMARY INDEX (wh_acc_no, scv_id);

CREATE MULTISET TABLE etl.transformed_data (
    -- Define columns based on the transformed data including deposit status codes
) PRIMARY INDEX (wh_acc_no, scv_id);

CREATE MULTISET TABLE etl.sorted_data (
    -- Define columns, identical to transformed_data
) PRIMARY INDEX (scv_id, wh_acc_no, acc_no);

CREATE MULTISET TABLE etl.enriched_data (
    -- Define columns, including fields from SCV Master Customer
) PRIMARY INDEX (scv_id);
Step 2: Insert Data Into Staging and Perform Transformations
sql
Copy code
-- Insert data into staging tables
INSERT INTO etl.stg_account_detail SELECT * FROM warehouse.dgs_account_detail;
INSERT INTO etl.stg_customer_account_rel SELECT * FROM warehouse.dgs_customer_account_rel;
INSERT INTO etl.stg_deposit_status_detail SELECT * FROM warehouse.dgs_deposit_status_detail;
INSERT INTO etl.stg_scv_master_customer SELECT * FROM warehouse.scv_master_customer;

-- Perform joins and transformations, inserting into the processing tables
INSERT INTO etl.joined_data
SELECT a.*, b.scv_id, b.dgs_acc_owner_ind, b.acc_owner_bal_split_rte
FROM etl.stg_account_detail a
JOIN etl.stg_customer_account_rel b ON a.wh_acc_no = b.wh_acc_no;

INSERT INTO etl.transformed_data
SELECT 
    j.wh_acc_no,
    j.acc_no,
    j.scv_id,
    j.iso_crncy_cde,
    j.bic_cde,
    j.iban_no,
    j.beneficiary_acc_ind,
    d.deposit_status_code,
    -- Add CASE logic here for status codes based on document specifics
FROM etl.joined_data j
LEFT JOIN etl.stg_deposit_status_detail d ON j.scv_id = d.scv_id;

-- Sorting and deduplication (Assuming sorted_data is a deduplicated and sorted version of transformed_data)
INSERT INTO etl.sorted_data
SELECT DISTINCT * 
FROM etl.transformed_data
ORDER BY scv_id, wh_acc_no, acc_no;
Step 3: Enrich, Finalize and Output
sql
Copy code
-- Enriching data with master customer details
INSERT INTO etl.enriched_data
SELECT d.*, m.customer_details -- Specify actual columns
FROM etl.sorted_data d
LEFT JOIN etl.stg_scv_master_customer m ON d.scv_id = m.scv_id;

-- Final output to another table or export to a file system
INSERT INTO warehouse.final_deposit_table
SELECT *,
       '36' AS src_sys,
       '1' AS src_inst,
       '\n' AS newline
FROM etl.enriched_data;
Notes:
Table Definitions: Ensure that the table definitions (CREATE TABLE) match the expected schema, including all necessary fields and their types.
Indexing: Adjust primary and secondary indexing based on query performance and access patterns.
Data Retention and Management: Consider how data will be managed in these tables over time, including archival or deletion policies.
This method ensures that the ETL process is robust, repeatable, and integrates well with the persistence layers in a Teradata environment. Adjustments might be necessary based on specific requirements or constraints within your actual deployment
--------
F1 UPLOAD MNFR
CREATE TABLE TRANSFORMED_DATA (
    WH_ACC_NO VARCHAR(20),
    ACC_NO VARCHAR(20),
    SCV_ID VARCHAR(20),
    BIC_CDE VARCHAR(20),
    IBAN_NO VARCHAR(20),
    BR_LANGUAGE_DESCR VARCHAR(20),
    PROD_DESCR VARCHAR(50),
    PROD_GRP_DESCR VARCHAR(50),
    DGS_CUST_OWNER_CNT INTEGER,
    EURO_BAL_AMT DECIMAL(18,2),
    EURO_ACCRD_INT_AMT DECIMAL(18,2),
    ORIG_CRNCY_BAL_AMT DECIMAL(18,2),
    ORIG_CRNCY_ACCRD_INT_AMT DECIMAL(18,2),
    ISO_CRNCY_CDE VARCHAR(5),
    SAP_FX_DAILY_RTE DECIMAL(18,9),
    SHORT_NAME VARCHAR(30),
    DESIGNATION_1_TXT VARCHAR(30),
    DESIGNATION_2_TXT VARCHAR(30),
    DEPOSIT_STATUS_CODE1 VARCHAR(10),
    DEPOSIT_STATUS_CODE2 VARCHAR(10),
    DEPOSIT_STATUS_CODE3 VARCHAR(10)
) PRIMARY INDEX (SCV_ID, WH_ACC_NO);

INSERT INTO TRANSFORMED_DATA
SELECT 
    A.WH_ACC_NO,
    A.ACC_NO,
    A.SCV_ID,
    A.BIC_CDE,
    A.IBAN_NO,
    A.BR_LANGUAGE_DESCR,
    A.PROD_DESCR,
    A.PROD_GRP_DESCR,
    A.DGS_CUST_OWNER_CNT,
    A.EURO_BAL_AMT,
    A.EURO_ACCRD_INT_AMT,
    A.ORIG_CRNCY_BAL_AMT,
    A.ORIG_CRNCY_ACCRD_INT_AMT,
    A.ISO_CRNCY_CDE,
    A.SAP_FX_DAILY_RTE,
    A.SHORT_NAME,
    A.DESIGNATION_1_TXT,
    A.DESIGNATION_2_TXT,
    CASE 
        WHEN B.STP_IND = 1 THEN 'STP'
        WHEN B.BEN_IND = 1 THEN 'BEN'
        WHEN B.DEC_IND = 1 THEN 'DEC'
        WHEN B.GA_IND = 1 THEN 'GA'
        WHEN B.BRP_IND = 1 THEN 'BRP'
        WHEN B.FRD_IND = 1 THEN 'FRD'
        WHEN B.MLO_IND = 1 THEN 'MLO'
        WHEN B.SAN_IND = 1 THEN 'SAN'
        WHEN B.LDIS_IND = 1 THEN 'LDIS'
        WHEN B.BLK_IND = 1 THEN 'BLK'
        ELSE NULL 
    END AS DEPOSIT_STATUS_CODE1,
    CASE 
        WHEN B.BEN_IND = 1 AND B.STP_IND <> 1 THEN 'BEN'
        ELSE NULL
    END AS DEPOSIT_STATUS_CODE2,
    CASE 
        WHEN B.GA_IND = 1 AND B.STP_IND <> 1 THEN 'GA'
        ELSE NULL
    END AS DEPOSIT_STATUS_CODE3
FROM V_DGS_ACCOUNT_DETAIL A
JOIN FORMATTED_DGS_DEPOSIT_STATUS_DETAIL B ON A.SCV_ID = B.SCV_ID;

CREATE TABLE SORTED_DATA AS (
    WH_ACC_NO VARCHAR(20),
    ACC_NO VARCHAR(20),
    SCV_ID VARCHAR(20),
    BIC_CDE VARCHAR(20),
    IBAN_NO VARCHAR(20),
    BR_LANGUAGE_DESCR VARCHAR(50),
    PROD_DESCR VARCHAR(50),
    PROD_GRP_DESCR VARCHAR(50),
    DGS_CUST_OWNER_CNT INTEGER,
    EURO_BAL_AMT DECIMAL(18,2),
    EURO_ACCRD_INT_AMT DECIMAL(18,2),
    ORIG_CRNCY_BAL_AMT DECIMAL(18,2),
    ORIG_CRNCY_ACCRD_INT_AMT DECIMAL(18,2),
    ISO_CRNCY_CDE VARCHAR(5),
    SAP_FX_DAILY_RTE DECIMAL(18,9),
    SHORT_NAME VARCHAR(30),
    DESIGNATION_1_TXT VARCHAR(30),
    DESIGNATION_2_TXT VARCHAR(30),
    DEPOSIT_STATUS_CODE1 VARCHAR(10),
    DEPOSIT_STATUS_CODE2 VARCHAR(10),
    DEPOSIT_STATUS_CODE3 VARCHAR(10),
    ROW_NUMBER INTEGER
)  PRIMARY INDEX (SCV_ID, WH_ACC_NO);

INSERT INTO SORTED_DATA
SELECT DISTINCT
    WH_ACC_NO,
    ACC_NO,
    SCV_ID,
    BIC_CDE,
    IBAN_NO,
    BR_LANGUAGE_DESCR,
    PROD_DESCR,
    PROD_GRP_DESCR,
    DGS_CUST_OWNER_CNT,
    EURO_BAL_AMT,
    EURO_ACCRD_INT_AMT,
    ORIG_CRNCY_BAL_AMT,
    ORIG_CRNCY_ACCRD_INT_AMT,
    ISO_CRNCY_CDE,
    SAP_FX_DAILY_RTE,
    SHORT_NAME,
    DESIGNATION_1_TXT,
    DESIGNATION_2_TXT,
    DEPOSIT_STATUS_CODE1,
    DEPOSIT_STATUS_CODE2,
    DEPOSIT_STATUS_CODE3,
    ROW_NUMBER() OVER (PARTITION BY SCV_ID ORDER BY WH_ACC_NO, ACC_NO) AS ROW_NUMBER
FROM TRANSFORMED_DATA
QUALIFY ROW_NUMBER = 1;

CREATE TABLE DEDWMSOP.DGS_SCV_DEPOSITS (
    WH_ACC_NO INTEGER,
    ACC_NO CHAR(16),
    SCV_ID VARCHAR(20),
    BIC_CDE VARCHAR(15),
    IBAN_NO VARCHAR(30),
    BR_LANGUAGE_DESCR VARCHAR(30),
    PROD_DESCR VARCHAR(50),
    PROD_GRP_DESCR VARCHAR(50),
    DGS_CUST_OWNER_CNT INTEGER,
    EURO_BAL_AMT DECIMAL(15,2),
    EURO_ACCRD_INT_AMT DECIMAL(15,2),
    ORIG_CRNCY_BAL_AMT DECIMAL(15,2),
    ORIG_CRNCY_ACCRD_INT_AMT DECIMAL(15,2),
    ISO_CRNCY_CDE CHAR(3),
    SAP_FX_DAILY_RTE DECIMAL(10,5),
    SHORT_NAME VARCHAR(20),
    DESIGNATION_1_TXT VARCHAR(30),
    DESIGNATION_2_TXT VARCHAR(30),
    DEP_STATUS_CODE1 VARCHAR(4),
    DEP_STATUS_CODE2 VARCHAR(4),
    DEP_STATUS_CODE3 VARCHAR(4),
    ACC_BAL_EURO_AMT DECIMAL(15,2),
    ACCRD_INT_EURO_AMT DECIMAL(15,2),
    ACC_BAL_ORIG_CRNCY_AMT DECIMAL(15,2),
    ACCRD_INT_ORIG_CRNCY_AMT DECIMAL(15,2),
    EXCH_RTE DECIMAL(10,5),
    SRC_SYS SMALLINT,
    BREE_ID VARCHAR(8),
    LOAD_DATE DATE FORMAT 'YYYY-MM-DD',
    LOAD_LAST_ACTN CHAR(1)
) PRIMARY INDEX (WH_ACC_NO);

INSERT INTO DEDWMSOP.DGS_SCV_DEPOSITS
SELECT
    A.WH_ACC_NO,
    A.ACC_NO,
    B.SCV_ID AS SCV_ID,
    A.BIC_CDE,
    A.IBAN_NO,
    A.BR_LANGUAGE_DESCR,
    A.PROD_DESCR,
    A.PROD_GRP_DESCR,
    A.DGS_CUST_OWNER_CNT,
    A.EURO_BAL_AMT,
    A.EURO_ACCRD_INT_AMT,
    A.ORIG_CRNCY_BAL_AMT,
    A.ORIG_CRNCY_ACCRD_INT_AMT,
    A.ISO_CRNCY_CDE,
    A.SAP_FX_DAILY_RTE,
    A.SHORT_NAME,
    A.DESIGNATION_1_TXT,
    A.DESIGNATION_2_TXT,
    B.DEPOSIT_STATUS_CODE1,
    B.DEPOSIT_STATUS_CODE2,
    B.DEPOSIT_STATUS_CODE3,
    A.ACC_BAL_EURO_AMT,
    A.ACCRD_INT_EURO_AMT,
    A.ACC_BAL_ORIG_CRNCY_AMT,
    A.ACCRD_INT_ORIG_CRNCY_AMT,
    A.EXCH_RTE,
    B.SRC_SYS,
    B.BREE_ID,
    CURRENT_DATE AS LOAD_DATE,
    'I' AS LOAD_LAST_ACTN
FROM
    SORTED_DATA A 
JOIN
   SCV_MASTER_CUSTOMER B ON A.SCV_ID = B.SCV_ID
WHERE
    A.WH_ACC_NO IS NOT NULL;

