My changes----
public class AIB_AP_OverrideReassignExtension {

    public String comment{get;set;}
    private final ProcessInstanceWorkitem processInstanceWorkItem;
    ApexPages.standardController stdController = null;
    private String originalActorId;
    private String newActorId;
    private String utpId;
    private String sicrId;
    public Boolean isDisabled{get;set;}
    private String SICR_UTPLINEMANAGERAPPROVAL = 'AIB_CM_SICR_UTPLineManagerApproval';

    /**********************************************************************************************
* @Author:      Mouli Paul
* @Date:        08/02/2022
* @Description: This is a constructor method
* @Revision(s): [Date] - [Change Reference] - [Changed By] - [Description] 
***********************************************************************************************/ 
    public AIB_AP_OverrideReassignExtension(ApexPages.StandardController stdController) {
        this.stdController = stdController;
        this.processInstanceWorkItem = (ProcessInstanceWorkItem)stdController.getRecord();
        originalActorId = this.processInstanceWorkItem.ActorId;
        isDisabled = False;
    }

    /**********************************************************************************************
* @Author:      Mouli Paul
* @Date:        08/02/2022
* @Description:  This method will reassign the application
* @Revision(s): [Date] - [Change Reference] - [Changed By] - [Description] 
29/3/2022 - NCS-2042 - Kritika Agrawal - Stopping users to re-assign to themselves  
****************************************************************************************************/ 
    public PageReference save() {
        try{
            isDisabled = True;
            newActorId = this.processInstanceWorkItem.ActorId;

            List<AIB_UTPInformation__c> utpInfoList = new List<AIB_UTPInformation__c>();
            List<AIB_SICRInformation__c> sicrInfoList = new List<AIB_SICRInformation__c>();

            Map<Id,User> userMap = new Map<Id,User>(AIB_AP_OverrideReassignExtensionProvider.fetchUserInfo(userInfo.getUserId(),newActorId));

            PermissionSetAssignment[] permissionSetList = AIB_AP_OverrideReassignExtensionProvider.fetchPermissionSet(SICR_UTPLINEMANAGERAPPROVAL,newActorId );

            List<processInstanceWorkItem> processInstanceWorkItemList = AIB_AP_OverrideReassignExtensionProvider.fetchprocessInstanceWorkItemList(this.processInstanceWorkItem.Id);

            if(processInstanceWorkItemList!= null && !processInstanceWorkItemList.isEmpty()){
            if(Label.AIB_CM_UTP_Object_API_Name.equalsIgnoreCase(processInstanceWorkItemList[0].ProcessInstance.TargetObjectId.getSobjectType().toString())){
               utpId = processInstanceWorkItemList[0].ProcessInstance.TargetObjectId;
               sicrId = processInstanceWorkItemList[0].ProcessInstance.TargetObjectId;
               }
               utpInfoList = AIB_SICR_RetryCalloutComponentProvider.getUTPDetails(utpId);
            }
            if(string.isNotBlank(utpId) && (permissionSetList==null || permissionSetList.isEmpty() )){
                return processError(Label.AIB_CM_ReassignPermissionUserUTPError);
            }
            if((newActorId == userInfo.getUserId() && string.isNotBlank(utpId)) || (newActorId == utpInfoList[0].CreatedById && string.isNotBlank(utpId))){
                return processError(Label.AIB_CM_ReassignUTPError);
            }
            if(newActorId == userInfo.getUserId() && string.isBlank(utpId)){
                return processError(Label.AIB_AP_ReassignError); //NCS-2042
            }
            if(userMap.get(userInfo.getUserId()).AIB_IDL__c == true && userMap.get(newActorId).AIB_IDL__c == false){//i: Replace false with actual condition when this is true then error should be shown.
                return processError(Label.AIB_AP_IDLError);
            }
            else if(userMap.get(userInfo.getUserId()).AIB_DCA__c == true && userMap.get(newActorId).AIB_DCA__c == false){//i: Replace false with actual condition when this is true then error should be shown.
                return processError(Label.AIB_AP_DCAError);
            }
            else{
                stdController.save();
                ProcessInstance[] processInstances = AIB_AP_OverrideReassignExtensionProvider.fetchProcessInstance(processInstanceWorkItemList[0].ProcessInstance.TargetObjectId);

                    if(processInstances.size()>0 && processInstances.get(0).StepsAndWorkitems.size()>0 && !utpInfoList.isEmpty()){
                    utpInfoList[0].AIB_CM_ApprovalStatus__c = AIB_Constants.STATUS_REASSIGNED;
                    update utpInfoList;
                    isDisabled = False;
                  }
                if(processInstances.size()>0 && processInstances.get(0).StepsAndWorkitems.size()>0 && Label.AIB_CM_Application_Object_API_Name.equalsIgnoreCase(processInstanceWorkItemList[0].ProcessInstance.TargetObjectId.getSobjectType().toString()))
                {
                    AIB_OtherDetails__c otherRec = new AIB_OtherDetails__c(
                        AIB_ProcessInstanceStep__c = processInstances.get(0).StepsAndWorkitems.get(0).Id, 
                        AIB_Comments__c = comment, 
                        RecordTypeId = Schema.SObjectType.AIB_OtherDetails__c.getRecordTypeInfosByName()
                        .get(AIB_Constants.APPROVAL_REASSIGN_RECORD_TYPE).getRecordTypeId(),
                        AIB_Application__c = processInstanceWorkItemList[0].ProcessInstance.TargetObjectId);

                    insert otherRec;
                    isDisabled = False;
                  }
                isDisabled = False;
                return new PageReference('/'+processInstances.get(0).StepsAndWorkitems.get(0).Id);
            }            
        }
        catch(exception exp){
            AIB_GenericLogger.log(AIB_AP_OverrideReassignExtension.class.getName(), 
                                  AIB_Constants.CONSTANT_METHOD_NAME_SAVE, 
                                  exp);
            return processError(Label.AIB_BK_ErrorExceptionOccurred);
        }
    } 

    /**********************************************************************************************
* @Author:      Mouli Paul
* @Date:        08/02/2022
* @Description:  This is a cancel method
* @Revision(s): [Date] - [Change Reference] - [Changed By] - [Description] 
***********************************************************************************************/ 
    public PageReference cancel() {
        return stdController.cancel();
    }

    /**********************************************************************************************
* @Author:      Mouli Paul
* @Date:        08/02/2022
* @Description:  This method will show the error messages
* @Revision(s): [Date] - [Change Reference] - [Changed By] - [Description] 
***********************************************************************************************/ 
    public PageReference processError(String err) {
        ApexPages.Message myMsg = new ApexPages.Message(ApexPages.Severity.ERROR,err);
        ApexPages.addMessage(myMsg);
        isDisabled = False;
        return null;
    }
}

My changes----
koi no---rouhj step 3---
final

 scv
Let's split the process into different steps: one for creating the temporary tables and one for inserting data into them. This will help maintain clarity and manage transformations and rollup separately.

### Step 1: Create the First Temporary Table (`VT_JOINED_DATA`)

```sql
CREATE VOLATILE TABLE VT_JOINED_DATA (
    SCV_ID VARCHAR(20),
    CUST_TITLE VARCHAR(20),
    FIRST_NAME VARCHAR(30),
    LAST_NAME VARCHAR(100),
    PREV_SURNAME_TXT VARCHAR(20),
    SFX VARCHAR(10),
    PREFERRED_LANGUAGE VARCHAR(7),
    DEPOSITOR_TYP_DESCR VARCHAR(20),
    DOB DATE,
    ADDR_LINE_1_TXT VARCHAR(30),
    ADDR_LINE_2_TXT VARCHAR(30),
    ADDR_LINE_3_TXT VARCHAR(30),
    ADDR_LINE_4_TXT VARCHAR(30),
    ZIP_CDE VARCHAR(10),
    CNTRY_NAME VARCHAR(30),
    HOME_PH_AREA DECIMAL(7, 0),
    HOME_PH_NO DECIMAL(8, 0),
    BUS_PH_AREA DECIMAL(7, 0),
    BUS_PH_NO DECIMAL(8, 0),
    CONT_PH_AREA DECIMAL(7, 0),
    CONT_PH_NO DECIMAL(8, 0),
    HOME_EMAIL_ID VARCHAR(75),
    WORK_EMAIL_ID VARCHAR(75),
    DEP_HLDR_BAL_EURO_AMT DECIMAL(17, 2)
) ON COMMIT PRESERVE ROWS;
```

### Step 2: Insert Data into the First Temporary Table (`VT_JOINED_DATA`)

```sql
INSERT INTO VT_JOINED_DATA
SELECT
    A.SCV_ID,
    A.CUST_TITLE,
    A.FIRST_NAME,
    A.LAST_NAME,
    A.PREV_SURNAME_TXT,
    A.SFX,
    A.PREFERRED_LANGUAGE,
    A.DEPOSITOR_TYP_DESCR,
    A.DOB,
    CASE 
        WHEN A.ADDR_LINE_1_TXT IS NULL OR TRIM(A.ADDR_LINE_1_TXT) = '' THEN 'UNKNOWN' 
        ELSE A.ADDR_LINE_1_TXT 
    END AS ADDR_LINE_1_TXT,
    CASE 
        WHEN A.ADDR_LINE_2_TXT IS NULL OR TRIM(A.ADDR_LINE_2_TXT) = '' THEN 'UNKNOWN' 
        ELSE A.ADDR_LINE_2_TXT 
    END AS ADDR_LINE_2_TXT,
    CASE 
        WHEN A.ADDR_LINE_3_TXT IS NULL OR TRIM(A.ADDR_LINE_3_TXT) = '' THEN '' 
        ELSE A.ADDR_LINE_3_TXT 
    END AS ADDR_LINE_3_TXT,
    CASE 
        WHEN A.ADDR_LINE_4_TXT IS NOT NULL AND A.ADDR_LINE_5_TXT IS NOT NULL AND TRIM(A.ADDR_LINE_4_TXT) <> '' AND TRIM(A.ADDR_LINE_5_TXT) <> '' 
        THEN TRIM(A.ADDR_LINE_4_TXT) || ', ' || TRIM(A.ADDR_LINE_5_TXT)
        WHEN A.ADDR_LINE_4_TXT IS NOT NULL AND TRIM(A.ADDR_LINE_4_TXT) <> '' 
        THEN A.ADDR_LINE_4_TXT
        WHEN A.ADDR_LINE_5_TXT IS NOT NULL AND TRIM(A.ADDR_LINE_5_TXT) <> '' 
        THEN A.ADDR_LINE_5_TXT
        ELSE ''
    END AS ADDR_LINE_4_TXT,
    A.ZIP_CDE,
    CASE 
        WHEN A.CNTRY_NAME IS NULL THEN 'UNKNOWN'
        ELSE A.CNTRY_NAME
    END AS CNTRY_NAME,
    A.HOME_PH_AREA,
    A.HOME_PH_NO,
    A.BUS_PH_AREA,
    A.BUS_PH_NO,
    A.CONT_PH_AREA,
    A.CONT_PH_NO,
    A.HOME_EMAIL_ID,
    A.WORK_EMAIL_ID,
    B.DEP_HLDR_BAL_EURO_AMT
FROM
    DEDW50P.DGS_CUSTOMER_DETAIL A
JOIN
    DEDW50P.DGS_SCV_DEPOSITS B
ON
    A.SCV_ID = B.SCV_ID;
```

### Step 3: Create the Second Temporary Table (`VT_PROCESSED_DATA`)

This step will apply the necessary transformations, including phone number formatting.

```sql
CREATE VOLATILE TABLE VT_PROCESSED_DATA (
    SCV_ID VARCHAR(20),
    NAME_PREFIX_TXT VARCHAR(10),
    FORENAME1_TXT VARCHAR(50),
    FORENAME2_TXT VARCHAR(50),
    FORENAME3_TXT VARCHAR(50),
    SURNAME_TXT VARCHAR(100),
    PREV_SURNAME_TXT VARCHAR(20),
    NAME_SUFFIX_TXT VARCHAR(20),
    PREFERRED_LANGUAGE VARCHAR(50),
    DEPOSITOR_TYP_DESCR VARCHAR(50),
    DOB DATE,
    ADDR_LINE_1_TXT VARCHAR(50),
    ADDR_LINE_2_TXT VARCHAR(50),
    ADDR_LINE_3_TXT VARCHAR(50),
    ADDR_LINE_4_TXT VARCHAR(50),
    POST_CDE VARCHAR(50),
    CNTRY_NAME VARCHAR(30),
    PH1_NO VARCHAR(30),
    PH2_NO VARCHAR(30),
    PH3_NO VARCHAR(30),
    EMAIL_ADDRESS1_TXT VARCHAR(50),
    EMAIL_ADDRESS2_TXT VARCHAR(50),
    FIT_STP_IND CHAR(1),
    AGGREGATE_BAL_AMT DECIMAL(15,2),
    COMPENSATABLE_AMT DECIMAL(15,2),
    SCV_CNT INTEGER,
    SRCE_SYS SMALLINT,
    SRCE_INST SMALLINT,
    NAME_LAST_ACTION CHAR(1),
    LOAD_DATE DATE,
    LOAD_TIME INTEGER
) ON COMMIT PRESERVE ROWS;
```

### Step 4: Insert Data into the Second Temporary Table (`VT_PROCESSED_DATA`)

```sql
INSERT INTO VT_PROCESSED_DATA
SELECT
    SCV_ID,
    CUST_TITLE AS NAME_PREFIX_TXT,
    -- Extract FORENAME1_TXT
    CASE 
        WHEN FIRST_NAME IS NULL OR TRIM(FIRST_NAME) = '' THEN ''
        ELSE
            CASE
                WHEN POSITION('-' IN FIRST_NAME) > 0 THEN SUBSTRING(FIRST_NAME FROM 1 FOR POSITION('-' IN FIRST_NAME) - 1)
                WHEN POSITION(' ' IN FIRST_NAME) > 0 THEN SUBSTRING(FIRST_NAME FROM 1 FOR POSITION(' ' IN FIRST_NAME) - 1)
                ELSE FIRST_NAME
            END
    END AS FORENAME1_TXT,
    -- Extract FORENAME2_TXT
    CASE 
        WHEN FIRST_NAME IS NULL OR TRIM(FIRST_NAME) = '' THEN ''
        ELSE
            CASE
                WHEN POSITION('-' IN FIRST_NAME) > 0 THEN
                    CASE
                        WHEN POSITION('-' IN SUBSTRING(FIRST_NAME FROM POSITION('-' IN FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(SUBSTRING(FIRST_NAME FROM POSITION('-' IN FIRST_NAME) + 1) FROM 1 FOR POSITION('-' IN SUBSTRING(FIRST_NAME FROM POSITION('-' IN FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(FIRST_NAME FROM POSITION('-' IN FIRST_NAME) + 1)
                    END
                WHEN POSITION(' ' IN FIRST_NAME) > 0 THEN
                    CASE
                        WHEN POSITION(' ' IN SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1) FROM 1 FOR POSITION(' ' IN SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1)
                    END
                ELSE ''
            END
    END AS FORENAME2_TXT,
    -- Extract FORENAME3_TXT
    CASE 
        WHEN FIRST_NAME IS NULL OR TRIM(FIRST_NAME) = '' THEN ''
        ELSE
            CASE 
                WHEN POSITION(' ' IN FIRST_NAME) > 0 
                THEN
                    CASE
                        WHEN POSITION(' ' IN SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1) FROM 1 FOR POSITION(' ' IN SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(FIRST_NAME FROM POSITION(' ' IN FIRST_NAME) + 1)
                    END
                ELSE ''
            END
    END AS FORENAME3_TXT,
    LAST_NAME AS SURNAME_TXT,
    PREV_SURNAME_TXT,
    SFX AS NAME_SUFFIX_TXT,
    PREFERRED_LANGUAGE,
    DEPOSITOR_TYP_DESCR,
    DOB,
    ADDR_LINE_1_TXT,
    ADDR_LINE_2_TXT,
    ADDR_LINE_3_TXT,
    ADDR_LINE_4_TXT,
    ZIP_CDE AS POST_CDE,
    CNTRY_NAME,
    -- Phone Numbers
    CASE 
        WHEN HOME_PH_AREA = 0 AND HOME_PH_NO = 0 THEN ''
        WHEN CNTRY_NAME = 'IRELAND' OR HOME_PH_AREA IN (83, 85, 86, 87, 89) 
        THEN '0' || TRIM(CAST(HOME_PH_AREA AS VARCHAR(7))) || TRIM(CAST(HOME_PH_NO AS VARCHAR(8)))
        ELSE TRIM(CAST(HOME_PH_AREA AS VARCHAR(7))) || TRIM(CAST(HOME_PH_NO AS VARCHAR(8)))
    END AS PH1_NO,
    CASE 
        WHEN BUS_PH_AREA = 0 AND BUS_PH_NO = 0 THEN ''
        WHEN CNTRY_NAME = 'IRELAND' OR BUS_PH_AREA IN (83, 85, 86, 87, 89) 
        THEN '0' || TRIM(CAST(BUS_PH_AREA AS VARCHAR(7))) || TRIM(CAST(BUS_PH_NO AS VARCHAR(8)))
        ELSE TRIM(CAST(BUS_PH_AREA AS VARCHAR(7))) || TRIM(CAST(BUS_PH_NO AS VARCHAR(8)))
    END AS PH2_NO,
    CASE 
        WHEN CONT_PH_AREA = 0 AND CONT_PH_NO = 0 THEN ''
        WHEN CNTRY_NAME = 'IRELAND' OR CONT_PH_AREA IN (83, 85, 86, 87, 89) 
        THEN '0' || TRIM(CAST(CONT_PH_AREA AS VARCHAR(7))) || TRIM(CAST(CONT_PH_NO AS VARCHAR(8)))
        ELSE TRIM(CAST(CONT_PH_AREA AS VARCHAR(7))) || TRIM(CAST(CONT_PH_NO AS VARCHAR(8)))
    END AS PH3_NO,
    HOME_EMAIL_ID AS EMAIL_ADDRESS1_TXT,
    WORK_EMAIL_ID AS EMAIL_ADDRESS2_TXT,
    '' AS FIT_STP_IND,
    DEP_HLDR_BAL_EURO_AMT AS AGGREGATE_BAL_AMT,
    0 AS COMPENSATABLE_AMT,
    0 AS SCV_CNT,
    68 AS SRCE_SYS,
    1 AS SRCE_INST,
    'I' AS NAME_LAST_ACTION,
    CURRENT_DATE AS LOAD_DATE,
    CAST(CURRENT_TIME(0) AS INTEGER) AS LOAD_TIME
FROM
    VT_JOINED_DATA;
```

### Step 5: Insert Data into the Final Table (`DGS_SCV_CUSTOMER`) with Rollup

```sql
INSERT INTO DEDW50P.DGS_SCV_CUSTOMER
SELECT
    SCV_ID,
    NAME_PREFIX_TXT,
    FORENAME1_TXT,
    FORENAME2_TXT,
    FORENAME3_TXT,
    SURNAME_TXT,
    PREV_SURNAME_TXT,
    NAME_SUFFIX_TXT,
    PREFERRED_LANGUAGE,
    DEPOSITOR_TYP_DESCR,
    DOB,
    ADDR_LINE_1_TXT,
    ADDR_LINE_2_TXT,
    ADDR_LINE_3_TXT,
    ADDR_LINE_4_TXT,
    POST_CDE,
    CNTRY_NAME,
    PH1_NO,
    PH2_NO,
    PH3_NO,
    EMAIL_ADDRESS1_TXT,
    EMAIL_ADDRESS2_TXT,
    FIT_STP_IND,
    SUM(AGGREGATE_BAL_AMT) AS AGGREGATE_BAL_AMT,
    CASE 
        WHEN SUM(AGGREGATE_BAL_AMT) > 100000 THEN 100000
        ELSE SUM(AGGREGATE_BAL_AMT)
    END AS COMPENSATABLE_AMT,
    COUNT(SCV_ID) AS SCV_CNT,
    SRCE_SYS,
    SRCE_INST,
    NAME_LAST_ACTION,
    LOAD_DATE,
    LOAD_TIME
FROM
    VT_PROCESSED_DATA
GROUP BY
    SCV_ID,
    NAME_PREFIX_TXT,
    FORENAME1_TXT,
    FORENAME2_TXT,
    FORENAME3_TXT,
    SURNAME_TXT,
    PREV_SURNAME_TXT,
    NAME_SUFFIX_TXT,
    PREFERRED_LANGUAGE,
    DEPOSITOR_TYP_DESCR,
    DOB,
    ADDR_LINE_1_TXT,
    ADDR_LINE_2_TXT,
    ADDR_LINE_3_TXT,
    ADDR_LINE_4_TXT,
    POST_CDE,
    CNTRY_NAME,
    PH1_NO,
    PH2_NO,
    PH3_NO,
    EMAIL_ADDRESS1_TXT,
    EMAIL_ADDRESS2_TXT,
    FIT_STP_IND,
    SRCE_SYS,
    SRCE_INST,
    NAME_LAST_ACTION,
    LOAD_DATE,
    LOAD_TIME;
```

This approach breaks down the process into manageable steps:

1. **Create and insert data into the first temporary table (`VT_JOINED_DATA`)** with initial transformations.
2. **Create and insert data into the second temporary table (`VT_PROCESSED_DATA`)** with additional transformations, especially for phone numbers.
3. **Insert data into the final table (`DGS_SCV_CUSTOMER`)** while performing the rollup (aggregation). 

This method ensures clarity and aligns with the structure you provided.
INSERT INTO DEDW50P.DGS_SCV_CUSTOMER
SELECT 
    SCV_ID,
    CUST_TITLE,
    FIRST_NAME,
    LAST_NAME,
    PREV_SURNAME_TXT,
    SFX,
    PREFERRED_LANGUAGE,
    DEPOSITOR_TYP_DESCR,
    DOB,
    ADDR_LINE_1_TXT,
    ADDR_LINE_2_TXT,
    ADDR_LINE_3_TXT,
    ADDR_LINE_4_TXT,
    ZIP_CDE,
    CNTRY_NAME,
    -- Phone Numbers Transformation
    CASE 
        WHEN HOME_PH_AREA = 0 AND HOME_PH_NO = 0 THEN ''
        WHEN CNTRY_NAME = 'IRELAND' OR HOME_PH_AREA IN (83, 85, 86, 87, 89) 
        THEN '0' || TRIM(CAST(HOME_PH_AREA AS VARCHAR(7))) || TRIM(CAST(HOME_PH_NO AS VARCHAR(8)))
        ELSE TRIM(CAST(HOME_PH_AREA AS VARCHAR(7))) || TRIM(CAST(HOME_PH_NO AS VARCHAR(8)))
    END AS PH1_NO,
    CASE 
        WHEN BUS_PH_AREA = 0 AND BUS_PH_NO = 0 THEN ''
        WHEN CNTRY_NAME = 'IRELAND' OR BUS_PH_AREA IN (83, 85, 86, 87, 89) 
        THEN '0' || TRIM(CAST(BUS_PH_AREA AS VARCHAR(7))) || TRIM(CAST(BUS_PH_NO AS VARCHAR(8)))
        ELSE TRIM(CAST(BUS_PH_AREA AS VARCHAR(7))) || TRIM(CAST(BUS_PH_NO AS VARCHAR(8)))
    END AS PH2_NO,
    CASE 
        WHEN CONT_PH_AREA = 0 AND CONT_PH_NO = 0 THEN ''
        WHEN CNTRY_NAME = 'IRELAND' OR CONT_PH_AREA IN (83, 85, 86, 87, 89) 
        THEN '0' || TRIM(CAST(CONT_PH_AREA AS VARCHAR(7))) || TRIM(CAST(CONT_PH_NO AS VARCHAR(8)))
        ELSE TRIM(CAST(CONT_PH_AREA AS VARCHAR(7))) || TRIM(CAST(CONT_PH_NO AS VARCHAR(8)))
    END AS PH3_NO,
    HOME_EMAIL_ID,
    WORK_EMAIL_ID,
    '' AS FIT_STP_IND,
    SUM(DEP_HLDR_BAL_EURO_AMT) AS AGGREGATE_BAL_AMT,
    CASE 
        WHEN SUM(DEP_HLDR_BAL_EURO_AMT) > 100000 THEN 100000
        ELSE SUM(DEP_HLDR_BAL_EURO_AMT)
    END AS COMPENSATABLE_AMT,
    COUNT(SCV_ID) AS SCV_CNT
FROM (
    SELECT
        A.SCV_ID,
        A.CUST_TITLE,
        A.FIRST_NAME,
        A.LAST_NAME,
        A.PREV_SURNAME_TXT,
        A.SFX,
        A.PREFERRED_LANGUAGE,
        A.DEPOSITOR_TYP_DESCR,
        A.DOB,
        CASE WHEN A.ADDR_LINE_1_TXT IS NULL OR TRIM(A.ADDR_LINE_1_TXT) = '' THEN 'UNKNOWN' ELSE A.ADDR_LINE_1_TXT END AS ADDR_LINE_1_TXT,
        CASE WHEN A.ADDR_LINE_2_TXT IS NULL OR TRIM(A.ADDR_LINE_2_TXT) = '' THEN 'UNKNOWN' ELSE A.ADDR_LINE_2_TXT END AS ADDR_LINE_2_TXT,
        CASE WHEN A.ADDR_LINE_3_TXT IS NULL OR TRIM(A.ADDR_LINE_3_TXT) = '' THEN '' ELSE A.ADDR_LINE_3_TXT END AS ADDR_LINE_3_TXT,
        CASE 
            WHEN A.ADDR_LINE_4_TXT IS NOT NULL AND A.ADDR_LINE_5_TXT IS NOT NULL AND TRIM(A.ADDR_LINE_4_TXT) <> '' AND TRIM(A.ADDR_LINE_5_TXT) <> '' 
            THEN TRIM(A.ADDR_LINE_4_TXT) || ', ' || TRIM(A.ADDR_LINE_5_TXT)
            WHEN A.ADDR_LINE_4_TXT IS NOT NULL AND TRIM(A.ADDR_LINE_4_TXT) <> '' 
            THEN A.ADDR_LINE_4_TXT
            WHEN A.ADDR_LINE_5_TXT IS NOT NULL AND TRIM(A.ADDR_LINE_5_TXT) <> '' 
            THEN A.ADDR_LINE_5_TXT
            ELSE ''
        END AS ADDR_LINE_4_TXT,
        A.ZIP_CDE,
        CASE WHEN A.CNTRY_NAME IS NOT NULL THEN A.CNTRY_NAME ELSE 'UNKNOWN' END AS CNTRY_NAME,
        A.HOME_PH_AREA,
        A.HOME_PH_NO,
        A.BUS_PH_AREA,
        A.BUS_PH_NO,
        A.CONT_PH_AREA,
        A.CONT_PH_NO,
        A.HOME_EMAIL_ID,
        A.WORK_EMAIL_ID,
        B.DEP_HLDR_BAL_EURO_AMT
    FROM
        DEDW50P.DGS_CUSTOMER_DETAIL A
    JOIN
        DEDW50P.DGS_SCV_DEPOSITS B
    ON
        A.SCV_ID = B.SCV_ID
) GROUP BY
    SCV_ID,
    CUST_TITLE,
    FIRST_NAME,
    LAST_NAME,
    PREV_SURNAME_TXT,
    SFX,
    PREFERRED_LANGUAGE,
    DEPOSITOR_TYP_DESCR,
    DOB,
    ADDR_LINE_1_TXT,
    ADDR_LINE_2_TXT,
    ADDR_LINE_3_TXT,
    ADDR_LINE_4_TXT,
    ZIP_CDE,
    CNTRY_NAME,
    HOME_PH_AREA,
    HOME_PH_NO,
    BUS_PH_AREA,
    BUS_PH_NO,
    CONT_PH_AREA,
    CONT_PH_NO,
    HOME_EMAIL_ID,
    WORK_EMAIL_ID;
--
SELECT 
    A.SCV_ID,
    A.CUST_TITLE AS NAME_PREFIX_TXT,
    -- Your other columns here...
    CASE 
        WHEN A.FIRST_NAME IS NULL OR TRIM(A.FIRST_NAME) = '' THEN ''
        ELSE
            CASE 
                WHEN POSITION(' ' IN A.FIRST_NAME) > 0 
                THEN
                    CASE
                        WHEN POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1 FOR POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)
                    END
                ELSE ''
            END
    END AS FORENAME3_TXT,
    -- Logic for forename1_txt and forename2_txt
    CASE 
        WHEN A.FIRST_NAME IS NULL OR TRIM(A.FIRST_NAME) = '' THEN ''
        ELSE
            CASE
                WHEN POSITION('-' IN A.FIRST_NAME) > 0 THEN SUBSTRING(A.FIRST_NAME FROM 1 FOR POSITION('-' IN A.FIRST_NAME) - 1)
                WHEN POSITION(' ' IN A.FIRST_NAME) > 0 THEN SUBSTRING(A.FIRST_NAME FROM 1 FOR POSITION(' ' IN A.FIRST_NAME) - 1)
                ELSE A.FIRST_NAME
            END
    END AS FORENAME1_TXT,
    CASE 
        WHEN A.FIRST_NAME IS NULL OR TRIM(A.FIRST_NAME) = '' THEN ''
        ELSE
            CASE
                WHEN POSITION('-' IN A.FIRST_NAME) > 0 THEN
                    CASE
                        WHEN POSITION('-' IN SUBSTRING(A.FIRST_NAME FROM POSITION('-' IN A.FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(SUBSTRING(A.FIRST_NAME FROM POSITION('-' IN A.FIRST_NAME) + 1) FROM 1 FOR POSITION('-' IN SUBSTRING(A.FIRST_NAME FROM POSITION('-' IN A.FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(A.FIRST_NAME FROM POSITION('-' IN A.FIRST_NAME) + 1)
                    END
                WHEN POSITION(' ' IN A.FIRST_NAME) > 0 THEN
                    CASE
                        WHEN POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1) FROM 1 FOR POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)
                    END
                ELSE ''
            END
    END AS FORENAME2_TXT
FROM 
    DEDW50P.DGS_CUSTOMER_DETAIL A
INNER JOIN 
    DEDW50P.DGS_SCV_DEPOSITS B 
    ON A.SCV_ID = B.SCV_ID;

INSERT INTO temp_transformed_data
SELECT 
    TRIM(a.wh_acc_no) AS wh_acc_no,
    TRIM(a.acc_no) AS acc_no,
    TRIM(b.scv_id) AS scv_id,
    CASE 
        WHEN TRIM(a.designation_1_txt) IS NULL AND TRIM(a.designation_2_txt) IS NULL AND TRIM(a.short_name) IS NOT NULL THEN TRIM(a.short_name)
        WHEN TRIM(a.designation_1_txt) IS NOT NULL OR TRIM(a.designation_2_txt) IS NOT NULL THEN TRIM(a.designation_1_txt) || ' ' || TRIM(a.designation_2_txt)
        WHEN a.srce_sys_descr = 'CALYPSO-CM' AND TRIM(a.short_name) IS NOT NULL THEN TRIM(a.short_name)
        ELSE TRIM(a.acc_no)
    END AS dep_title_txt,
    TRIM(a.bic_cde) AS bic_cde,
    TRIM(a.iban_no) AS iban_no,
    TRIM(a.br_language_descr) AS br_language_descr,
    CASE 
        WHEN TRIM(a.prod_grp_descr) IS NOT NULL THEN TRIM(a.prod_grp_descr)
        ELSE TRIM(a.prod_descr)
    END AS prod_typ,
    a.dgs_cust_owner_cnt AS dep_hldr_tot_cnt,
    b.dgs_acc_owner_ind AS dep_hldr_ind,
    CASE 
        WHEN SUBSTRING(TRIM(b.acc_owner_bal_split_rte) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(b.acc_owner_bal_split_rte) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(b.acc_owner_bal_split_rte)
        ELSE TRIM(b.acc_owner_bal_split_rte)
    END AS dep_hldr_bal_split_rte,
    CASE 
        WHEN SUBSTRING(TRIM(b.split_euro_bal_amt) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(b.split_euro_bal_amt) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(b.split_euro_bal_amt)
        ELSE TRIM(b.split_euro_bal_amt)
    END AS dep_hldr_bal_euro_amt,
    CASE 
        WHEN c.stp_ind = 1 THEN 'STP'
        WHEN c.ben_ind = 1 THEN 'BEN'
        WHEN c.dec_ind = 1 THEN 'DEC'
        WHEN c.ga_ind = 1 THEN 'GA'
        WHEN c.brp_ind = 1 THEN 'BRP'
        WHEN c.frd_ind = 1 THEN 'FRD'
        WHEN c.mlo_ind = 1 THEN 'MLO'
        WHEN c.san_ind = 1 THEN 'SAN'
        WHEN c.ldis_ind = 1 THEN 'LDIS'
        WHEN c.blk_ind = 1 THEN 'BLK'
        ELSE NULL
    END AS dep_sta_1_cde,
    CASE
        WHEN c.stp_ind = 1 THEN NULL
        ELSE COALESCE(
            CASE WHEN c.ben_ind = 1 AND dep_sta_1_cde != 'BEN' THEN 'BEN' ELSE NULL END,
            CASE WHEN c.dec_ind = 1 AND dep_sta_1_cde != 'DEC' THEN 'DEC' ELSE NULL END,
            CASE WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' THEN 'GA' ELSE NULL END,
            CASE WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' THEN 'BRP' ELSE NULL END,
            CASE WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' THEN 'FRD' ELSE NULL END,
            CASE WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' THEN 'MLO' ELSE NULL END,
            CASE WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' THEN 'SAN' ELSE NULL END,
            CASE WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' THEN 'LDIS' ELSE NULL END,
            CASE WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' THEN 'BLK' ELSE NULL END
        )
    END AS dep_sta_2_cde,
    CASE 
        WHEN dep_sta_1_cde = 'STP' THEN NULL
        WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' AND dep_sta_2_cde != 'GA' THEN 'GA'
        WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' AND dep_sta_2_cde != 'BRP' THEN 'BRP'
        WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' AND dep_sta_2_cde != 'FRD' THEN 'FRD'
        WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' AND dep_sta_2_cde != 'MLO' THEN 'MLO'
        WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' AND dep_sta_2_cde != 'SAN' THEN 'SAN'
        WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' AND dep_sta_2_cde != 'LDIS' THEN 'LDIS'
        WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' AND dep_sta_2_cde != 'BLK' THEN 'BLK'
        ELSE NULL
    END AS dep_sta_3_cde,
    a.euro_bal_amt AS acc_bal_euro_amt,
    a.euro_accrd_int_amt AS accrd_int_euro_amt,
    a.orig_crncy_bal_amt AS acc_bal_orig_crncy_amt,
    a.orig_crncy_accrd_int_amt AS accrd_int_orig_crncy_amt,
    TRIM(a.iso_crncy_cde) AS orig_crncy_cde,
    CASE 
        WHEN SUBSTRING(TRIM(a.sap_fx_daily_rte) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(a.sap_fx_daily_rte) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(a.sap_fx_daily_rte)
        ELSE TRIM(a.sap_fx_daily_rte)
    END AS exch_rte,
    TRIM(b.split_euro_bal_amt) AS balance,
    36 AS srce_sys,
    1 AS srce_inst,
    CURRENT_DATE AS load_date,
    CAST(CURRENT_TIME AS FORMAT 'HH:MI:SS') AS load_time
FROM temp_formatted_dgs_account_detail a
JOIN temp_formatted_dgs_customer_account_rel b ON a.wh_acc_no = b.wh_acc_no
JOIN temp_formatted_dgs_deposit_status_detail c ON b.scv_id = c.scv_id
JOIN temp_dgs_scv_master_customer d ON c.scv_id = d.scv_id
WHERE (a.beneficiary_acc_ind = 'Y' OR b.dgs_excl_ind = 'N') AND b.split_euro_bal_amt >= 0.01;

---end--
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

final ___


### Step 1: Create Temporary Tables

```sql
-- Create temporary table for Formatted DGS Account Detail
CREATE MULTISET VOLATILE TABLE temp_formatted_dgs_account_detail (
    wh_acc_no CHAR(16),
    acc_no CHAR(16),
    bic_cde VARCHAR(35),
    iban_no VARCHAR(30),
    br_language_descr CHAR(7),
    prod_descr VARCHAR(50),
    prod_grp_descr VARCHAR(50),
    dgs_cust_owner_cnt INTEGER,
    euro_bal_amt DECIMAL(15,2),
    orig_crncy_bal_amt DECIMAL(15,2),
    euro_accrd_int_amt DECIMAL(15,2),
    orig_crncy_accrd_int_amt DECIMAL(15,2),
    iso_crncy_cde CHAR(3),
    sap_fx_daily_rte DECIMAL(10,5),
    short_name VARCHAR(50),
    designation_1_txt VARCHAR(50),
    designation_2_txt VARCHAR(50),
    beneficiary_acc_ind CHAR(1),
    srce_sys_descr VARCHAR(12),
    iso_cntry_cde CHAR(4)
) PRIMARY INDEX (wh_acc_no) ON COMMIT PRESERVE ROWS;

-- Create temporary table for Formatted DGS Customer Account Rel
CREATE MULTISET VOLATILE TABLE temp_formatted_dgs_customer_account_rel (
    wh_acc_no CHAR(16),
    acc_no CHAR(20),
    scv_id VARCHAR(20),
    wh_cust_no INTEGER,
    srce_cust_no VARCHAR(19),
    dgs_acc_owner_ind DECIMAL(8,3),
    acc_owner_bal_split_rte DECIMAL(15,2),
    dgs_excl_ind CHAR(1),
    split_euro_bal_amt DECIMAL(15,2)
) PRIMARY INDEX (wh_acc_no) ON COMMIT PRESERVE ROWS;

-- Create temporary table for Formatted DGS Deposit Status Detail
CREATE MULTISET VOLATILE TABLE temp_formatted_dgs_deposit_status_detail (
    scv_id VARCHAR(20),
    stp_ind INTEGER,
    ben_ind INTEGER,
    dec_ind INTEGER,
    ga_ind INTEGER,
    brp_ind INTEGER,
    frd_ind INTEGER,
    mlo_ind INTEGER,
    san_ind INTEGER,
    ldis_ind INTEGER,
    blk_ind INTEGER
) PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;

-- Create temporary table for DGS SCV Master Customer
CREATE MULTISET VOLATILE TABLE temp_dgs_scv_master_customer (
    scv_id VARCHAR(20),
    customer_data VARCHAR(100) -- Example column, add all necessary columns
) PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;

-- Create temporary table for storing intermediate transformed data
CREATE MULTISET VOLATILE TABLE temp_transformed_data (
    wh_acc_no CHAR(16),
    acc_no CHAR(16),
    scv_id VARCHAR(20),
    dep_title_txt VARCHAR(50),
    bic_cde VARCHAR(35),
    iban_no VARCHAR(30),
    br_language_descr CHAR(7),
    prod_typ VARCHAR(50),
    dep_hldr_tot_cnt INTEGER,
    dep_hldr_ind INTEGER,
    dep_hldr_bal_split_rte DECIMAL(6,3),
    dep_hldr_bal_euro_amt DECIMAL(15,2),
    dep_sta_1_cde VARCHAR(4),
    dep_sta_2_cde VARCHAR(4),
    dep_sta_3_cde VARCHAR(4),
    acc_bal_euro_amt DECIMAL(15,2),
    accrd_int_euro_amt DECIMAL(15,2),
    acc_bal_orig_crncy_amt DECIMAL(15,2),
    accrd_int_orig_crncy_amt DECIMAL(15,2),
    orig_crncy_cde VARCHAR(3),
    exch_rte DECIMAL(10,5),
    balance DECIMAL(15,2),
    srce_sys SMALLINT,
    srce_inst SMALLINT,
    load_date CHAR(10),
    load_time CHAR(10)
) PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;

-- Create temporary table for sorted and deduplicated data
CREATE MULTISET VOLATILE TABLE temp_sorted_deduped_data (
    wh_acc_no CHAR(16),
    acc_no CHAR(16),
    scv_id VARCHAR(20),
    dep_title_txt VARCHAR(50),
    bic_cde VARCHAR(35),
    iban_no VARCHAR(30),
    br_language_descr CHAR(7),
    prod_typ VARCHAR(50),
    dep_hldr_tot_cnt INTEGER,
    dep_hldr_ind INTEGER,
    dep_hldr_bal_split_rte DECIMAL(6,3),
    dep_hldr_bal_euro_amt DECIMAL(15,2),
    dep_sta_1_cde VARCHAR(4),
    dep_sta_2_cde VARCHAR(4),
    dep_sta_3_cde VARCHAR(4),
    acc_bal_euro_amt DECIMAL(15,2),
    accrd_int_euro_amt DECIMAL(15,2),
    acc_bal_orig_crncy_amt DECIMAL(15,2),
    accrd_int_orig_crncy_amt DECIMAL(15,2),
    orig_crncy_cde VARCHAR(3),
    exch_rte DECIMAL(10,5),
    balance DECIMAL(15,2),
    srce_sys SMALLINT,
    srce_inst SMALLINT,
    load_date CHAR(10),
    load_time CHAR(10)
) PRIMARY INDEX (scv_id) ON COMMIT PRESERVE ROWS;
```

### Step 2: Load Data into Temporary Tables

```sql
-- Load data into temp_formatted_dgs_account_detail with trimming
INSERT INTO temp_formatted_dgs_account_detail
SELECT 
    TRIM(wh_acc_no),
    TRIM(acc_no),
    TRIM(bic_cde),
    TRIM(iban_no),
    TRIM(br_language_descr),
    TRIM(prod_descr),
    TRIM(prod_grp_descr),
    dgs_cust_owner_cnt,
    euro_bal_amt,
    orig_crncy_bal_amt,
    euro_accrd_int_amt,
    orig_crncy_accrd_int_amt,
    TRIM(iso_crncy_cde),
    sap_fx_daily_rte,
    TRIM(short_name),
    TRIM(designation_1_txt),
    TRIM(designation_2_txt),
    TRIM(beneficiary_acc_ind),
    TRIM(srce_sys_descr),
    TRIM(iso_cntry_cde)
FROM dgs_account_detail;

-- Load data into temp_formatted_dgs_customer_account_rel with trimming
INSERT INTO temp_formatted_dgs_customer_account_rel
SELECT 
    TRIM(wh_acc_no),
    TRIM(acc_no),
    TRIM(scv_id),
    wh_cust_no,
    TRIM(srce_cust_no),
    dgs_acc_owner_ind,
    acc_owner_bal_split_rte,
    TRIM(dgs_excl_ind),
    split_euro_bal_amt
FROM dgs_customer_account_rel;

-- Load data into temp_formatted_dgs_deposit_status_detail with trimming
INSERT INTO temp_formatted_dgs_deposit_status_detail
SELECT 
    TRIM(scv_id),
    stp_ind,
    ben_ind,
    dec_ind,
    ga_ind,
    brp_ind,
    frd_ind,
    mlo_ind,
    san_ind,
    ldis_ind,
    blk_ind
FROM dgs_deposit_status_detail;

-- Load data into temp_dgs_scv_master_customer with trimming
INSERT INTO temp_dgs_scv_master_customer
SELECT 
    TRIM(scv_id),
    TRIM(customer_data) -- Add all necessary columns
FROM dgs_scv_master_customer;
```

### Step 3: Transform Data and Store in Intermediate Temporary Table

```sql
-- Insert transformed data into intermediate temporary table
INSERT INTO temp_transformed_data
SELECT 
    a.wh_acc_no,
    a.acc_no,
    b.scv_id,
    CASE 
        WHEN a.designation_1_txt IS NULL AND a.designation_2_txt IS NULL AND a.short_name IS NOT NULL THEN a.short_name
        WHEN a.designation_1_txt IS NOT NULL OR a.designation_2_txt IS NOT NULL THEN TRIM(BOTH ' ' FROM (a.designation_1_txt || ' ' || a.designation_2_txt))
        WHEN a.srce_sys_descr = 'CALYPSO-CM' AND a.short_name IS NOT NULL THEN a.short_name
        ELSE a.acc_no
    END AS dep_title_txt,
    a.bic_cde,
    a.iban_no,
    a.br_language_descr,
    CASE 
        WHEN a.prod_grp_descr IS NOT NULL THEN a.prod_grp_descr
        ELSE a.prod_descr
    END AS prod_typ,
    a.dgs_cust_owner_cnt AS dep_hldr_tot_cnt,
    b.dgs_acc_owner_ind AS dep_hldr_ind,
    CASE 
        WHEN SUBSTRING(b.acc_owner_bal_split_rte FROM 1 FOR 1) = '.' OR SUBSTRING(b.acc_owner_bal_split_rte FROM 1 FOR 2) = '-.' THEN '0' || b.acc_owner_bal_split_rte
        ELSE b.acc_owner_bal_split_rte
    END AS dep_hldr_bal_split_rte,
    CASE 
        WHEN SUBSTRING(b.split_euro_bal_amt FROM 1 FOR 1) = '.' OR SUBSTRING(b.split_euro_bal_amt FROM 1 FOR 2) = '-.' THEN '0' || b.split_euro_bal_amt
        ELSE b.split_euro_bal_amt
    END AS dep_hldr_bal_euro_amt,
    CASE 
        WHEN c.stp_ind = 1 THEN 'STP'
        WHEN c.ben_ind = 1 THEN 'BEN'
        WHEN c.dec_ind = 1 THEN 'DEC'
        WHEN c.ga_ind = 1 THEN 'GA'
        WHEN c.brp_ind = 1 THEN 'BRP'
        WHEN c.frd_ind = 1 THEN 'FRD'
        WHEN c.mlo_ind = 1 THEN 'MLO'
        WHEN c.san_ind = 1 THEN 'SAN'
        WHEN c.ldis_ind = 1 THEN 'LDIS'
        WHEN c.blk_ind = 1 THEN 'BLK'
        ELSE NULL
    END AS dep_sta_1_cde,
    CASE 
        WHEN dep_sta_1_cde = 'STP' THEN NULL
        WHEN c.ben_ind = 1 AND dep_sta_1_cde != 'BEN' THEN 'BEN'
        WHEN c.dec_ind = 1 AND dep_sta_1_cde != 'DEC' THEN 'DEC'
        WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' THEN 'GA'
        WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' THEN 'BRP'
        WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' THEN 'FRD'
        WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' THEN 'MLO'
        WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' THEN 'SAN'
        WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' THEN 'LDIS'
        WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' THEN 'BLK'
        ELSE NULL
    END AS dep_sta_2_cde,
    CASE 
        WHEN dep_sta_1_cde = 'STP' THEN NULL
        WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' AND dep_sta_2_cde != 'GA' THEN 'GA'
        WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' AND dep_sta_2_cde != 'BRP' THEN 'BRP'
        WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' AND dep_sta_2_cde != 'FRD' THEN 'FRD'
        WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' AND dep_sta_2_cde != 'MLO' THEN 'MLO'
        WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' AND dep_sta_2_cde != 'SAN' THEN 'SAN'
        WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' AND dep_sta_2_cde != 'LDIS' THEN 'LDIS'
        WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' AND dep_sta_2_cde != 'BLK' THEN 'BLK'
        ELSE NULL
    END AS dep_sta_3_cde,
    a.euro_bal_amt AS acc_bal_euro_amt,
    a.euro_accrd_int_amt AS accrd_int_euro_amt,
    a.orig_crncy_bal_amt AS acc_bal_orig_crncy_amt,
    a.orig_crncy_accrd_int_amt AS accrd_int_orig_crncy_amt,
    a.iso_crncy_cde AS orig_crncy_cde,
    CASE 
        WHEN SUBSTRING(a.sap_fx_daily_rte FROM 1 FOR 1) = '.' OR SUBSTRING(a.sap_fx_daily_rte FROM 1 FOR 2) = '-.' THEN '0' || a.sap_fx_daily_rte
        ELSE a.sap_fx_daily_rte
    END AS exch_rte,
    b.split_euro_bal_amt AS balance,
    36 AS srce_sys,
    1 AS srce_inst,
    CAST(CURRENT_DATE AS CHAR(10)) AS load_date,
    CAST(CURRENT_TIME AS CHAR(10)) AS load_time
FROM temp_formatted_dgs_account_detail a
JOIN temp_formatted_dgs_customer_account_rel b 
ON a.wh_acc_no = b.wh_acc_no
JOIN temp_formatted_dgs_deposit_status_detail c
ON b.scv_id = c.scv_id
JOIN temp_dgs_scv_master_customer d
ON c.scv_id = d.scv_id
WHERE ((a.beneficiary_acc_ind = 'Y') OR (b.dgs_excl_ind = 'N')) AND b.split_euro_bal_amt >= 0.01;
```

### Step 4: Sort and Deduplicate Data

```sql
-- Insert sorted and deduplicated data into temporary table
INSERT INTO temp_sorted_deduped_data
SELECT 
    wh_acc_no,
    acc_no,
    scv_id,
    dep_title_txt,
    bic_cde,
    iban_no,
    br_language_descr,
    prod_typ,
    dep_hldr_tot_cnt,
    dep_hldr_ind,
    dep_hldr_bal_split_rte,
    dep_hldr_bal_euro_amt,
    dep_sta_1_cde,
    dep_sta_2_cde,
    dep_sta_3_cde,
    acc_bal_euro_amt,
    accrd_int_euro_amt,
    acc_bal_orig_crncy_amt,
    accrd_int_orig_crncy_amt,
    orig_crncy_cde,
    exch_rte,
    balance,
    srce_sys,
    srce_inst,
    load_date,
    load_time
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY scv_id, wh_acc_no, acc_no, dep_title_txt, bic_cde, 
                           iban_no, br_language_descr, prod_typ, dep_hldr_tot_cnt, 
                           dep_hldr_ind, dep_hldr_bal_split_rte, dep_hldr_bal_euro_amt, 
                           dep_sta_1_cde, dep_sta_2_cde, dep_sta_3_cde, acc_bal_euro_amt, 
                           accrd_int_euro_amt, acc_bal_orig_crncy_amt, accrd_int_orig_crncy_amt, 
                           orig_crncy_cde, exch_rte, balance) AS row_num
    FROM temp_transformed_data
) AS sub
WHERE row_num = 1;
```

### Step 5: Insert Sorted and Deduplicated Data into Final Table

```sql
-- Insert sorted and deduplicated data into the final table
INSERT INTO dgs_scv_deposits
SELECT 
    wh_acc_no,
    acc_no,
    scv_id,
    dep_title_txt,
    bic_cde,
    iban_no,
    br_language_descr,
    prod_typ,
    dep_hldr_tot_cnt,
    dep_hldr_ind,
    dep_hldr_bal_split_rte,
    dep_hldr_bal_euro_amt,
    dep_sta_1_cde,
    dep_sta_2_cde,
    dep_sta_3_cde,
    acc_bal_euro_amt,
    accrd_int_euro_amt,
    acc_bal_orig_crncy_amt,
    accrd_int_orig_crncy_amt,
    orig_crncy_cde,
    exch_rte,
    balance,
    srce_sys,
    srce_inst,
    load_date,
    load_time
FROM temp_sorted_deduped_data;
```

### Step 6: Drop Temporary Tables

```sql
-- Drop temporary tables
DROP TABLE temp_formatted_dgs_account_detail;
DROP TABLE temp_formatted_dgs_customer_account_rel;
DROP TABLE temp_formatted_dgs_deposit_status_detail;
DROP TABLE temp_dgs_scv_master_customer;
DROP TABLE temp_transformed_data;
DROP TABLE temp_sorted_deduped_data;
```

This code follows the required process steps, ensures that input data is trimmed for tables starting with "Formatted", and uses separate `CREATE` and `INSERT` statements for temporary tables. If you have any further requirements or adjustments, please let me know.

updated step 3

INSERT INTO temp_transformed_data
SELECT 
    TRIM(a.wh_acc_no) AS wh_acc_no,
    TRIM(a.acc_no) AS acc_no,
    TRIM(b.scv_id) AS scv_id,
    CASE 
        WHEN TRIM(a.designation_1_txt) IS NULL AND TRIM(a.designation_2_txt) IS NULL AND TRIM(a.short_name) IS NOT NULL THEN TRIM(a.short_name)
        WHEN TRIM(a.designation_1_txt) IS NOT NULL OR TRIM(a.designation_2_txt) IS NOT NULL THEN TRIM(a.designation_1_txt) || ' ' || TRIM(a.designation_2_txt)
        WHEN a.srce_sys_descr = 'CALYPSO-CM' AND TRIM(a.short_name) IS NOT NULL THEN TRIM(a.short_name)
        ELSE TRIM(a.acc_no)
    END AS dep_title_txt,
    TRIM(a.bic_cde) AS bic_cde,
    TRIM(a.iban_no) AS iban_no,
    TRIM(a.br_language_descr) AS br_language_descr,
    CASE 
        WHEN TRIM(a.prod_grp_descr) IS NOT NULL THEN TRIM(a.prod_grp_descr)
        ELSE TRIM(a.prod_descr)
    END AS prod_typ,
    a.dgs_cust_owner_cnt AS dep_hldr_tot_cnt,
    b.dgs_acc_owner_ind AS dep_hldr_ind,
    CASE 
        WHEN SUBSTRING(TRIM(b.acc_owner_bal_split_rte) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(b.acc_owner_bal_split_rte) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(b.acc_owner_bal_split_rte)
        ELSE TRIM(b.acc_owner_bal_split_rte)
    END AS dep_hldr_bal_split_rte,
    CASE 
        WHEN SUBSTRING(TRIM(b.split_euro_bal_amt) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(b.split_euro_bal_amt) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(b.split_euro_bal_amt)
        ELSE TRIM(b.split_euro_bal_amt)
    END AS dep_hldr_bal_euro_amt,
    CASE 
        WHEN c.stp_ind = 1 THEN 'STP'
        WHEN c.ben_ind = 1 THEN 'BEN'
        WHEN c.dec_ind = 1 THEN 'DEC'
        WHEN c.ga_ind = 1 THEN 'GA'
        WHEN c.brp_ind = 1 THEN 'BRP'
        WHEN c.frd_ind = 1 THEN 'FRD'
        WHEN c.mlo_ind = 1 THEN 'MLO'
        WHEN c.san_ind = 1 THEN 'SAN'
        WHEN c.ldis_ind = 1 THEN 'LDIS'
        WHEN c.blk_ind = 1 THEN 'BLK'
        ELSE NULL
    END AS dep_sta_1_cde,
    CASE 
        WHEN dep_sta_1_cde = 'STP' THEN NULL
        ELSE COALESCE(
            CASE WHEN c.ben_ind = 1 AND dep_sta_1_cde != 'BEN' THEN 'BEN' ELSE NULL END,
            CASE WHEN c.dec_ind = 1 AND dep_sta_1_cde != 'DEC' THEN 'DEC' ELSE NULL END,
            CASE WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' THEN 'GA' ELSE NULL END,
            CASE WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' THEN 'BRP' ELSE NULL END,
            CASE WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' THEN 'FRD' ELSE NULL END,
            CASE WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' THEN 'MLO' ELSE NULL END,
            CASE WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' THEN 'SAN' ELSE NULL END,
            CASE WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' THEN 'LDIS' ELSE NULL END,
            CASE WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' THEN 'BLK' ELSE NULL END
        )
    END AS dep_sta_2_cde,
    CASE 
        WHEN dep_sta_1_cde = 'STP' THEN NULL
        WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' AND dep_sta_2_cde != 'GA' THEN 'GA'
        WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' AND dep_sta_2_cde != 'BRP' THEN 'BRP'
        WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' AND dep_sta_2_cde != 'FRD' THEN 'FRD'
        WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' AND dep_sta_2_cde != 'MLO' THEN 'MLO'
        WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' AND dep_sta_2_cde != 'SAN' THEN 'SAN'
        WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' AND dep_sta_2_cde != 'LDIS' THEN 'LDIS'
        WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' AND dep_sta_2_cde != 'BLK' THEN 'BLK'
        ELSE NULL
    END AS dep_sta_3_cde,
    a.euro_bal_amt AS acc_bal_euro_amt,
    a.euro_accrd_int_amt AS accrd_int_euro_amt,
    a.orig_crncy_bal_amt AS acc_bal_orig_crncy_amt,
    a.orig_crncy_accrd_int_amt AS accrd_int_orig_crncy_amt,
    TRIM(a.iso_crncy_cde) AS orig_crncy_cde,
    CASE 
        WHEN SUBSTRING(TRIM(a.sap_fx_daily_rte) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(a.sap_fx_daily_rte) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(a.sap_fx_daily_rte)
        ELSE TRIM(a.sap_fx_daily_rte)
    END AS exch_rte,
    TRIM(b.split_euro_bal_amt) AS balance,
    36 AS srce_sys,
    1 AS srce_inst,
    CURRENT_DATE AS load_date,
    CAST(CURRENT_TIME AS FORMAT 'HH:MI:SS') AS load_time
FROM temp_formatted_dgs_account_detail a
JOIN temp_formatted_dgs_customer_account_rel b ON a.wh_acc_no = b.wh_acc_no
JOIN temp_formatted_dgs_deposit_status_detail c ON b.scv_id = c.scv_id
JOIN temp_dgs_scv_master_customer d ON c.scv_id = d.scv_id
WHERE (a.beneficiary_acc_ind = 'Y' OR b.dgs_excl_ind = 'N') AND b.split_euro_bal_amt >= 0.01;

____alll in one--
INSERT INTO final_dgs_scv_deposits
SELECT 
    wh_acc_no,
    acc_no,
    scv_id,
    dep_title_txt,
    bic_cde,
    account_number,
    iban_no,
    br_language_descr,
    prod_typ,
    dep_hldr_tot_cnt,
    dep_hldr_ind,
    dep_hldr_bal_split_rte,
    dep_hldr_bal_euro_amt,
    dep_sta_1_cde,
    dep_sta_2_cde,
    dep_sta_3_cde,
    acc_bal_euro_amt,
    accrd_int_euro_amt,
    acc_bal_orig_crncy_amt,
    accrd_int_orig_crncy_amt,
    orig_crncy_cde,
    exch_rte,
    srce_sys,
    srce_inst,
    CURRENT_DATE AS load_date,
    CAST(CURRENT_TIME AS TIME(0)) AS load_time
FROM (
    SELECT 
        a.wh_acc_no,
        a.acc_no,
        b.scv_id,
        CASE 
            WHEN TRIM(a.designation_1_txt) IS NULL AND TRIM(a.designation_2_txt) IS NULL AND TRIM(a.short_name) IS NOT NULL THEN TRIM(a.short_name) 
            WHEN TRIM(a.designation_1_txt) IS NOT NULL OR TRIM(a.designation_2_txt) IS NOT NULL THEN TRIM(a.designation_1_txt) || ' ' || TRIM(a.designation_2_txt)
            WHEN a.srce_sys_descr = 'CALYPSO-CM' AND TRIM(a.short_name) IS NOT NULL THEN TRIM(a.short_name)
            ELSE TRIM(a.acc_no) 
        END AS dep_title_txt,
        TRIM(a.bic_cde) AS bic_cde,
        TRIM(a.acc_no) AS account_number,
        TRIM(a.iban_no) AS iban_no,
        TRIM(a.br_language_descr) AS br_language_descr,
        CASE 
            WHEN TRIM(a.prod_grp_descr) IS NOT NULL THEN TRIM(a.prod_grp_descr) 
            ELSE TRIM(a.prod_descr) 
        END AS prod_typ,
        a.dgs_cust_owner_cnt AS dep_hldr_tot_cnt,
        b.dgs_acc_owner_ind AS dep_hldr_ind,
        CASE 
            WHEN SUBSTRING(TRIM(b.acc_owner_bal_split_rte) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(b.acc_owner_bal_split_rte) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(b.acc_owner_bal_split_rte)
            ELSE TRIM(b.acc_owner_bal_split_rte)
        END AS dep_hldr_bal_split_rte,
        CASE 
            WHEN SUBSTRING(TRIM(b.split_euro_bal_amt) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(b.split_euro_bal_amt) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(b.split_euro_bal_amt)
            ELSE TRIM(b.split_euro_bal_amt)
        END AS dep_hldr_bal_euro_amt,
        CASE 
            WHEN c.stp_ind = 1 THEN 'STP'
            WHEN c.ben_ind = 1 THEN 'BEN'
            WHEN c.dec_ind = 1 THEN 'DEC'
            WHEN c.ga_ind = 1 THEN 'GA'
            WHEN c.brp_ind = 1 THEN 'BRP'
            WHEN c.frd_ind = 1 THEN 'FRD'
            WHEN c.mlo_ind = 1 THEN 'MLO'
            WHEN c.san_ind = 1 THEN 'SAN'
            WHEN c.ldis_ind = 1 THEN 'LDIS'
            WHEN c.blk_ind = 1 THEN 'BLK'
            ELSE NULL
        END AS dep_sta_1_cde,
        CASE 
            WHEN dep_sta_1_cde = 'STP' THEN NULL
            ELSE COALESCE(
                CASE WHEN c.ben_ind = 1 AND dep_sta_1_cde != 'BEN' THEN 'BEN' ELSE NULL END,
                CASE WHEN c.dec_ind = 1 AND dep_sta_1_cde != 'DEC' THEN 'DEC' ELSE NULL END,
                CASE WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' THEN 'GA' ELSE NULL END,
                CASE WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' THEN 'BRP' ELSE NULL END,
                CASE WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' THEN 'FRD' ELSE NULL END,
                CASE WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' THEN 'MLO' ELSE NULL END,
                CASE WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' THEN 'SAN' ELSE NULL END,
                CASE WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' THEN 'LDIS' ELSE NULL END,
                CASE WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' THEN 'BLK' ELSE NULL END
            )
        END AS dep_sta_2_cde,
        CASE 
            WHEN dep_sta_1_cde = 'STP' THEN NULL
            WHEN c.ga_ind = 1 AND dep_sta_1_cde != 'GA' AND dep_sta_2_cde != 'GA' THEN 'GA'
            WHEN c.brp_ind = 1 AND dep_sta_1_cde != 'BRP' AND dep_sta_2_cde != 'BRP' THEN 'BRP'
            WHEN c.frd_ind = 1 AND dep_sta_1_cde != 'FRD' AND dep_sta_2_cde != 'FRD' THEN 'FRD'
            WHEN c.mlo_ind = 1 AND dep_sta_1_cde != 'MLO' AND dep_sta_2_cde != 'MLO' THEN 'MLO'
            WHEN c.san_ind = 1 AND dep_sta_1_cde != 'SAN' AND dep_sta_2_cde != 'SAN' THEN 'SAN'
            WHEN c.ldis_ind = 1 AND dep_sta_1_cde != 'LDIS' AND dep_sta_2_cde != 'LDIS' THEN 'LDIS'
            WHEN c.blk_ind = 1 AND dep_sta_1_cde != 'BLK' AND dep_sta_2_cde != 'BLK' THEN 'BLK'
            ELSE NULL
        END AS dep_sta_3_cde,
        a.euro_bal_amt AS acc_bal_euro_amt,
        a.euro_accrd_int_amt AS accrd_int_euro_amt,
        a.orig_crncy_bal_amt AS acc_bal_orig_crncy_amt,
        a.orig_crncy_accrd_int_amt AS accrd_int_orig_crncy_amt,
        TRIM(a.iso_crncy_cde) AS orig_crncy_cde,
        CASE 
            WHEN SUBSTRING(TRIM(a.sap_fx_daily_rte) FROM 1 FOR 1) = '.' OR SUBSTRING(TRIM(a.sap_fx_daily_rte) FROM 1 FOR 2) = '-.' THEN '0' || TRIM(a.sap_fx_daily_rte)
            ELSE TRIM(a.sap_fx_daily_rte)
        END AS exch_rte,
        36 AS srce_sys,
         AS srce_inst,
        CURRENT_DATE AS load_date,
        CAST(CURRENT_TIME AS TIME(0)) AS load_time,
        ROW_NUMBER() OVER (
            PARTITION BY scv_id, a.wh_acc_no, a.acc_no, dep_title_txt, bic_cde, account_number, iban_no, br_language_descr, prod_typ, dep_hldr_tot_cnt, dep_hldr_ind, dep_hldr_bal_split_rte, dep_hldr_bal_euro_amt, dep_sta_1_cde, dep_sta_2_cde, dep_sta_3_cde, acc_bal_euro_amt, accrd_int_euro_amt, acc_bal_orig_crncy_amt, accrd_int_orig_crncy_amt, orig_crncy_cde, exch_rte
            ORDER BY scv_id, a.wh_acc_no, a.acc_no, dep_title_txt, bic_cde, account_number, iban_no, br_language_descr, prod_typ, dep_hldr_tot_cnt, dep_hldr_ind, dep_hldr_bal_split_rte, dep_hldr_bal_euro_amt, dep_sta_1_cde, dep_sta_2_cde, dep_sta_3_cde, acc_bal_euro_amt, accrd_int_euro_amt, acc_bal_orig_crncy_amt, accrd_int_orig_crncy_amt, orig_crncy_cde, exch_rte
        ) AS row_num
    FROM temp_formatted_dgs_account_detail a
    JOIN temp_formatted_dgs_customer_account_rel b ON a.wh_acc_no = b.wh_acc_no
    JOIN temp_formatted_dgs_deposit_status_detail c ON b.scv_id = c.scv_id
    JOIN temp_dgs_scv_master_customer d ON c.scv_id = d.scv_id
    WHERE (a.beneficiary_acc_ind = 'Y' OR b.dgs_excl_ind = 'N') AND b.split_euro_bal_amt >= 0.01
) AS sub
WHERE row_num = 1;
```

This code separates the table creation and the data insertion steps and ensures that all necessary fields are correctly handled and that `ROW_NUMBER()` is used to deduplicate the records before inserting into the final table.

cor
SELECT 
    A.SCV_ID,
    A.CUST_TITLE AS NAME_PREFIX_TXT,
    -- Your other columns here...
    CASE 
        WHEN A.FIRST_NAME IS NULL OR TRIM(A.FIRST_NAME) = '' THEN ''
        ELSE
            CASE 
                WHEN POSITION(' ' IN A.FIRST_NAME) > 0 
                THEN
                    CASE
                        WHEN POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) > 0 
                        THEN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1 FOR POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) - 1)
                        ELSE SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)
                    END
                ELSE ''
            END
    END AS FORENAME3_TXT,
    -- Similar logic for forename1_txt and forename2_txt
    CASE 
        WHEN A.FIRST_NAME IS NULL OR TRIM(A.FIRST_NAME) = '' THEN ''
        ELSE
            CASE
                WHEN POSITION('-' IN A.FIRST_NAME) > 0 THEN SUBSTRING(A.FIRST_NAME FROM 1 FOR POSITION('-' IN A.FIRST_NAME) - 1)
                ELSE SUBSTRING(A.FIRST_NAME FROM 1 FOR POSITION(' ' IN A.FIRST_NAME) - 1)
            END
    END AS FORENAME1_TXT,
    CASE 
        WHEN A.FIRST_NAME IS NULL OR TRIM(A.FIRST_NAME) = '' THEN ''
        ELSE
            CASE
                WHEN POSITION('-' IN A.FIRST_NAME) > 0 THEN SUBSTRING(SUBSTRING(A.FIRST_NAME FROM POSITION('-' IN A.FIRST_NAME) + 1) FROM 1 FOR POSITION('-' IN SUBSTRING(A.FIRST_NAME FROM POSITION('-' IN A.FIRST_NAME) + 1)) - 1)
                ELSE SUBSTRING(SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1) FROM 1 FOR POSITION(' ' IN SUBSTRING(A.FIRST_NAME FROM POSITION(' ' IN A.FIRST_NAME) + 1)) - 1)
            END
    END AS FORENAME2_TXT
FROM 
    DEDW50P.DGS_CUSTOMER_DETAIL A
INNER JOIN 
    DEDW50P.DGS_SCV_DEPOSITS B 
    ON A.SCV_ID = B.SCV_ID;



