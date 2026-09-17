-- PREPROD SSMS test: run this whole file in one query window.
-- This executes the query directly. It does not create/alter a procedure or view.
-- NULL account array = all frequency-eligible accounts, including inactive.
-- Keep customer numbers as quoted strings to preserve leading zeros.
USE [CLMAA_SpecialAccounts];
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;

-- Edit these inputs before executing (start with one account):
DECLARE @CustomerNumbersJson nvarchar(max) = N'["1521764304"]';
DECLARE @PolicyEffectiveDateFrom date = NULL;
DECLARE @CompareCurrentView bit = 0;
-- Set comparison to 1 only for active accounts with a NULL cutoff.
-- Comparison runs the old slow view too; its time is reported separately.
-- Date example: '20040101'. NULL preserves the existing six-year cutoff.
-- Multiple accounts example: N'["1514748014", "0033165294"]'.

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Allows rerunning the batch in the same SSMS session after cancellation.
DROP TABLE IF EXISTS #FinancialFeatures;
DROP TABLE IF EXISTS #ClaimNumbers;
DROP TABLE IF EXISTS #Claims;
DROP TABLE IF EXISTS #Policies;
DROP TABLE IF EXISTS #Customers;
DROP TABLE IF EXISTS #LossRunResult;
DROP TABLE IF EXISTS #BaselineResult;

BEGIN TRY
IF @CompareCurrentView = 1 AND @PolicyEffectiveDateFrom IS NOT NULL
    THROW 50004, 'Compare with the current view using a NULL cutoff only.', 1;

IF @CustomerNumbersJson IS NOT NULL
BEGIN
    IF ISJSON(@CustomerNumbersJson) <> 1
       OR LEFT(LTRIM(@CustomerNumbersJson), 1) <> '['
        THROW 50001, 'Customer numbers must be a JSON array of strings.', 1;

    IF NOT EXISTS (SELECT 1 FROM OPENJSON(@CustomerNumbersJson))
        THROW 50002, 'Provide at least one customer number or NULL for all.', 1;

    IF EXISTS (
        SELECT 1 FROM OPENJSON(@CustomerNumbersJson)
        WHERE [type] <> 1
           OR LEN(LTRIM(RTRIM([value]))) NOT BETWEEN 1 AND 10
           OR LTRIM(RTRIM([value])) COLLATE Latin1_General_100_BIN2
               LIKE N'%[^0-9]%'
    )
        THROW 50003, 'Customer numbers must be strings containing 1 to 10 digits.', 1;
END;

DECLARE @DefaultCutoff date =
    DATEADD(MONTH, -72, DATETRUNC(YEAR, GETDATE()));
DECLARE @BatchStartedAt datetime2 = SYSUTCDATETIME();


-- Separate statements materialize the account scope before expensive joins.
-- SELECT INTO preserves source column types/collations for cross-database joins.
SELECT DISTINCT a.CustomerNum
INTO #Customers
FROM dbo.tblAcctSpecial a
WHERE a.LossRunDistFreq <> 'Not Needed'
  AND a.LossRunDistFreq <> ''
  AND (
      @CustomerNumbersJson IS NULL
      OR a.CustomerNum IN (
          SELECT CONVERT(varchar(10), LTRIM(RTRIM([value])))
          FROM OPENJSON(@CustomerNumbersJson)
      )
  )
OPTION (RECOMPILE);

SELECT POL.*
INTO #Policies
FROM CLM_Lakehouse.dbo.DW_POL_DIMNSN POL
WHERE POL.POL_CURR_VERSN_IND = 'Y'
  AND EXISTS (
      SELECT 1 FROM #Customers C WHERE C.CustomerNum = POL.PMS_CUSTMR_NBR
  );
CREATE INDEX IX_Policies_Policy ON #Policies (POL_SYM, POL_NBR, POL_MOD);

-- EXISTS avoids multiplying claims when several policy rows match.
SELECT CD.*
INTO #Claims
FROM CLM_Lakehouse.dbo.DW_CLMS_CLM_DIMNSN CD
WHERE CD.CURR_VERSN_IND = 'Y'
  AND UPPER(ISNULL(CD.ACCT_LOSS_LOC_ID, 'NA')) NOT LIKE '%REKEY%'
  AND EXISTS (
      SELECT 1 FROM #Policies POL
      WHERE CD.POL_SYM = POL.POL_SYM
        AND UPPER(CD.POL_NBR) = POL.POL_NBR
        AND CD.POL_MOD = POL.POL_MOD
  );
CREATE INDEX IX_Claims_Number ON #Claims (CLM_NBR);

SELECT DISTINCT CLM_NBR INTO #ClaimNumbers FROM #Claims;
CREATE UNIQUE CLUSTERED INDEX IX_ClaimNumbers ON #ClaimNumbers (CLM_NBR);

SELECT F.*
INTO #FinancialFeatures
FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED F
WHERE EXISTS (SELECT 1 FROM #ClaimNumbers C WHERE C.CLM_NBR = F.CLM_NBR);
CREATE INDEX IX_FinancialFeatures_ClaimFeature
    ON #FinancialFeatures (CLM_NBR, CLM_FTR_NBR);

-- Keep the policy-date cutoff in HAVING: applying it here would exclude
-- old open claims and claims retained by the positive-reserve exception.

;WITH 

LAST_DRIVER AS(
  SELECT 
    CLM_NBR,
    MAX(CLM_CNTCT_KEY) AS LAST_DRIVER_KEY
  FROM
    CLM_Lakehouse.dbo.DW_CLMS_CLM_CNTCT_DIMNSN CONTACT
  WHERE
    EXISTS (SELECT 1 FROM #ClaimNumbers C WHERE C.CLM_NBR = CONTACT.CLM_NBR)
    AND
    CURR_VERSN_IND = 'Y' 
    AND INVLV_PARTY_ROLE_CD = 'ID'
    AND UPPER(CLM_CNTCT_ROLE_CD) = 'driver'
  GROUP BY
    CLM_NBR
),

ALAE AS (
  SELECT
    SUM(CLM_TXN_AMT) AS TotalExpenses,
    CLM_NBR,
    CLM_FTR_NBR
  FROM
    CLMAA_SpecialAccounts.dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED TXN
  WHERE
    EXISTS (SELECT 1 FROM #ClaimNumbers C WHERE C.CLM_NBR = TXN.CLM_NBR)
    AND
    LOSS_TXN_TYPE_DESC LIKE '%Expense%'
    AND (COST_CATGRY_DESC <> 'State Penalties' OR COST_CATGRY_DESC IS NULL) -- NULL does not eval with '<>', so IS NULL clause is necessary
  GROUP BY
    CLM_NBR,
    CLM_FTR_NBR
),

LAST_TXN AS (
  SELECT
    CLM_NBR,
    CLM_FTR_NBR,
    MAX(FTR_KEY) AS LAST_FTR_KEY,
    MAX(LOSS_TXN_PRCSS_DT_DIMKEY) AS LAST_TXN_PRCSS_DT_DIMKEY
  FROM 
    #FinancialFeatures
  GROUP BY
    CLM_NBR,
    CLM_FTR_NBR
),

LAST_LGL AS (
  SELECT 
    CLM_KEY,
    FTR_KEY,
    MAX(CLMS_LGL_KEY) AS LAST_CLMS_LGL_KEY 
  FROM 
    CLM_Lakehouse.dbo.DW_CLMS_LGL_FACT LEGAL
  WHERE
    EFF_IND = 1
    AND EXISTS (
        SELECT 1 FROM #FinancialFeatures F
        WHERE F.CLM_KEY = LEGAL.CLM_KEY AND F.FTR_KEY = LEGAL.FTR_KEY
    ) 
  GROUP BY 
    CLM_KEY, 
    FTR_KEY
),

DATA AS (
  SELECT
    CD.CLM_NBR,
    FD.CLM_FTR_NBR,
    LFIN.LAST_TXN_PRCSS_DT_DIMKEY,
    POL.PMS_CUSTMR_NBR,
    POL.POL_NAMED_INSRD_TXT,
    POL.POL_SYM,
    POL.POL_NBR,
    POL.POL_MOD,
    DATETRUNC( DAY, POL.POL_EFF_DT) AS POL_EFF_DT,
    DATETRUNC( DAY, POL.POL_EXP_DT) AS POL_EXP_DT,
    POL.PRDCNG_AGNT_CD,
    RPA.CUR_PA_FULL_NM,
    CD.CLM_KEY,
    CASE 
      WHEN UPPER(CD.POL_HLDR_CLM_NBR) = 'TRUE' THEN '' 
      ELSE CD.POL_HLDR_CLM_NBR 
    END AS POL_HLDR_CLM_NBR,
    CD.ACCT_LOSS_LOC_ID,
    DATETRUNC( DAY, CD.DT_OF_LOSS) AS DT_OF_LOSS,
    LFINCL.MAJ_PERIL_CD,
    RMP.MAJ_PERIL_DESC,
    LD.LITGTN_STATUS_CD,
    LD.LITGTN_STATUS_DESC,
    CASE 
      WHEN CD.ACDNT_ST_ABBR <> '!' THEN ISNULL(CD.ACDNT_ST_ABBR, CD.CLM_JRSDTN_ST_ABBR) 
      ELSE CD.CLM_JRSDTN_ST_ABBR 
    END AS ACDNT_ST_ABBR,
    CLMNT.CLMNT_FULL_NM,
    CLMNT.CLMNT_CO_NM,
    ISNULL(CLMNT.OCUPTN_TXT, ' ') AS OCUPTN_TXT,
    LFINCL.CLM_DRVR_KEY,
    DRVD.DRV_REL_TO_INSURED_CODE,
    CONCAT(LDRVD.CLM_CNTCT_FIRST_NM,  ' ', LDRVD.CLM_CNTCT_LAST_NM) AS INSURED_DRIVER,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN ISNULL(VD.VEH_TOTL_LOSS_IND, 'N') 
    END AS VEH_TOTL_LOSS_IND,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN VD.VEH_YEAR 
    END AS VEH_YEAR,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN VD.VEH_MANUFACTURER_CODE 
    END AS VEH_MANUFACTURER_CODE,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN VD.VEH_MODEL 
    END AS VEH_MODEL,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN VD.VEH_VIN_NUMBER 
    END AS VEH_VIN_NUMBER,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN VD.VEH_LICENSE_PLATE 
    END AS VEH_LICENSE_PLATE,
    CASE 
      WHEN UPPER(VD.INVLV_VEH_TYPE_CD) = 'INSURED' THEN VD.VEH_REGISTERED_STATE_ABBR 
    END AS VEH_REGISTERED_STATE_ABBR,
    UPPER(FD.FTR_ASGNED_USER_FULL_NM) AS FTR_ASGNED_USER_FULL_NM,
    UPPER(FD.FTR_ASGN_USER_EMAIL_ADDR) AS FTR_ASGN_USER_EMAIL_ADDR,
    FD.FTR_ASGN_USER_PHONE_NBR,
    CASE 
      WHEN CD.CLM_LOB_CD='wc' THEN CD.AIA_POSTNS_1_AND_2_DESC 
      ELSE CD.CAUSE_OF_LOSS 
    END AS CAUSE_OF_LOSS,--Changed for WC 05252017 KJ
    CASE 
      WHEN CD.CLM_LOB_CD='wc' THEN CD.AIA_POSTNS_3_AND_4_DESC 
      ELSE CD.LOSS_DESC 
    END AS LOSS_DESC,--Changed for WC 05252017 KJ
    CASE 
      WHEN CD.CLM_LOB_CD='wc' THEN CD.AIA_POSTNS_5_AND_6_DESC 
      ELSE CD.CONTRIBUTING_FCTR 
    END AS CONTRIBUTING_FCTR,--Changed for WC 05252017 KJ
    CD.MF_PRODT_DESC,
    CD.MF_DESC,
    CD.POL_HLDR_RGN_DESC,
    CD.POL_HLDR_RETAILER_DESC,
    CD.CLM_RPTD_DT,
    CD.CLM_STATUS,
    CD.CLM_CLOSE_DT,
    CD.RCRD_SRC_CD, --WHEN 'Y' THEN MANY FIELDS MAY BE BLANK
    CD.INCID_RPT_IND, --WHEN 'Y' THEN FTR_NUMBER MAY BE BLANK
    DATENAME(WEEKDAY, CD.DT_OF_LOSS) AS DAY_DT_OF_LOSS,
    FD.EXPOSR_IND,
    FD.FTR_TYPE_CD,
    FD.FTR_TYPE_DESC,
    FD.FTR_COVG_TYPE_CD,
    FD.FTR_COVG_TYPE_DESC,
    ISNULL(LFINCL.FTR_CHNG_IN_OSLS_AMT, 0) AS FTR_CHNG_IN_OSLS_AMT,
    (ISNULL(LFINCL.FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0) - ISNULL(LFINCL.DEDTBL_RECOVRY_AMT, 0)) AS FTR_PDLS_INCL_SLVG_SUBRO_AMT,
    ISNULL(LFINCL.FTR_SLVG_AMT, 0) AS FTR_SLVG_AMT,
    ISNULL(LFINCL.FTR_SUBRO_AMT, 0) AS FTR_SUBRO_AMT,
    ISNULL(LFINCL.LOSS_RECOVRY_AMT,0) AS LOSS_RECOVRY_AMT,
    ISNULL(LFINCL.DEDTBL_RECOVRY_AMT, 0) AS DEDTBL_RECOVRY_AMT,
    CAST(ISNULL(ALAE.TotalExpenses, 0) AS DECIMAL(18,2)) AS FTR_ALAE_AMT,/*Total amount of loss payments for the feature including expense recoveries.*/
    --  ISNULL(nra.EXPNS_PD_NET_RECOVRY_AMT, 0) FTR_ALAE_AMT1,/*Total amount of loss expense payments for the feature excluding expense recoveries.*/
    ISNULL(LFINCL.FTR_ALAE_AMT - LFINCL.expnse_recovry_amt, 0) AS FTR_ALAE_AMT2,
    ISNULL(LFINCL.expnse_recovry_amt, 0) AS expnse_recovry_amt,
    (ISNULL(LFINCL.FTR_INCUR_INCL_SLVG_SUBRO_AMT, 0) - ISNULL(LFINCL.DEDTBL_RECOVRY_AMT, 0)) AS FTR_INCUR_INCL_SLVG_SUBRO_AMT,
    (ISNULL(LFINCL.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, 0) - ISNULL(LFINCL.DEDTBL_RECOVRY_AMT, 0)) AS FTR_PDLS_EXCL_SLVG_SUBRO_AMT,
    ISNULL(LFINCL.CHNG_IN_ALAE_RSRV_AMT, 0) AS CHNG_IN_ALAE_RSRV_AMT,
    CD.CURR_VERSN_IND
  FROM
    #Claims CD
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_FTR_DIMNSN FD  
      ON CD.CLM_NBR = FD.CLM_NBR 
      --AND LV.CLM_FTR_NBR = FD.CLM_FTR_NBR 
      AND FD.CURR_VERSN_IND = 'Y' 
      --AND DATETRUNC( DAY, FD.VERSN_BEG_DT) = DATETRUNC( DAY, lv.last_VERSN)--LFINCL.FTR_KEY = FD.FTR_KEY
    --  LEFT OUTER JOIN NET_RECOVRY_AMT NRA ON FD.FTR_KEY = NRA.FTR_KEY
    LEFT OUTER JOIN LAST_TXN LFIN
      ON LFIN.CLM_NBR = CD.CLM_NBR
      AND LFIN.CLM_FTR_NBR = FD.CLM_FTR_NBR
    LEFT OUTER JOIN #FinancialFeatures LFINCL 
      ON  LFIN.CLM_NBR = LFINCL.CLM_NBR 
      AND LFIN.CLM_FTR_NBR = LFINCL.CLM_FTR_NBR 
      --AND LFIN.LAST_TXN_PRCSS_DT_DIMKEY = LFINCL.LOSS_TXN_PRCSS_DT_DIMKEY
    LEFT OUTER JOIN ALAE 
      ON ALAE.CLM_NBR = LFIN.CLM_NBR 
      AND LFIN.CLM_FTR_NBR = ALAE.CLM_FTR_NBR
    --LEFT OUTER JOIN LAST_FTR_VERSN LV 
    --  ON LFIN.CLM_NBR = LV.CLM_NBR 
    --  AND LFIN.CLM_FTR_NBR = lv.CLM_FTR_NBR
    INNER JOIN #Policies POL 
      ON CD.POL_SYM = POL.POL_SYM 
      AND UPPER(CD.POL_NBR) = POL.POL_NBR 
      AND CD.POL_MOD = POL.POL_MOD 
      AND POL.POL_CURR_VERSN_IND = 'Y'
    LEFT OUTER JOIN LAST_LGL LL 
      ON LFINCL.CLM_KEY = LL.CLM_KEY 
      AND LFINCL.FTR_KEY = LL.FTR_KEY
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_LGL_FACT LF 
      ON LFINCL.FTR_KEY = LF.FTR_KEY 
      AND LFINCL.CLM_KEY = LF.CLM_KEY 
      AND LF.CLMS_LGL_KEY = LL.LAST_CLMS_LGL_KEY
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_LGL_DIMNSN LD 
      ON LF.CLMS_LGL_KEY = LD.CLMS_LGL_KEY
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DM_RETNTN_PRDCNG_AGNT_HR_D RPA 
      ON POL.PRDCNG_AGNT_CD = RPA.CUR_ENTR_PA_CD
    -- LEFT OUTER JOIN DW_CLMS_INCID_LOC_DIMNSN INCID ON --LFINCL.INCID_KEY = INCID.INCID_KEY
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_CLMNT_DIMNSN CLMNT 
      ON FD.CLMNT_HCS_SYS_ID = CLMNT.CLMNT_HCS_SYS_ID
      AND CLMNT.CURR_VERSN_IND = 'Y'
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_DRVR_DIMNSN DRVD 
      ON LFINCL.CLM_DRVR_KEY = DRVD.CLM_DRVR_KEY
    LEFT OUTER JOIN LAST_DRIVER LDRD 
      ON LFINCL.CLM_NBR = LDRD.CLM_NBR
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_CLM_CNTCT_DIMNSN LDRVD 
      ON LDRVD.CLM_NBR = LDRD.CLM_NBR 
      AND LDRVD.CLM_CNTCT_KEY = LDRD.LAST_DRIVER_KEY
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_CLMS_VEH_DIMNSN VD 
      ON FD.VEH_HCS_SYS_ID = VD.VEH_HCS_SYS_ID
      AND VD.CURR_VERSN_IND = 'Y'
    LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_REF_MAJ_PERIL RMP 
      ON LFINCL.MAJ_PERIL_CD = RMP.MAJ_PERIL_CD 
      AND DATETRUNC( DAY, LFINCL.RCRD_INSRT_DT) BETWEEN DATETRUNC( DAY, RMP.VERSN_BEG_DT) 
      AND DATETRUNC( DAY, RMP.VERSN_END_DT)
    --
    --LEFT OUTER JOIN CLM_Lakehouse.dbo.DW_FINCL_LOSS_FTR_SS_F FINCL -- join to itself??? this is probably worthless, but the select has stuff from here
    --  ON LFINCL.CLM_NBR = FINCL.CLM_NBR 
    --  AND LFINCL.CLM_FTR_NBR = FINCL.CLM_FTR_NBR
  WHERE
    UPPER(ISNULL(CD.ACCT_LOSS_LOC_ID, 'NA')) NOT LIKE '%REKEY%' AND
    CD.CURR_VERSN_IND = 'Y' AND
    --ISNULL(CD.INCID_RPT_IND, 'N') in ('U','N') AND
    --ISNULL(FD.EXPOSR_IND, 'N') = 'N' AND
    POL.PMS_CUSTMR_NBR IN (SELECT CustomerNum FROM #Customers) -- = '1512034786'
    --AND CD.CLM_NBR = '85-00898010'
)
SELECT
  D.PMS_CUSTMR_NBR AS "Customer Number",
  D.POL_NAMED_INSRD_TXT AS "Policy Insured Name",
  D.CLM_NBR AS "Claim Number",
  D.CLM_FTR_NBR AS "Exposure",
  CONCAT(D.CLMNT_FULL_NM, D.CLMNT_CO_NM) AS "Claimant Name",
  D.ACCT_LOSS_LOC_ID AS "Account Location Identifier",
  D.ACDNT_ST_ABBR AS "Accident State",
  D.POL_SYM AS "Policy Symbol",
  D.POL_NBR AS "Policy Number",
  D.POL_MOD AS "Policy MOD",
  CONVERT(varchar, D.POL_EFF_DT, 101) AS "Policy Effective Date",
  CONVERT(varchar, D.POL_EXP_DT, 101) AS "Policy Expiration Date",
  CONVERT(varchar, D.DT_OF_LOSS, 101) AS "Loss Date",
  CONVERT(varchar, D.CLM_RPTD_DT, 101) AS "Date of Notice", -- claim reported date
  DATEDIFF(DAY, D.DT_OF_LOSS, D.CLM_RPTD_DT) AS "Lag Time",
  D.CLM_STATUS AS "Claim Status",
  CONVERT(varchar, D.CLM_CLOSE_DT, 101) AS "Claim Closed Date",
  D.FTR_COVG_TYPE_DESC AS "Coverage",
  SUM(D.FTR_CHNG_IN_OSLS_AMT) AS "Outstanding Loss Reserve",
  SUM(D.FTR_PDLS_INCL_SLVG_SUBRO_AMT) AS "Total Paid Loss Net Salvage/Subro/Loss Recovery",
  (SUM(D.FTR_CHNG_IN_OSLS_AMT) + SUM(D.FTR_PDLS_INCL_SLVG_SUBRO_AMT)) AS "Incurred w/o ALAE",
  SUM(D.CHNG_IN_ALAE_RSRV_AMT) AS "ALAE Reserve",
  CAST(
    CASE 
        WHEN ISNULL(SUM(D.FTR_ALAE_AMT2), 0) = 0 
        THEN SUM(CAST(D.FTR_ALAE_AMT AS DECIMAL(18,2)))
        ELSE SUM(CAST(D.FTR_ALAE_AMT2 AS DECIMAL(18,2)))
    END
  AS DECIMAL(18,2)) AS "ALAE Paid",

  CAST(
      CASE 
          WHEN ISNULL(SUM(D.FTR_ALAE_AMT2), 0) = 0 
          THEN SUM(CAST(D.FTR_ALAE_AMT AS DECIMAL(18,2)))
          ELSE SUM(CAST(D.FTR_ALAE_AMT2 AS DECIMAL(18,2)))
      END  -- ALAE PAID
      + SUM(CAST(D.CHNG_IN_ALAE_RSRV_AMT AS DECIMAL(18,2))) -- ALAE Reserve
      + SUM(CAST(D.FTR_CHNG_IN_OSLS_AMT AS DECIMAL(18,2))) -- first part Incurred w/o ALAE
      + SUM(CAST(D.FTR_PDLS_INCL_SLVG_SUBRO_AMT AS DECIMAL(18,2))) -- second part Incurred w/0 ALAE
  AS DECIMAL(18,2)) AS "Total Incurred + ALAE",
  SUM(D.FTR_SLVG_AMT) AS "Salvage Recovery",
  SUM(D.FTR_SUBRO_AMT) AS "Subro Recovery",
  SUM(D.LOSS_RECOVRY_AMT) AS "Loss Recovery",
  SUM(D.DEDTBL_RECOVRY_AMT) AS "Deductible Recovery",
  ISNULL(SUM(D.expnse_recovry_amt), 0) AS "Expense Recovery",
  D.LITGTN_STATUS_DESC AS "Litigation Status", --Possible Duplicate Issue - Open HTG
  D.CAUSE_OF_LOSS AS "Cause Of Loss",
  D.LOSS_DESC AS "Description of Loss",
  D.CONTRIBUTING_FCTR AS "Contributing Factor",
  D.MF_PRODT_DESC AS "Product",
  D.MF_DESC AS "Manufacturer",
  D.POL_HLDR_RGN_DESC AS "Policy Region Description",
  D.POL_HLDR_RETAILER_DESC AS "Policy Holder Retailer Desc",
  ISNULL(D.POL_HLDR_CLM_NBR,'') AS "Policy Holder Claim Number",
  D.OCUPTN_TXT AS "Occupation",
  D.DAY_DT_OF_LOSS AS "Accident Day Of Week",
  D.INSURED_DRIVER AS "Insured Driver",
  D.VEH_TOTL_LOSS_IND AS "Total Loss Indicator",
  D.VEH_YEAR AS "Vehicle Year",
  D.VEH_MANUFACTURER_CODE AS "Vehicle Manufacturer",
  D.VEH_MODEL AS "Vehicle Model",
  D.VEH_VIN_NUMBER AS "Vehicle ID Number",
  D.VEH_LICENSE_PLATE AS "Vehicle License Plate",
  D.VEH_REGISTERED_STATE_ABBR AS "Vehicle License State",
  D.FTR_ASGNED_USER_FULL_NM AS "Adjuster",
  --D.FTR_ASGN_USER_EMAIL_ADDR AS "Adjuster Email",
  --D.FTR_ASGN_USER_PHONE_NBR AS "Adjuster Phone Number",
  --D.RCRD_SRC_CD AS "Record Source Code",
  D.INCID_RPT_IND AS "Record Only Indicator",
  --D.EXPOSR_IND AS "Exposure Indicator",
  --D.CURR_VERSN_IND "Current Version IND",
  CONCAT(D.POL_SYM, '-', D.POL_NBR, '-', D.POL_MOD) AS "Policy",
  YEAR(D.POL_EFF_DT) AS "Policy Year",
  CASE 
    WHEN SUM(D.FTR_INCUR_INCL_SLVG_SUBRO_AMT) > 50000 THEN 1 
    ELSE 0 
  END AS "Claims above 50K",
  CONCAT(D.CLM_NBR, D.CLM_FTR_NBR) AS "Claim Feature Number",
  
  --D.PRDCNG_AGNT_CD AS "Producing Agent Code",
  --D.CUR_PA_FULL_NM AS "Producing Agent Name",
  SUM(D.FTR_INCUR_INCL_SLVG_SUBRO_AMT) AS "Total Incurred"
  --D.FTR_ASGNED_USER_FULL_NM AS "Adjuster Name"
 
  --D.FTR_ALAE_AMT AS "Total ALAE Amount", --Remove this column/ALAE column hidden in the report/*Total amount 
  --of loss expense payments for the feature including expense recoveries.*/
  --ISNULL(MAX(D.FTR_ALAE_AMT1), 0) AS "Expense Column", --Remove this column,/Expense column hidden in the report
  /*Total amount of loss expense payments for the feature excluding expense recoveries.*/
  
INTO #LossRunResult
FROM
  DATA D
GROUP BY

-- columns in group by not in select
--D.CLM_DRVR_KEY,
--D.DRV_REL_TO_INSURED_CODE,
--D.EXPOSR_IND,
--D.CURR_VERSN_IND,
--D.FTR_ASGN_USER_EMAIL_ADDR,
--D.FTR_ASGN_USER_PHONE_NBR,
--D.RCRD_SRC_CD,
--D.PRDCNG_AGNT_CD,
--D.CUR_PA_FULL_NM,
--D.MAJ_PERIL_DESC,
--D.FTR_TYPE_DESC


  D.PMS_CUSTMR_NBR,
  D.POL_NAMED_INSRD_TXT,
  D.ACCT_LOSS_LOC_ID,
  D.POL_SYM,
  D.POL_NBR,
  D.POL_MOD,
  D.POL_EFF_DT,
  D.POL_EXP_DT,
  D.POL_HLDR_CLM_NBR,
  --D.PRDCNG_AGNT_CD,
  --D.CUR_PA_FULL_NM,
  D.CLM_NBR,
  D.CLM_FTR_NBR,
  --D.MAJ_PERIL_DESC, 
  --D.FTR_TYPE_DESC,
  D.FTR_COVG_TYPE_DESC,
  D.DT_OF_LOSS,
  D.ACDNT_ST_ABBR,
  D.LITGTN_STATUS_DESC,
  D.CAUSE_OF_LOSS,
  D.LOSS_DESC,
  D.CONTRIBUTING_FCTR,
  D.MF_PRODT_DESC,
  D.MF_DESC,
  D.POL_HLDR_RGN_DESC,
  D.POL_HLDR_RETAILER_DESC,
  D.CLM_RPTD_DT,
  D.CLM_STATUS,
  D.CLM_CLOSE_DT,
  CONCAT(D.CLMNT_FULL_NM, D.CLMNT_CO_NM),
  D.OCUPTN_TXT,
  D.DAY_DT_OF_LOSS,
  D.INSURED_DRIVER,
  --D.CLM_DRVR_KEY,
  --D.DRV_REL_TO_INSURED_CODE,
  D.VEH_TOTL_LOSS_IND,
  D.VEH_YEAR,
  D.VEH_MANUFACTURER_CODE,
  D.VEH_MODEL,
  D.VEH_VIN_NUMBER,
  D.VEH_LICENSE_PLATE,
  D.VEH_REGISTERED_STATE_ABBR,
  D.FTR_ASGNED_USER_FULL_NM,
  --D.FTR_ASGN_USER_EMAIL_ADDR,
  --D.FTR_ASGN_USER_PHONE_NBR,
  --D.RCRD_SRC_CD,
  D.INCID_RPT_IND,
  --D.EXPOSR_IND,
  D.CURR_VERSN_IND
  --D.FTR_ALAE_AMT
HAVING
  (
  (
    D.CLM_STATUS IN ('Closed','closed')
    AND (
      (@PolicyEffectiveDateFrom IS NOT NULL
       AND D.POL_EFF_DT >= @PolicyEffectiveDateFrom)
      OR
      (@PolicyEffectiveDateFrom IS NULL
       AND D.POL_EFF_DT > @DefaultCutoff)
    )
  )
  OR SUM(D.FTR_CHNG_IN_OSLS_AMT) > 0
  OR D.CLM_STATUS IN ('Open','open')
  )
OPTION (RECOMPILE);

DECLARE @BatchFinishedAt datetime2 = SYSUTCDATETIME();

-- Result 1: elapsed time excludes comparison and transferring report rows.
SELECT
    'Staged batch' AS RunType,
    @CustomerNumbersJson AS RequestedCustomers,
    @PolicyEffectiveDateFrom AS RequestedCutoff,
    @DefaultCutoff AS DefaultCutoff,
    CAST(DATEDIFF_BIG(MILLISECOND, @BatchStartedAt, @BatchFinishedAt) / 1000.0
         AS decimal(18,3)) AS ElapsedSeconds;

-- Result 2: confirm the scope before judging performance.
SELECT 'Customers' AS Stage, COUNT_BIG(*) AS [RowCount] FROM #Customers
UNION ALL SELECT 'Policies', COUNT_BIG(*) FROM #Policies
UNION ALL SELECT 'Claims', COUNT_BIG(*) FROM #Claims
UNION ALL SELECT 'Distinct claim numbers', COUNT_BIG(*) FROM #ClaimNumbers
UNION ALL SELECT 'Financial feature rows', COUNT_BIG(*) FROM #FinancialFeatures
UNION ALL SELECT 'Report rows', COUNT_BIG(*) FROM #LossRunResult;

-- Result 3: includes eligible accounts with no report rows.
SELECT
    C.CustomerNum,
    A.CustomerName,
    A.AcctStatus,
    A.LossRunDistFreq,
    COUNT_BIG(R.[Customer Number]) AS ReportRows,
    COUNT(DISTINCT R.[Claim Number]) AS DistinctClaims,
    SUM(R.[Outstanding Loss Reserve]) AS OutstandingLossReserve,
    SUM(R.[Total Paid Loss Net Salvage/Subro/Loss Recovery]) AS NetPaidLoss,
    SUM(R.[Incurred w/o ALAE]) AS IncurredExcludingExpenses
FROM #Customers C
LEFT JOIN dbo.tblAcctSpecial A ON A.CustomerNum = C.CustomerNum
LEFT JOIN #LossRunResult R ON R.[Customer Number] = C.CustomerNum
GROUP BY C.CustomerNum, A.CustomerName, A.AcctStatus, A.LossRunDistFreq
ORDER BY C.CustomerNum;

-- Result 4: actual report rows, with the original API/view column names.
SELECT * FROM #LossRunResult
ORDER BY [Customer Number], [Claim Number], [Exposure];

IF @CompareCurrentView = 1
BEGIN
    DECLARE @BaselineStartedAt datetime2 = SYSUTCDATETIME();
    SELECT V.* INTO #BaselineResult
    FROM dbo.SAC_Loss_Run V
    WHERE V.[Customer Number] IN (SELECT CustomerNum FROM #Customers);

    SELECT
        'Current view' AS RunType,
        CAST(DATEDIFF_BIG(MILLISECOND, @BaselineStartedAt, SYSUTCDATETIME()) / 1000.0
             AS decimal(18,3)) AS ElapsedSeconds,
        (SELECT COUNT_BIG(*) FROM #BaselineResult) AS CurrentViewRows,
        (SELECT COUNT_BIG(*) FROM #LossRunResult) AS StagedBatchRows;

    -- Compare every output column, including the number of repeated rows.
    -- Names come only from temp-table metadata, never from user input.
    DECLARE @Columns nvarchar(max);
    SELECT @Columns = STRING_AGG(CONVERT(nvarchar(max), QUOTENAME(name)), ',')
        WITHIN GROUP (ORDER BY column_id)
    FROM tempdb.sys.columns
    WHERE object_id = OBJECT_ID('tempdb..#LossRunResult');

    DECLARE @ComparisonSql nvarchar(max) = N'
    WITH NewRows AS (
        SELECT ' + @Columns + N',
            ROW_NUMBER() OVER (PARTITION BY ' + @Columns + N' ORDER BY (SELECT NULL)) AS DuplicateOrdinal
        FROM #LossRunResult
    ), OldRows AS (
        SELECT ' + @Columns + N',
            ROW_NUMBER() OVER (PARTITION BY ' + @Columns + N' ORDER BY (SELECT NULL)) AS DuplicateOrdinal
        FROM #BaselineResult
    )
    SELECT ''Only in staged batch'' AS Difference, D.*
    FROM (SELECT * FROM NewRows EXCEPT SELECT * FROM OldRows) D
    UNION ALL
    SELECT ''Only in current view'' AS Difference, D.*
    FROM (SELECT * FROM OldRows EXCEPT SELECT * FROM NewRows) D;';
    EXEC sys.sp_executesql @ComparisonSql;
END;

DROP TABLE IF EXISTS #BaselineResult;
DROP TABLE IF EXISTS #LossRunResult;
DROP TABLE IF EXISTS #FinancialFeatures;
DROP TABLE IF EXISTS #ClaimNumbers;
DROP TABLE IF EXISTS #Claims;
DROP TABLE IF EXISTS #Policies;
DROP TABLE IF EXISTS #Customers;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS #BaselineResult;
    DROP TABLE IF EXISTS #LossRunResult;
    DROP TABLE IF EXISTS #FinancialFeatures;
    DROP TABLE IF EXISTS #ClaimNumbers;
    DROP TABLE IF EXISTS #Claims;
    DROP TABLE IF EXISTS #Policies;
    DROP TABLE IF EXISTS #Customers;
    SET STATISTICS IO OFF;
    SET STATISTICS TIME OFF;
    THROW;
END CATCH;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
