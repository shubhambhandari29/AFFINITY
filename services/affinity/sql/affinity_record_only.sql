-- SQL Server translation of the client-supplied OBIEE Affinity query (16 Oct 2026).
-- Read-only; executed by the backend, not a view/procedure installation script.
-- Parameters in order: JSON agent-code array, Eastern report date.
-- Claims and Record Only intentionally retain their DIFFERENT OBIEE history rules.
WITH SelectedAgents AS (
    SELECT CAST([value] AS varchar(50)) COLLATE SQL_Latin1_General_CP1_CI_AS AS AgentCode
    FROM OPENJSON(?)
), ReportParameters AS (
    SELECT CAST(? AS date) AS ReportDate
), DATA
     AS (SELECT
           CD.CLM_NBR,
           FD.CLM_FTR_NBR,
           NULL AS LAST_TXN_PRCSS_DT_DIMKEY ,
           POL.PMS_CUSTMR_NBR,
           POL.POL_NAMED_INSRD_TXT,
           POL.POL_SYM,
           POL.POL_NBR,
           POL.POL_MOD,
           CAST(POL.POL_EFF_DT AS date) POL_EFF_DT,
           CAST(POL.POL_EXP_DT AS date) POL_EXP_DT,
           POL.PRDCNG_AGNT_CD,
           RPA.CUR_PA_FULL_NM,
           CD.CLM_KEY,
           CASE
             WHEN UPPER(CD.POL_HLDR_CLM_NBR) = 'TRUE' THEN ''
             ELSE CD.POL_HLDR_CLM_NBR
           END
             POL_HLDR_CLM_NBR,
           CD.ACCT_LOSS_LOC_ID,
           CASE WHEN CD.ACDNT_ST_ABBR <> '!' THEN ISNULL(CD.ACDNT_ST_ABBR, CD.CLM_JRSDTN_ST_ABBR)
                ELSE CD.CLM_JRSDTN_ST_ABBR END AS ACDNT_ST_ABBR,
           CAST(CD.DT_OF_LOSS AS date) DT_OF_LOSS,
           FD.FTR_ASGNED_USER_FULL_NM,
           FD.FTR_ASGN_USER_EMAIL_ADDR,
           FD.FTR_ASGN_USER_PHONE_NBR,
           CD.CAUSE_OF_LOSS,
           CD.LOSS_DESC,
           CD.CONTRIBUTING_FCTR,
           CD.MF_PRODT_DESC,
           CD.MF_DESC,
           CD.POL_HLDR_RGN_DESC,
           CD.POL_HLDR_RETAILER_DESC,
           CD.CLM_RPTD_DT,
           CD.CLM_STATUS,
           CD.CLM_CLOSE_DT,
           CD.RCRD_SRC_CD, 
           CD.INCID_RPT_IND, 
           DATENAME(WEEKDAY, CD.DT_OF_LOSS) AS DAY_DT_OF_LOSS,
           FD.EXPOSR_IND,
           FD.FTR_TYPE_CD,
           FD.FTR_TYPE_DESC,
           FD.FTR_COVG_TYPE_CD,
           FD.FTR_COVG_TYPE_DESC,
           CLMNT.CLMNT_FULL_NM,
           CLMNT.CLMNT_CO_NM,
           ISNULL(CLMNT.OCUPTN_TXT, ' ') OCUPTN_TXT
FROM
CLM_LakeHouse.dbo.DW_POL_DIMNSN POL
INNER JOIN CLM_LakeHouse.dbo.DW_CLMS_CLM_DIMNSN CD ON POL.POL_SYM = CD.POL_SYM AND POL.POL_NBR = CD.POL_NBR AND POL.POL_MOD = CD.POL_MOD AND CD.CURR_VERSN_IND = 'Y'
LEFT OUTER JOIN CLM_LakeHouse.dbo.DW_CLMS_FTR_DIMNSN FD ON CD.CLM_NBR = FD.CLM_NBR AND FD.CURR_VERSN_IND = 'Y'
LEFT OUTER JOIN CLM_LakeHouse.dbo.DM_RETNTN_PRDCNG_AGNT_HR_D RPA ON POL.PRDCNG_AGNT_CD = RPA.CUR_ENTR_PA_CD

LEFT OUTER JOIN CLM_LakeHouse.dbo.DW_CLMS_CLMNT_DIMNSN CLMNT ON CLMNT.CLMNT_HCS_SYS_ID = FD.CLMNT_HCS_SYS_ID AND CLMNT.CURR_VERSN_IND='Y'
WHERE CD.INCID_RPT_IND = 'Y' 
AND UPPER(ISNULL(CD.ACCT_LOSS_LOC_ID, 'NA')) NOT LIKE '%REKEY%' 
AND POL.PRDCNG_AGNT_CD IN (SELECT AgentCode FROM SelectedAgents)

AND POL.POL_CURR_VERSN_IND = 'Y' 

AND CAST(POL.POL_EFF_DT AS date) > DATEFROMPARTS(YEAR((SELECT ReportDate FROM ReportParameters)) - 6, 1, 1) 
AND NOT EXISTS (SELECT 1 FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F FINCL WHERE 

FINCL.CLM_NBR = CD.CLM_NBR AND FINCL.CLM_FTR_NBR=FD.CLM_FTR_NBR))
SELECT 
  D.PMS_CUSTMR_NBR AS [Customer Number],
    D.POL_NAMED_INSRD_TXT AS [Policy Insured Name],
  D.ACCT_LOSS_LOC_ID AS [Account Location Identifier],
  D.ACDNT_ST_ABBR AS [Accident State],
  D.POL_SYM AS [Policy Symbol],
  D.POL_NBR AS [Policy Number],
  D.POL_MOD AS [Policy MOD],
  D.POL_EFF_DT AS [Policy Effective Date],
  D.POL_EXP_DT AS [Policy Expiration Date],
  D.POL_HLDR_CLM_NBR AS [Policy Holder Claim Number],
  D.PRDCNG_AGNT_CD AS [Producing Agent Code],
  D.CUR_PA_FULL_NM AS [Producing Agent Name],
  D.CLM_NBR AS [Claim Number],
  D.CLM_FTR_NBR AS [Exposure],
  D.FTR_COVG_TYPE_DESC AS [Coverage],
  D.DT_OF_LOSS AS [Loss Date],
  D.CAUSE_OF_LOSS AS [Cause Of Loss],
  D.LOSS_DESC AS [Description Of Loss],
  D.CONTRIBUTING_FCTR AS [Contributing Factor],
  D.MF_PRODT_DESC AS [Product],
  D.MF_DESC AS [Manufacturer],
  D.POL_HLDR_RGN_DESC AS [Policy Region Description],
  D.POL_HLDR_RETAILER_DESC AS [Policy Holder Retailer Desc],
  D.CLM_RPTD_DT AS [Date of Notice (Report Date)],
  D.CLM_STATUS AS [Claim Status],
  D.CLM_CLOSE_DT AS [Claim Closed Date],
  D.DAY_DT_OF_LOSS AS [Accident Day Of Week],
  D.FTR_ASGNED_USER_FULL_NM AS [Adjuster],
  D.FTR_ASGN_USER_EMAIL_ADDR AS [Adjuster Email],
  D.FTR_ASGN_USER_PHONE_NBR AS [Adjuster Phone Number],
  D.RCRD_SRC_CD AS [Record Source Code],
  D.INCID_RPT_IND AS [Record Only Indicator],
  D.EXPOSR_IND AS [Exposure Indicator],
  (SELECT ReportDate FROM ReportParameters) AS [RO System Date],
  CONCAT(D.CLMNT_FULL_NM, D.CLMNT_CO_NM) AS [Claimant Name - Company],
  D.OCUPTN_TXT AS [Occupation]
FROM
  DATA D
GROUP BY
  D.PMS_CUSTMR_NBR,
  D.POL_NAMED_INSRD_TXT,
  D.ACCT_LOSS_LOC_ID,
  D.ACDNT_ST_ABBR,
  D.POL_SYM,
  D.POL_NBR,
  D.POL_MOD,
  D.POL_EFF_DT,
  D.POL_EXP_DT,
  D.POL_HLDR_CLM_NBR,
  D.PRDCNG_AGNT_CD,
  D.CUR_PA_FULL_NM,
  D.CLM_NBR,
  D.CLM_FTR_NBR,
  D.FTR_TYPE_DESC,
  D.FTR_COVG_TYPE_DESC,
  D.DT_OF_LOSS,
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
  D.DAY_DT_OF_LOSS,
  D.FTR_ASGNED_USER_FULL_NM,
  D.FTR_ASGN_USER_EMAIL_ADDR,
  D.FTR_ASGN_USER_PHONE_NBR,
  D.RCRD_SRC_CD,
  D.INCID_RPT_IND,
  D.EXPOSR_IND,
  D.CLMNT_FULL_NM,
  D.CLMNT_CO_NM,
  D.OCUPTN_TXT
ORDER BY
  D.PMS_CUSTMR_NBR,
  D.CLM_NBR,
  D.CLM_FTR_NBR;

