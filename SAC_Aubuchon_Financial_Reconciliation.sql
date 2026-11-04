USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;

-- READ ONLY: permanent tables/views are never modified.
-- Run the ENTIRE file in one SSMS window, then save each numbered result grid.
-- The two large source tables are each queried once into temporary staging.
-- Expect several minutes. No SHOWPLAN permission needed.
-- Grid 9 runs the current live view and may take another full report-query time.
DECLARE @IncludeViewOutput bit = 1;

DROP TABLE IF EXISTS #Claims;
DROP TABLE IF EXISTS #Financial;
DROP TABLE IF EXISTS #Transactions;
DROP TABLE IF EXISTS #Features;
CREATE TABLE #Claims (CLM_NBR nvarchar(40) COLLATE DATABASE_DEFAULT PRIMARY KEY);
INSERT INTO #Claims VALUES (N'83-00028520'), (N'83-00073688');

-- Grid 1: environment and account eligibility.
SELECT 1 AS GridNumber, @@SERVERNAME AS ServerName, DB_NAME() AS DatabaseName,
       CustomerNum, CustomerName, AcctStatus, LossRunDistFreq
FROM dbo.tblAcctSpecial WHERE CustomerNum = '1500744571';

SELECT 'LakeHouse' AS SourceName, f.FTR_KEY, f.CLM_KEY, f.LOSS_TXN_PRCSS_DT_DIMKEY, f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_ALAE_AMT, f.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, f.FTR_INCUR_INCL_SLVG_SUBRO_AMT, f.FTR_SLVG_AMT, f.FTR_SUBRO_AMT, f.FTR_CHNG_IN_OSLS_AMT, f.RCRD_INSRT_DT, f.CLM_DRVR_KEY, f.INCID_KEY, f.MAJ_PERIL_CD, f.DEDTBL_RECOVRY_AMT, f.CHNG_IN_ALAE_RSRV_AMT, f.LOSS_RECOVRY_AMT, f.EXPNSE_RECOVRY_AMT, f.LOSS_TXN_PRCSS_DT, f.SRC_ACCT_PRD, f.CLM_RCRD_TYPE_CD, f.OFFSET_IND
INTO #Financial
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F f
JOIN #Claims c ON f.CLM_NBR = c.CLM_NBR
UNION ALL
SELECT 'Filtered', f.FTR_KEY, f.CLM_KEY, f.LOSS_TXN_PRCSS_DT_DIMKEY, f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_ALAE_AMT, f.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, f.FTR_INCUR_INCL_SLVG_SUBRO_AMT, f.FTR_SLVG_AMT, f.FTR_SUBRO_AMT, f.FTR_CHNG_IN_OSLS_AMT, f.RCRD_INSRT_DT, f.CLM_DRVR_KEY, f.INCID_KEY, f.MAJ_PERIL_CD, f.DEDTBL_RECOVRY_AMT, f.CHNG_IN_ALAE_RSRV_AMT, f.LOSS_RECOVRY_AMT, f.EXPNSE_RECOVRY_AMT, f.LOSS_TXN_PRCSS_DT, f.SRC_ACCT_PRD, f.CLM_RCRD_TYPE_CD, f.OFFSET_IND
FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED f
JOIN #Claims c ON f.CLM_NBR = c.CLM_NBR
OPTION (RECOMPILE);

-- Preserve the target's decimal(18,2) amount scale in the union.
SELECT 'LakeHouse' AS SourceName, t.TXN_FACT_KEY, t.ACCT_PRD,
       t.LOSS_TXN_TYPE_DESC, CONVERT(decimal(20,2),t.CLM_TXN_AMT) AS CLM_TXN_AMT,
       t.RCRD_INSRT_DT, t.CLM_RCRD_TYPE_CD, t.CLM_NBR, t.CLM_FTR_NBR,
       t.COST_CATGRY_DESC
INTO #Transactions
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_TXN_SS_F t
JOIN #Claims c ON t.CLM_NBR = c.CLM_NBR
UNION ALL
SELECT 'Filtered', t.TXN_FACT_KEY, t.ACCT_PRD, t.LOSS_TXN_TYPE_DESC,
       CONVERT(decimal(20,2),t.CLM_TXN_AMT), t.RCRD_INSRT_DT,
       t.CLM_RCRD_TYPE_CD, t.CLM_NBR, t.CLM_FTR_NBR, t.COST_CATGRY_DESC
FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t
JOIN #Claims c ON t.CLM_NBR = c.CLM_NBR
OPTION (RECOMPILE);

-- Grid 2: source vs filtered, before ANY dimension joins.
-- These are diagnostic historical sums, not an approved financial calculation.
SELECT 2 AS GridNumber, SourceName, CLM_NBR, CLM_FTR_NBR,
       COUNT_BIG(*) AS FinancialRows, MIN(LOSS_TXN_PRCSS_DT) AS FirstProcessDate,
       MAX(LOSS_TXN_PRCSS_DT) AS LastProcessDate,
       SUM(ISNULL(FTR_CHNG_IN_OSLS_AMT,0)) AS RawReserveSum,
       SUM(ISNULL(FTR_PDLS_INCL_SLVG_SUBRO_AMT,0)-ISNULL(DEDTBL_RECOVRY_AMT,0)) AS RawNetPaidSum,
       SUM(ISNULL(CHNG_IN_ALAE_RSRV_AMT,0)) AS RawALAEReserveSum,
       SUM(ISNULL(FTR_ALAE_AMT-EXPNSE_RECOVRY_AMT,0)) AS RawALAEPaidSum
FROM #Financial
GROUP BY SourceName, CLM_NBR, CLM_FTR_NBR
ORDER BY CLM_NBR, CLM_FTR_NBR, SourceName;

-- Grid 3: exact content differences across the 24 copied snapshot columns.
-- Zero rows is expected when backfill accurately copied this scope.
SELECT 3 AS GridNumber, 'LakeHouse only/different' AS DifferenceKind, d.*
FROM (
 SELECT FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND FROM #Financial WHERE SourceName='LakeHouse'
 EXCEPT
 SELECT FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND FROM #Financial WHERE SourceName='Filtered'
) d
UNION ALL
SELECT 3, 'Filtered only/different', d.*
FROM (
 SELECT FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND FROM #Financial WHERE SourceName='Filtered'
 EXCEPT
 SELECT FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND FROM #Financial WHERE SourceName='LakeHouse'
) d;

-- Grid 4: row-level history; keep ALL rows including negatives and offsets.
SELECT 4 AS GridNumber, *
FROM #Financial
ORDER BY CLM_NBR, CLM_FTR_NBR, SourceName, LOSS_TXN_PRCSS_DT,
         SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND;

-- Grid 5: transaction comparison by key/content. Zero rows expected.
-- Includes source/target monetary scale conversion, without inventing scaling.
SELECT 5 AS GridNumber, 'LakeHouse only/different' AS DifferenceKind, d.*
FROM (
 SELECT TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT,
        CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
 FROM #Transactions WHERE SourceName='LakeHouse'
 EXCEPT
 SELECT TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT,
        CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
 FROM #Transactions WHERE SourceName='Filtered'
) d
UNION ALL
SELECT 5, 'Filtered only/different', d.*
FROM (
 SELECT TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT,
        CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
 FROM #Transactions WHERE SourceName='Filtered'
 EXCEPT
 SELECT TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT,
        CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
 FROM #Transactions WHERE SourceName='LakeHouse'
) d;

-- Grid 6: expense transaction totals using the view's actual expense rule.
SELECT 6 AS GridNumber, SourceName, CLM_NBR, CLM_FTR_NBR,
       CLM_RCRD_TYPE_CD, LOSS_TXN_TYPE_DESC, COST_CATGRY_DESC,
       COUNT_BIG(*) AS TransactionRows, SUM(CLM_TXN_AMT) AS Amount
FROM #Transactions
WHERE LOSS_TXN_TYPE_DESC LIKE '%Expense%'
  AND (COST_CATGRY_DESC <> 'State Penalties' OR COST_CATGRY_DESC IS NULL)
GROUP BY SourceName, CLM_NBR, CLM_FTR_NBR, CLM_RCRD_TYPE_CD,
         LOSS_TXN_TYPE_DESC, COST_CATGRY_DESC
ORDER BY CLM_NBR, CLM_FTR_NBR, SourceName, CLM_RCRD_TYPE_CD, LOSS_TXN_TYPE_DESC;

SELECT f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_KEY, f.CURR_VERSN_IND,
       f.FTR_COVG_TYPE_DESC, f.CLMNT_HCS_SYS_ID, f.VEH_HCS_SYS_ID,
       f.FTR_ASGNED_USER_FULL_NM, f.VERSN_BEG_DT
INTO #Features
FROM CLM_LakeHouse.dbo.DW_CLMS_FTR_DIMNSN f
JOIN #Claims c ON f.CLM_NBR = c.CLM_NBR;

-- Grid 7: current feature versions and matching claim/policy/claimant/vehicle rows.
-- Counts above 1 are candidates for join fan-out, NOT automatic proof of a bug.
-- Counts of 0 on LEFT joins still produce one output row with NULL attributes.
SELECT 7 AS GridNumber, f.*,
       (SELECT COUNT_BIG(*) FROM #Features x
        WHERE x.CLM_NBR=f.CLM_NBR AND x.CLM_FTR_NBR=f.CLM_FTR_NBR
          AND x.CURR_VERSN_IND='Y') AS CurrentFeatureRowsForExposure,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_CLMS_CLM_DIMNSN cd
        WHERE cd.CLM_NBR=f.CLM_NBR AND cd.CURR_VERSN_IND='Y') AS CurrentClaimRows,
       (SELECT COUNT_BIG(*)
        FROM CLM_LakeHouse.dbo.DW_CLMS_CLM_DIMNSN cd
        JOIN CLM_LakeHouse.dbo.DW_POL_DIMNSN p
          ON cd.POL_SYM=p.POL_SYM AND UPPER(cd.POL_NBR)=p.POL_NBR
         AND cd.POL_MOD=p.POL_MOD AND p.POL_CURR_VERSN_IND='Y'
        WHERE cd.CLM_NBR=f.CLM_NBR AND cd.CURR_VERSN_IND='Y'
          AND p.PMS_CUSTMR_NBR='1500744571') AS CurrentClaimPolicyJoinRows,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_CLMS_CLMNT_DIMNSN n
        WHERE n.CLMNT_HCS_SYS_ID=f.CLMNT_HCS_SYS_ID AND n.CURR_VERSN_IND='Y') AS ClaimantMatches,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_CLMS_VEH_DIMNSN v
        WHERE v.VEH_HCS_SYS_ID=f.VEH_HCS_SYS_ID AND v.CURR_VERSN_IND='Y') AS VehicleMatches
FROM #Features f WHERE f.CURR_VERSN_IND='Y'
ORDER BY f.CLM_NBR,f.CLM_FTR_NBR,f.FTR_KEY;

-- Grid 8: inspect auxiliary joins for EACH filtered financial history row.
-- We deliberately do not multiply these counts into a supposed final amount.
SELECT 8 AS GridNumber, f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_KEY, f.CLM_KEY,
       f.LOSS_TXN_PRCSS_DT, f.SRC_ACCT_PRD, f.CLM_RCRD_TYPE_CD, f.OFFSET_IND,
       f.FTR_CHNG_IN_OSLS_AMT,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_CLMS_DRVR_DIMNSN d
        WHERE d.CLM_DRVR_KEY=f.CLM_DRVR_KEY) AS DriverMatches,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_REF_MAJ_PERIL p
        WHERE p.MAJ_PERIL_CD=f.MAJ_PERIL_CD
          AND CONVERT(date,f.RCRD_INSRT_DT) BETWEEN CONVERT(date,p.VERSN_BEG_DT)
                                              AND CONVERT(date,p.VERSN_END_DT)) AS PerilMatches,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_CLMS_LGL_FACT l
        WHERE l.CLM_KEY=f.CLM_KEY AND l.FTR_KEY=f.FTR_KEY
          AND l.CLMS_LGL_KEY=(
              SELECT MAX(x.CLMS_LGL_KEY) FROM CLM_LakeHouse.dbo.DW_CLMS_LGL_FACT x
              WHERE x.CLM_KEY=f.CLM_KEY AND x.FTR_KEY=f.FTR_KEY AND x.EFF_IND=1)
       ) AS LatestLegalFactMatches,
       (SELECT COUNT_BIG(*) FROM CLM_LakeHouse.dbo.DW_CLMS_LGL_FACT l
        JOIN CLM_LakeHouse.dbo.DW_CLMS_LGL_DIMNSN d ON d.CLMS_LGL_KEY=l.CLMS_LGL_KEY
        WHERE l.CLM_KEY=f.CLM_KEY AND l.FTR_KEY=f.FTR_KEY
          AND l.CLMS_LGL_KEY=(
              SELECT MAX(x.CLMS_LGL_KEY) FROM CLM_LakeHouse.dbo.DW_CLMS_LGL_FACT x
              WHERE x.CLM_KEY=f.CLM_KEY AND x.FTR_KEY=f.FTR_KEY AND x.EFF_IND=1)
       ) AS LegalFactDimensionJoinMatches
FROM #Financial f WHERE SourceName='Filtered'
ORDER BY f.CLM_NBR,f.CLM_FTR_NBR,f.LOSS_TXN_PRCSS_DT,f.SRC_ACCT_PRD;

-- Grid 9: actual LIVE view rows. All output columns retained so duplicate
-- exposures can be compared for differing litigation/driver/claimant/etc.
-- This may be slow because outer filters need not push through the full view.
IF @IncludeViewOutput=1
BEGIN
    SELECT 9 AS GridNumber, v.*
    FROM dbo.SAC_Loss_Run v
    WHERE v.[Customer Number]='1500744571'
      AND v.[Claim Number] IN ('83-00028520','83-00073688')
    ORDER BY v.[Claim Number],v.[Exposure]
    OPTION (RECOMPILE);
END;

-- Grid 10: values in Walter's OBIEE attachment dated November 2, 2026.
-- Expense is OBIEE's Expense column, NOT assumed equal to our ALAE reserve+paid.
SELECT 10 AS GridNumber, *
FROM (VALUES
 ('83-00028520','01',48270.76,236842.05,54689.13),
 ('83-00028520','02',43463.72,16447.14,711.76),
 ('83-00073688','01',500.00,48024.84,6975.95),
 ('83-00073688','02',86242.51,81364.51,3593.80)
) e(ClaimNumber,Exposure,OBIEEReserve,OBIEENetPaid,OBIEEExpense);

-- Grid 11: live definition (SSMS grid text may truncate; save/copy full cell).
-- Confirms the deployed SQL rather than assuming the repo view is identical.
SELECT 11 AS GridNumber, OBJECT_DEFINITION(OBJECT_ID('dbo.SAC_Loss_Run')) AS LiveViewDefinition;

DROP TABLE #Features;
DROP TABLE #Transactions;
DROP TABLE #Financial;
DROP TABLE #Claims;
