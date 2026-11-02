USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;
SET XACT_ABORT ON;

-- PREPROD ONLY: one-time backfill for the 10 previously identified inactive accounts.
-- Includes Aubuchon, but existing financial keys (including its pilot) are skipped.
-- Rollback for this batch does NOT undo the earlier Aubuchon pilot.
-- Stop local/Azure loss-run workers and coordinate with the filtered-table loader before applying.
-- All source scans are staged before the short insert transaction. No backend changes.
-- First run with @Apply = 0 and save all results. Only then rerun with @Apply = 1.
-- Copies ALL matching historical source rows, unchanged (no latest-row selection).
-- This is an experiment, not a verified replacement for the existing ETL rules.
-- Permanent audit tables are created ONLY on apply and contain exactly inserted rows.
-- Keep them until the pilot is approved or rolled back. Do not edit/drop them.
DECLARE @Apply bit = 0;

IF DB_NAME() <> 'CLMAA_SpecialAccounts'
 OR CONVERT(nvarchar(256), SERVERPROPERTY('ServerName')) NOT LIKE 'clms-preprd-sqlmanagedinstance%'
    THROW 50001, 'This backfill is restricted to the known PREPROD server/database.', 1;
IF OBJECT_ID('dbo.SAC_InactiveBackfill_SnapshotInserted') IS NOT NULL
 OR OBJECT_ID('dbo.SAC_InactiveBackfill_TxnInserted') IS NOT NULL
    THROW 50003, 'Inactive-backfill audit tables already exist. Review the prior run; do not drop them to bypass this check.', 1;

DROP TABLE IF EXISTS #InactiveAccounts;
DROP TABLE IF EXISTS #AccountClaims;
DROP TABLE IF EXISTS #PilotClaims;
DROP TABLE IF EXISTS #Snapshot;
DROP TABLE IF EXISTS #Txn;
-- Fixed approved test scope, not every inactive account that might be added later.
SELECT DISTINCT a.CustomerNum, a.CustomerName
INTO #InactiveAccounts
FROM dbo.tblAcctSpecial a
WHERE a.CustomerNum IN (
    '0033165294', '1500744571', '1504423911', '0019463319', '0025077990',
    '1500426952', '1500253888', '1507487766', '1505762079', '1500773492'
)
AND a.AcctStatus = 'Inactive'
AND a.LossRunDistFreq <> 'Not Needed' AND a.LossRunDistFreq <> '';
IF (SELECT COUNT(*) FROM #InactiveAccounts) <> 10
    THROW 50011, 'Expected 10 eligible inactive accounts; review account/status changes.', 1;

SELECT DISTINCT a.CustomerNum, c.CLM_NBR
INTO #AccountClaims
FROM #InactiveAccounts a
JOIN CLM_LakeHouse.dbo.DW_POL_DIMNSN p
 ON p.PMS_CUSTMR_NBR = a.CustomerNum AND p.POL_CURR_VERSN_IND = 'Y'
JOIN CLM_LakeHouse.dbo.DW_CLMS_CLM_DIMNSN c
 ON c.POL_SYM = p.POL_SYM AND UPPER(c.POL_NBR) = p.POL_NBR
 AND c.POL_MOD = p.POL_MOD AND c.CURR_VERSN_IND = 'Y'
WHERE UPPER(ISNULL(c.ACCT_LOSS_LOC_ID, 'NA')) NOT LIKE '%REKEY%';

-- A claim shared by account mappings is loaded only once.
SELECT DISTINCT CLM_NBR INTO #PilotClaims FROM #AccountClaims;
IF NOT EXISTS (SELECT 1 FROM #PilotClaims)
    THROW 50004, 'No eligible inactive-account claims found.', 1;
CREATE UNIQUE CLUSTERED INDEX IX_Claims ON #PilotClaims(CLM_NBR);

-- Source/target metadata previously checked in the assessment. Source amounts
-- must fit the target decimal(18,2); do not silently truncate/scale amounts.

SELECT s.FTR_KEY, s.CLM_KEY, s.LOSS_TXN_PRCSS_DT_DIMKEY, s.CLM_NBR, s.CLM_FTR_NBR, s.FTR_ALAE_AMT, s.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, s.FTR_PDLS_INCL_SLVG_SUBRO_AMT, s.FTR_INCUR_INCL_SLVG_SUBRO_AMT, s.FTR_SLVG_AMT, s.FTR_SUBRO_AMT, s.FTR_CHNG_IN_OSLS_AMT, s.RCRD_INSRT_DT, s.CLM_DRVR_KEY, s.INCID_KEY, s.MAJ_PERIL_CD, s.DEDTBL_RECOVRY_AMT, s.CHNG_IN_ALAE_RSRV_AMT, s.LOSS_RECOVRY_AMT, s.EXPNSE_RECOVRY_AMT, s.LOSS_TXN_PRCSS_DT, s.SRC_ACCT_PRD, s.CLM_RCRD_TYPE_CD, s.OFFSET_IND
INTO #Snapshot
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F s
JOIN #PilotClaims c ON s.CLM_NBR = c.CLM_NBR COLLATE DATABASE_DEFAULT
OPTION (RECOMPILE);
IF EXISTS (SELECT 1 FROM #Snapshot GROUP BY LOSS_TXN_PRCSS_DT, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND HAVING COUNT_BIG(*) > 1)
    THROW 50005, 'Duplicate source primary keys in Snapshot staging.', 1;
IF EXISTS (SELECT 1 FROM #Snapshot WHERE LOSS_TXN_PRCSS_DT IS NULL OR LOSS_TXN_PRCSS_DT_DIMKEY IS NULL OR CLM_NBR IS NULL OR CLM_FTR_NBR IS NULL OR SRC_ACCT_PRD IS NULL OR CLM_RCRD_TYPE_CD IS NULL OR OFFSET_IND IS NULL)
    THROW 50006, 'NULL primary key in Snapshot staging.', 1;
SELECT 'Snapshot' AS SourceKind, COUNT_BIG(*) AS SourceRows,
       COUNT(DISTINCT s.CLM_NBR) AS Claims,
       (SELECT COUNT_BIG(*) FROM #Snapshot x WHERE NOT EXISTS
        (SELECT 1 FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED t WHERE t.LOSS_TXN_PRCSS_DT = x.LOSS_TXN_PRCSS_DT AND t.LOSS_TXN_PRCSS_DT_DIMKEY = x.LOSS_TXN_PRCSS_DT_DIMKEY AND t.CLM_NBR = x.CLM_NBR AND t.CLM_FTR_NBR = x.CLM_FTR_NBR AND t.SRC_ACCT_PRD = x.SRC_ACCT_PRD AND t.CLM_RCRD_TYPE_CD = x.CLM_RCRD_TYPE_CD AND t.OFFSET_IND = x.OFFSET_IND)) AS RowsToInsert
FROM #Snapshot s;

SELECT s.TXN_FACT_KEY, s.ACCT_PRD, s.LOSS_TXN_TYPE_DESC, s.CLM_TXN_AMT, s.RCRD_INSRT_DT, s.CLM_RCRD_TYPE_CD, s.CLM_NBR, s.CLM_FTR_NBR, s.COST_CATGRY_DESC
INTO #Txn
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_TXN_SS_F s
JOIN #PilotClaims c ON s.CLM_NBR = c.CLM_NBR COLLATE DATABASE_DEFAULT
OPTION (RECOMPILE);
IF EXISTS (SELECT 1 FROM #Txn GROUP BY TXN_FACT_KEY, ACCT_PRD, CLM_RCRD_TYPE_CD HAVING COUNT_BIG(*) > 1)
    THROW 50005, 'Duplicate source primary keys in Txn staging.', 1;
IF EXISTS (SELECT 1 FROM #Txn WHERE TXN_FACT_KEY IS NULL OR ACCT_PRD IS NULL OR CLM_RCRD_TYPE_CD IS NULL)
    THROW 50006, 'NULL primary key in Txn staging.', 1;
IF EXISTS (SELECT 1 FROM #Txn
           WHERE CLM_TXN_AMT IS NOT NULL
             AND TRY_CONVERT(decimal(18,2), CLM_TXN_AMT) IS NULL)
    THROW 50010, 'Transaction amount does not fit the target decimal(18,2).', 1;
SELECT 'Txn' AS SourceKind, COUNT_BIG(*) AS SourceRows,
       COUNT(DISTINCT s.CLM_NBR) AS Claims,
       (SELECT COUNT_BIG(*) FROM #Txn x WHERE NOT EXISTS
        (SELECT 1 FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t WHERE t.TXN_FACT_KEY = x.TXN_FACT_KEY AND t.ACCT_PRD = x.ACCT_PRD AND t.CLM_RCRD_TYPE_CD = x.CLM_RCRD_TYPE_CD)) AS RowsToInsert
FROM #Txn s;

-- Per-account diagnostics: zero-financial claims are retained in scope.
-- Existing rows are skipped, NOT refreshed even if source values differ.
SELECT a.CustomerNum AS CustomerNumber, a.CustomerName,
       (SELECT COUNT(*) FROM #AccountClaims c WHERE c.CustomerNum = a.CustomerNum) AS ClaimsInScope,
       (SELECT COUNT_BIG(*) FROM #Snapshot s WHERE EXISTS
          (SELECT 1 FROM #AccountClaims c WHERE c.CustomerNum = a.CustomerNum
           AND s.CLM_NBR = c.CLM_NBR COLLATE DATABASE_DEFAULT)) AS SourceSnapshotRows,
       (SELECT COUNT_BIG(*) FROM #Txn t WHERE EXISTS
          (SELECT 1 FROM #AccountClaims c WHERE c.CustomerNum = a.CustomerNum
           AND t.CLM_NBR = c.CLM_NBR COLLATE DATABASE_DEFAULT)) AS SourceTransactionRows
FROM #InactiveAccounts a ORDER BY a.CustomerNum;

SELECT @@SERVERNAME AS ServerName,
       (SELECT COUNT(*) FROM #InactiveAccounts) AS EligibleAccounts,
       (SELECT COUNT(*) FROM #PilotClaims) AS UniqueClaimsInScope,
       @Apply AS ApplyRequested;
-- Original inventory: 10 accounts / 3,106 unique claims. Review any claim-count
-- change before applying; claims can change as upstream data is refreshed.
IF NOT EXISTS (SELECT 1 FROM #Snapshot) OR NOT EXISTS (SELECT 1 FROM #Txn)
    THROW 50007, 'One financial source is empty. Stop and investigate.', 1;
IF @Apply = 0
BEGIN
    PRINT 'PREVIEW ONLY: nothing was inserted. Save results before applying.';
    RETURN;
END;

BEGIN TRY
    BEGIN TRANSACTION;
    DECLARE @LockResult int;
    EXEC @LockResult = sys.sp_getapplock
         @Resource = 'SAC_Aubuchon_Financial_Pilot', @LockMode = 'Exclusive',
         @LockOwner = 'Transaction', @LockTimeout = 0;
    IF @LockResult < 0 THROW 50008, 'Another pilot operation is running.', 1;
    IF OBJECT_ID('dbo.SAC_InactiveBackfill_SnapshotInserted') IS NOT NULL
     OR OBJECT_ID('dbo.SAC_InactiveBackfill_TxnInserted') IS NOT NULL
        THROW 50009, 'Inactive backfill already applied by another session.', 1;

    SELECT TOP (0) FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND
    INTO dbo.SAC_InactiveBackfill_SnapshotInserted
    FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED;

    INSERT INTO dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED (FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND)
    OUTPUT inserted.FTR_KEY, inserted.CLM_KEY, inserted.LOSS_TXN_PRCSS_DT_DIMKEY, inserted.CLM_NBR, inserted.CLM_FTR_NBR, inserted.FTR_ALAE_AMT, inserted.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, inserted.FTR_PDLS_INCL_SLVG_SUBRO_AMT, inserted.FTR_INCUR_INCL_SLVG_SUBRO_AMT, inserted.FTR_SLVG_AMT, inserted.FTR_SUBRO_AMT, inserted.FTR_CHNG_IN_OSLS_AMT, inserted.RCRD_INSRT_DT, inserted.CLM_DRVR_KEY, inserted.INCID_KEY, inserted.MAJ_PERIL_CD, inserted.DEDTBL_RECOVRY_AMT, inserted.CHNG_IN_ALAE_RSRV_AMT, inserted.LOSS_RECOVRY_AMT, inserted.EXPNSE_RECOVRY_AMT, inserted.LOSS_TXN_PRCSS_DT, inserted.SRC_ACCT_PRD, inserted.CLM_RCRD_TYPE_CD, inserted.OFFSET_IND
      INTO dbo.SAC_InactiveBackfill_SnapshotInserted (FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND)
    SELECT s.FTR_KEY, s.CLM_KEY, s.LOSS_TXN_PRCSS_DT_DIMKEY, s.CLM_NBR, s.CLM_FTR_NBR, s.FTR_ALAE_AMT, s.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, s.FTR_PDLS_INCL_SLVG_SUBRO_AMT, s.FTR_INCUR_INCL_SLVG_SUBRO_AMT, s.FTR_SLVG_AMT, s.FTR_SUBRO_AMT, s.FTR_CHNG_IN_OSLS_AMT, s.RCRD_INSRT_DT, s.CLM_DRVR_KEY, s.INCID_KEY, s.MAJ_PERIL_CD, s.DEDTBL_RECOVRY_AMT, s.CHNG_IN_ALAE_RSRV_AMT, s.LOSS_RECOVRY_AMT, s.EXPNSE_RECOVRY_AMT, s.LOSS_TXN_PRCSS_DT, s.SRC_ACCT_PRD, s.CLM_RCRD_TYPE_CD, s.OFFSET_IND
    FROM #Snapshot s
    WHERE NOT EXISTS (SELECT 1 FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED t WITH (UPDLOCK, HOLDLOCK)
                      WHERE t.LOSS_TXN_PRCSS_DT = s.LOSS_TXN_PRCSS_DT AND t.LOSS_TXN_PRCSS_DT_DIMKEY = s.LOSS_TXN_PRCSS_DT_DIMKEY AND t.CLM_NBR = s.CLM_NBR AND t.CLM_FTR_NBR = s.CLM_FTR_NBR AND t.SRC_ACCT_PRD = s.SRC_ACCT_PRD AND t.CLM_RCRD_TYPE_CD = s.CLM_RCRD_TYPE_CD AND t.OFFSET_IND = s.OFFSET_IND);

    SELECT TOP (0) TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT, CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
    INTO dbo.SAC_InactiveBackfill_TxnInserted
    FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED;

    INSERT INTO dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED (TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT, CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC)
    OUTPUT inserted.TXN_FACT_KEY, inserted.ACCT_PRD, inserted.LOSS_TXN_TYPE_DESC, inserted.CLM_TXN_AMT, inserted.RCRD_INSRT_DT, inserted.CLM_RCRD_TYPE_CD, inserted.CLM_NBR, inserted.CLM_FTR_NBR, inserted.COST_CATGRY_DESC
      INTO dbo.SAC_InactiveBackfill_TxnInserted (TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT, CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC)
    SELECT s.TXN_FACT_KEY, s.ACCT_PRD, s.LOSS_TXN_TYPE_DESC, s.CLM_TXN_AMT, s.RCRD_INSRT_DT, s.CLM_RCRD_TYPE_CD, s.CLM_NBR, s.CLM_FTR_NBR, s.COST_CATGRY_DESC
    FROM #Txn s
    WHERE NOT EXISTS (SELECT 1 FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t WITH (UPDLOCK, HOLDLOCK)
                      WHERE t.TXN_FACT_KEY = s.TXN_FACT_KEY AND t.ACCT_PRD = s.ACCT_PRD AND t.CLM_RCRD_TYPE_CD = s.CLM_RCRD_TYPE_CD);

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;

SELECT 'Snapshot' AS SourceKind, COUNT_BIG(*) AS InsertedRows
FROM dbo.SAC_InactiveBackfill_SnapshotInserted
UNION ALL
SELECT 'Transactions', COUNT_BIG(*) FROM dbo.SAC_InactiveBackfill_TxnInserted;
PRINT 'Inactive backfill committed. Keep audit tables. Generate and validate standard and claim-review reports for these inactive accounts.';
