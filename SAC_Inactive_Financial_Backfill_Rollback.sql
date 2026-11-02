USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;
SET XACT_ABORT ON;
-- Stop workers and coordinate with the loader before rollback.
-- Preview default. Undoes BOTH the original Aubuchon pilot and the later
-- inactive-account backfill, using their recorded inserted rows only.
-- Does not remove pre-existing rows. Both sets of audit tables must be retained.
-- Refuses rollback if an inserted row has since changed or disappeared.
-- Audit tables are kept for review; rerunning after rollback will fail safely.
DECLARE @ApplyRollback bit = 1;
IF CONVERT(nvarchar(256), SERVERPROPERTY('ServerName')) NOT LIKE 'clms-preprd-sqlmanagedinstance%'
 OR DB_NAME() <> 'CLMAA_SpecialAccounts'
    THROW 50100, 'PREPROD only.', 1;
IF OBJECT_ID('dbo.SAC_InactiveBackfill_SnapshotInserted') IS NULL
 OR OBJECT_ID('dbo.SAC_InactiveBackfill_TxnInserted') IS NULL
    THROW 50101, 'Inactive-backfill audit tables are missing. Do not guess which rows to delete.', 1;

IF OBJECT_ID('dbo.SAC_AubuchonPilot_SnapshotInserted') IS NULL
 OR OBJECT_ID('dbo.SAC_AubuchonPilot_TxnInserted') IS NULL
    THROW 50104, 'Original Aubuchon audit tables are missing. Combined rollback refused.', 1;

DROP TABLE IF EXISTS #RollbackSnapshot;
DROP TABLE IF EXISTS #RollbackTxn;
-- UNION removes identical overlap; conflicting copies of a key are rejected.
SELECT FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND
INTO #RollbackSnapshot
FROM dbo.SAC_InactiveBackfill_SnapshotInserted
UNION
SELECT FTR_KEY, CLM_KEY, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR, CLM_FTR_NBR, FTR_ALAE_AMT, FTR_PDLS_EXCL_SLVG_SUBRO_AMT, FTR_PDLS_INCL_SLVG_SUBRO_AMT, FTR_INCUR_INCL_SLVG_SUBRO_AMT, FTR_SLVG_AMT, FTR_SUBRO_AMT, FTR_CHNG_IN_OSLS_AMT, RCRD_INSRT_DT, CLM_DRVR_KEY, INCID_KEY, MAJ_PERIL_CD, DEDTBL_RECOVRY_AMT, CHNG_IN_ALAE_RSRV_AMT, LOSS_RECOVRY_AMT, EXPNSE_RECOVRY_AMT, LOSS_TXN_PRCSS_DT, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND
FROM dbo.SAC_AubuchonPilot_SnapshotInserted;
-- UNION removes identical overlap; conflicting copies of a key are rejected.
SELECT TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT, CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
INTO #RollbackTxn
FROM dbo.SAC_InactiveBackfill_TxnInserted
UNION
SELECT TXN_FACT_KEY, ACCT_PRD, LOSS_TXN_TYPE_DESC, CLM_TXN_AMT, RCRD_INSRT_DT, CLM_RCRD_TYPE_CD, CLM_NBR, CLM_FTR_NBR, COST_CATGRY_DESC
FROM dbo.SAC_AubuchonPilot_TxnInserted;
IF EXISTS (
    SELECT 1 FROM #RollbackSnapshot
    GROUP BY LOSS_TXN_PRCSS_DT, LOSS_TXN_PRCSS_DT_DIMKEY, CLM_NBR,
             CLM_FTR_NBR, SRC_ACCT_PRD, CLM_RCRD_TYPE_CD, OFFSET_IND
    HAVING COUNT_BIG(*) > 1
) OR EXISTS (
    SELECT 1 FROM #RollbackTxn
    GROUP BY TXN_FACT_KEY, ACCT_PRD, CLM_RCRD_TYPE_CD HAVING COUNT_BIG(*) > 1
)
    THROW 50105, 'Conflicting audit rows for the same financial key. Rollback refused.', 1;

SELECT 'Snapshot' AS SourceKind, COUNT_BIG(*) AS RecordedBackfillRows
FROM #RollbackSnapshot
UNION ALL
SELECT 'Transactions', COUNT_BIG(*) FROM #RollbackTxn;
IF @ApplyRollback = 0
BEGIN
    PRINT 'PREVIEW ONLY. Set @ApplyRollback = 1 to undo BOTH Aubuchon pilot and inactive-account backfill.';
    RETURN;
END;
BEGIN TRY
    BEGIN TRANSACTION;
    DECLARE @LockResult int;
    EXEC @LockResult = sys.sp_getapplock
         @Resource = 'SAC_Aubuchon_Financial_Pilot', @LockMode = 'Exclusive',
         @LockOwner = 'Transaction', @LockTimeout = 0;
    IF @LockResult < 0 THROW 50102, 'Another pilot operation is running.', 1;

    IF EXISTS (
        SELECT 1 FROM #RollbackSnapshot b
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED t WITH (UPDLOCK, HOLDLOCK)
            WHERE t.LOSS_TXN_PRCSS_DT = b.LOSS_TXN_PRCSS_DT AND t.LOSS_TXN_PRCSS_DT_DIMKEY = b.LOSS_TXN_PRCSS_DT_DIMKEY AND t.CLM_NBR = b.CLM_NBR AND t.CLM_FTR_NBR = b.CLM_FTR_NBR AND t.SRC_ACCT_PRD = b.SRC_ACCT_PRD AND t.CLM_RCRD_TYPE_CD = b.CLM_RCRD_TYPE_CD AND t.OFFSET_IND = b.OFFSET_IND
              AND NOT EXISTS (
                  SELECT t.FTR_KEY, t.CLM_KEY, t.LOSS_TXN_PRCSS_DT_DIMKEY, t.CLM_NBR, t.CLM_FTR_NBR, t.FTR_ALAE_AMT, t.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, t.FTR_PDLS_INCL_SLVG_SUBRO_AMT, t.FTR_INCUR_INCL_SLVG_SUBRO_AMT, t.FTR_SLVG_AMT, t.FTR_SUBRO_AMT, t.FTR_CHNG_IN_OSLS_AMT, t.RCRD_INSRT_DT, t.CLM_DRVR_KEY, t.INCID_KEY, t.MAJ_PERIL_CD, t.DEDTBL_RECOVRY_AMT, t.CHNG_IN_ALAE_RSRV_AMT, t.LOSS_RECOVRY_AMT, t.EXPNSE_RECOVRY_AMT, t.LOSS_TXN_PRCSS_DT, t.SRC_ACCT_PRD, t.CLM_RCRD_TYPE_CD, t.OFFSET_IND
                  EXCEPT
                  SELECT b.FTR_KEY, b.CLM_KEY, b.LOSS_TXN_PRCSS_DT_DIMKEY, b.CLM_NBR, b.CLM_FTR_NBR, b.FTR_ALAE_AMT, b.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, b.FTR_PDLS_INCL_SLVG_SUBRO_AMT, b.FTR_INCUR_INCL_SLVG_SUBRO_AMT, b.FTR_SLVG_AMT, b.FTR_SUBRO_AMT, b.FTR_CHNG_IN_OSLS_AMT, b.RCRD_INSRT_DT, b.CLM_DRVR_KEY, b.INCID_KEY, b.MAJ_PERIL_CD, b.DEDTBL_RECOVRY_AMT, b.CHNG_IN_ALAE_RSRV_AMT, b.LOSS_RECOVRY_AMT, b.EXPNSE_RECOVRY_AMT, b.LOSS_TXN_PRCSS_DT, b.SRC_ACCT_PRD, b.CLM_RCRD_TYPE_CD, b.OFFSET_IND
              )
        )
    )
        THROW 50103, 'Snapshot backfill rows changed/disappeared. Automatic rollback refused.', 1;

    IF EXISTS (
        SELECT 1 FROM #RollbackTxn b
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t WITH (UPDLOCK, HOLDLOCK)
            WHERE t.TXN_FACT_KEY = b.TXN_FACT_KEY AND t.ACCT_PRD = b.ACCT_PRD AND t.CLM_RCRD_TYPE_CD = b.CLM_RCRD_TYPE_CD
              AND NOT EXISTS (
                  SELECT t.TXN_FACT_KEY, t.ACCT_PRD, t.LOSS_TXN_TYPE_DESC, t.CLM_TXN_AMT, t.RCRD_INSRT_DT, t.CLM_RCRD_TYPE_CD, t.CLM_NBR, t.CLM_FTR_NBR, t.COST_CATGRY_DESC
                  EXCEPT
                  SELECT b.TXN_FACT_KEY, b.ACCT_PRD, b.LOSS_TXN_TYPE_DESC, b.CLM_TXN_AMT, b.RCRD_INSRT_DT, b.CLM_RCRD_TYPE_CD, b.CLM_NBR, b.CLM_FTR_NBR, b.COST_CATGRY_DESC
              )
        )
    )
        THROW 50103, 'Txn backfill rows changed/disappeared. Automatic rollback refused.', 1;

    DELETE t
    FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED t
    JOIN #RollbackSnapshot b
      ON t.LOSS_TXN_PRCSS_DT = b.LOSS_TXN_PRCSS_DT AND t.LOSS_TXN_PRCSS_DT_DIMKEY = b.LOSS_TXN_PRCSS_DT_DIMKEY AND t.CLM_NBR = b.CLM_NBR AND t.CLM_FTR_NBR = b.CLM_FTR_NBR AND t.SRC_ACCT_PRD = b.SRC_ACCT_PRD AND t.CLM_RCRD_TYPE_CD = b.CLM_RCRD_TYPE_CD AND t.OFFSET_IND = b.OFFSET_IND;
    SELECT 'Snapshot' AS SourceKind, @@ROWCOUNT AS DeletedBackfillRows;

    DELETE t
    FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t
    JOIN #RollbackTxn b
      ON t.TXN_FACT_KEY = b.TXN_FACT_KEY AND t.ACCT_PRD = b.ACCT_PRD AND t.CLM_RCRD_TYPE_CD = b.CLM_RCRD_TYPE_CD;
    SELECT 'Txn' AS SourceKind, @@ROWCOUNT AS DeletedBackfillRows;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
PRINT 'Aubuchon pilot and inactive backfill rolled back. Audit tables retained; do not reapply without review.';
