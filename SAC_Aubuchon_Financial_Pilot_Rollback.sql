USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;
SET XACT_ABORT ON;
-- Stop workers and coordinate with the loader before rollback.
-- Preview default. Removes ONLY rows recorded by the Aubuchon pilot.
-- Refuses rollback if an inserted row has since changed or disappeared.
-- Audit tables are kept for review; rerunning after rollback will fail safely.
DECLARE @ApplyRollback bit = 0;
IF CONVERT(nvarchar(256), SERVERPROPERTY('ServerName')) NOT LIKE 'clms-preprd-sqlmanagedinstance%'
 OR DB_NAME() <> 'CLMAA_SpecialAccounts'
    THROW 50100, 'PREPROD only.', 1;
IF OBJECT_ID('dbo.SAC_AubuchonPilot_SnapshotInserted') IS NULL
 OR OBJECT_ID('dbo.SAC_AubuchonPilot_TxnInserted') IS NULL
    THROW 50101, 'Pilot audit tables are missing. Do not guess which rows to delete.', 1;

SELECT 'Snapshot' AS SourceKind, COUNT_BIG(*) AS RecordedPilotRows
FROM dbo.SAC_AubuchonPilot_SnapshotInserted
UNION ALL
SELECT 'Transactions', COUNT_BIG(*) FROM dbo.SAC_AubuchonPilot_TxnInserted;
IF @ApplyRollback = 0
BEGIN
    PRINT 'PREVIEW ONLY. Set @ApplyRollback = 1 only to undo this pilot.';
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
        SELECT 1 FROM dbo.SAC_AubuchonPilot_SnapshotInserted b
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
        THROW 50103, 'Snapshot pilot rows changed/disappeared. Automatic rollback refused.', 1;

    IF EXISTS (
        SELECT 1 FROM dbo.SAC_AubuchonPilot_TxnInserted b
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
        THROW 50103, 'Txn pilot rows changed/disappeared. Automatic rollback refused.', 1;

    DELETE t
    FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED t
    JOIN dbo.SAC_AubuchonPilot_SnapshotInserted b
      ON t.LOSS_TXN_PRCSS_DT = b.LOSS_TXN_PRCSS_DT AND t.LOSS_TXN_PRCSS_DT_DIMKEY = b.LOSS_TXN_PRCSS_DT_DIMKEY AND t.CLM_NBR = b.CLM_NBR AND t.CLM_FTR_NBR = b.CLM_FTR_NBR AND t.SRC_ACCT_PRD = b.SRC_ACCT_PRD AND t.CLM_RCRD_TYPE_CD = b.CLM_RCRD_TYPE_CD AND t.OFFSET_IND = b.OFFSET_IND;
    SELECT 'Snapshot' AS SourceKind, @@ROWCOUNT AS DeletedPilotRows;

    DELETE t
    FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t
    JOIN dbo.SAC_AubuchonPilot_TxnInserted b
      ON t.TXN_FACT_KEY = b.TXN_FACT_KEY AND t.ACCT_PRD = b.ACCT_PRD AND t.CLM_RCRD_TYPE_CD = b.CLM_RCRD_TYPE_CD;
    SELECT 'Txn' AS SourceKind, @@ROWCOUNT AS DeletedPilotRows;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
PRINT 'Pilot rolled back. Audit tables retained; do not reapply without review.';
