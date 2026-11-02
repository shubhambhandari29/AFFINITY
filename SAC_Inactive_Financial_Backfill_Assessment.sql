USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;

-- Step 1: read-only backfill assessment. No application table is modified.
-- Run the entire file in one SSMS window. It creates temporary tables only.
-- Default pilot: Aubuchon. Change the value to NULL to assess all eligible
-- inactive accounts after reviewing the pilot results.
DECLARE @CustomerNumber varchar(50) = '1500744571';

-- This can take time: each financial source is read by one bulk statement,
-- rather than running one financial-table lookup per claim.
-- Reference samples are existing populated filtered-table rows. They help
-- determine whether the current load copies, aggregates or transforms values.
-- Counts/sums below are diagnostics, NOT approved report financial totals.

DROP TABLE IF EXISTS #AccountClaims;
DROP TABLE IF EXISTS #SnapshotScope;
DROP TABLE IF EXISTS #TxnScope;
DROP TABLE IF EXISTS #ReferenceSnapshot;
DROP TABLE IF EXISTS #ReferenceTxn;
DROP TABLE IF EXISTS #RawSnapshot;
DROP TABLE IF EXISTS #FilteredSnapshot;
DROP TABLE IF EXISTS #RawTxn;
DROP TABLE IF EXISTS #FilteredTxn;

SELECT DISTINCT a.CustomerNum, a.CustomerName, c.CLM_NBR AS ClaimNumber
INTO #AccountClaims
FROM dbo.tblAcctSpecial a
JOIN CLM_LakeHouse.dbo.DW_POL_DIMNSN p
  ON p.PMS_CUSTMR_NBR = a.CustomerNum AND p.POL_CURR_VERSN_IND = 'Y'
JOIN CLM_LakeHouse.dbo.DW_CLMS_CLM_DIMNSN c
  ON c.POL_SYM = p.POL_SYM AND UPPER(c.POL_NBR) = p.POL_NBR
 AND c.POL_MOD = p.POL_MOD AND c.CURR_VERSN_IND = 'Y'
WHERE a.AcctStatus = 'Inactive'
  AND a.LossRunDistFreq <> 'Not Needed' AND a.LossRunDistFreq <> ''
  AND (@CustomerNumber IS NULL OR a.CustomerNum = @CustomerNumber)
  AND UPPER(ISNULL(c.ACCT_LOSS_LOC_ID, 'NA')) NOT LIKE '%REKEY%';

-- Grid 1: confirm environment and scope. No final history filter is applied.
SELECT @@SERVERNAME AS ServerName, @CustomerNumber AS PilotCustomerNumber,
       COUNT(DISTINCT CustomerNum) AS AccountsWithClaims,
       COUNT(DISTINCT ClaimNumber) AS UniqueClaims
FROM #AccountClaims;

IF NOT EXISTS (SELECT 1 FROM #AccountClaims)
    THROW 50001, 'No claims match the selected inactive account scope.', 1;

-- Grid 2: compare target column types with their source equivalents.
-- Differences in monetary scale/type or missing source columns need review.
SELECT 'Snapshot' AS SourceKind, tc.column_id AS ColumnPosition,
       tc.name COLLATE DATABASE_DEFAULT AS TargetColumn, tt.name COLLATE DATABASE_DEFAULT AS TargetType,
       tc.max_length AS TargetMaxLengthBytes, tc.precision AS TargetPrecision,
       tc.scale AS TargetScale, tc.is_nullable AS TargetNullable,
       st.name COLLATE DATABASE_DEFAULT AS SourceType, sc.max_length AS SourceMaxLengthBytes,
       sc.precision AS SourcePrecision, sc.scale AS SourceScale
FROM sys.columns tc
JOIN sys.types tt ON tt.user_type_id = tc.user_type_id
LEFT JOIN CLM_LakeHouse.sys.columns sc
  ON sc.object_id = OBJECT_ID('CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F')
 AND sc.name COLLATE DATABASE_DEFAULT = tc.name
LEFT JOIN CLM_LakeHouse.sys.types st ON st.user_type_id = sc.user_type_id
WHERE tc.object_id = OBJECT_ID('dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED')
UNION ALL
SELECT 'Transactions', tc.column_id, tc.name COLLATE DATABASE_DEFAULT, tt.name COLLATE DATABASE_DEFAULT, tc.max_length,
       tc.precision, tc.scale, tc.is_nullable, st.name COLLATE DATABASE_DEFAULT, sc.max_length, sc.precision, sc.scale
FROM sys.columns tc
JOIN sys.types tt ON tt.user_type_id = tc.user_type_id
LEFT JOIN CLM_LakeHouse.sys.columns sc
  ON sc.object_id = OBJECT_ID('CLM_LakeHouse.dbo.DW_FINCL_LOSS_TXN_SS_F')
 AND sc.name COLLATE DATABASE_DEFAULT = tc.name
LEFT JOIN CLM_LakeHouse.sys.types st ON st.user_type_id = sc.user_type_id
WHERE tc.object_id = OBJECT_ID('dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED')
ORDER BY SourceKind, ColumnPosition;

SELECT TOP (5) *
INTO #ReferenceSnapshot
FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED
WHERE CLM_NBR IS NOT NULL AND FTR_KEY <> 0 AND CLM_KEY <> 0
  AND (ISNULL(FTR_CHNG_IN_OSLS_AMT, 0) <> 0
       OR ISNULL(FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0) <> 0);

SELECT TOP (5) *
INTO #ReferenceTxn
FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED
WHERE CLM_NBR IS NOT NULL AND TXN_FACT_KEY <> 0 AND ISNULL(CLM_TXN_AMT, 0) <> 0;

-- Derive each scope's string type/collation from its physical source, avoiding
-- conversion of an indexed source claim-number column just to join the scope.
SELECT TOP (0) CLM_NBR INTO #SnapshotScope FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F;
INSERT INTO #SnapshotScope (CLM_NBR)
SELECT ClaimNumber COLLATE DATABASE_DEFAULT FROM #AccountClaims
UNION
SELECT CLM_NBR COLLATE DATABASE_DEFAULT FROM #ReferenceSnapshot
UNION
SELECT CLM_NBR COLLATE DATABASE_DEFAULT FROM #ReferenceTxn;
CREATE UNIQUE CLUSTERED INDEX IX_SnapshotScope ON #SnapshotScope (CLM_NBR);

SELECT TOP (0) CLM_NBR INTO #TxnScope FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_TXN_SS_F;
INSERT INTO #TxnScope (CLM_NBR)
SELECT CLM_NBR FROM #SnapshotScope;
CREATE UNIQUE CLUSTERED INDEX IX_TxnScope ON #TxnScope (CLM_NBR);

DECLARE @StartedAt datetime2 = SYSUTCDATETIME();

SELECT f.FTR_KEY, f.CLM_KEY, f.LOSS_TXN_PRCSS_DT_DIMKEY, f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_ALAE_AMT, f.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, f.FTR_INCUR_INCL_SLVG_SUBRO_AMT, f.FTR_SLVG_AMT, f.FTR_SUBRO_AMT, f.FTR_CHNG_IN_OSLS_AMT, f.RCRD_INSRT_DT, f.CLM_DRVR_KEY, f.INCID_KEY, f.MAJ_PERIL_CD, f.DEDTBL_RECOVRY_AMT, f.CHNG_IN_ALAE_RSRV_AMT, f.LOSS_RECOVRY_AMT, f.EXPNSE_RECOVRY_AMT, f.LOSS_TXN_PRCSS_DT, f.SRC_ACCT_PRD, f.CLM_RCRD_TYPE_CD, f.OFFSET_IND
INTO #RawSnapshot
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F f
JOIN #SnapshotScope s ON f.CLM_NBR = s.CLM_NBR
OPTION (RECOMPILE);
CREATE INDEX IX_Claim ON #RawSnapshot (CLM_NBR);

SELECT f.FTR_KEY, f.CLM_KEY, f.LOSS_TXN_PRCSS_DT_DIMKEY, f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_ALAE_AMT, f.FTR_PDLS_EXCL_SLVG_SUBRO_AMT, f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, f.FTR_INCUR_INCL_SLVG_SUBRO_AMT, f.FTR_SLVG_AMT, f.FTR_SUBRO_AMT, f.FTR_CHNG_IN_OSLS_AMT, f.RCRD_INSRT_DT, f.CLM_DRVR_KEY, f.INCID_KEY, f.MAJ_PERIL_CD, f.DEDTBL_RECOVRY_AMT, f.CHNG_IN_ALAE_RSRV_AMT, f.LOSS_RECOVRY_AMT, f.EXPNSE_RECOVRY_AMT, f.LOSS_TXN_PRCSS_DT, f.SRC_ACCT_PRD, f.CLM_RCRD_TYPE_CD, f.OFFSET_IND
INTO #FilteredSnapshot
FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED f
JOIN #SnapshotScope s ON f.CLM_NBR = s.CLM_NBR
OPTION (RECOMPILE);
CREATE INDEX IX_Claim ON #FilteredSnapshot (CLM_NBR);

SELECT f.TXN_FACT_KEY, f.ACCT_PRD, f.LOSS_TXN_TYPE_DESC, f.CLM_TXN_AMT, f.RCRD_INSRT_DT, f.CLM_RCRD_TYPE_CD, f.CLM_NBR, f.CLM_FTR_NBR, f.COST_CATGRY_DESC
INTO #RawTxn
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_TXN_SS_F f
JOIN #TxnScope s ON f.CLM_NBR = s.CLM_NBR
OPTION (RECOMPILE);
CREATE INDEX IX_Claim ON #RawTxn (CLM_NBR);

SELECT f.TXN_FACT_KEY, f.ACCT_PRD, f.LOSS_TXN_TYPE_DESC, f.CLM_TXN_AMT, f.RCRD_INSRT_DT, f.CLM_RCRD_TYPE_CD, f.CLM_NBR, f.CLM_FTR_NBR, f.COST_CATGRY_DESC
INTO #FilteredTxn
FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED f
JOIN #TxnScope s ON f.CLM_NBR = s.CLM_NBR
OPTION (RECOMPILE);
CREATE INDEX IX_Claim ON #FilteredTxn (CLM_NBR);

-- Grid 3: matching physical financial rows per account/source, excluding the
-- extra reference-sample claims unless they also belong to the chosen account.
;WITH Counts AS (
    SELECT a.CustomerNum, a.CustomerName, 'LakeHouse snapshot' AS SourceName,
           COUNT_BIG(*) AS FinancialRows, COUNT(DISTINCT f.CLM_NBR) AS MatchedClaims,
           SUM(CAST(CASE WHEN ISNULL(f.FTR_CHNG_IN_OSLS_AMT, 0) <> 0
                          OR ISNULL(f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0) <> 0
                        THEN 1 ELSE 0 END AS bigint)) AS NonzeroAmountRows
    FROM #AccountClaims a JOIN #RawSnapshot f
      ON f.CLM_NBR COLLATE DATABASE_DEFAULT = a.ClaimNumber
    GROUP BY a.CustomerNum, a.CustomerName
    UNION ALL
    SELECT a.CustomerNum, a.CustomerName, 'Filtered snapshot', COUNT_BIG(*),
           COUNT(DISTINCT f.CLM_NBR),
           SUM(CAST(CASE WHEN ISNULL(f.FTR_CHNG_IN_OSLS_AMT, 0) <> 0
                          OR ISNULL(f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0) <> 0
                        THEN 1 ELSE 0 END AS bigint))
    FROM #AccountClaims a JOIN #FilteredSnapshot f
      ON f.CLM_NBR COLLATE DATABASE_DEFAULT = a.ClaimNumber
    GROUP BY a.CustomerNum, a.CustomerName
    UNION ALL
    SELECT a.CustomerNum, a.CustomerName, 'LakeHouse transactions', COUNT_BIG(*),
           COUNT(DISTINCT t.CLM_NBR),
           SUM(CAST(CASE WHEN ISNULL(t.CLM_TXN_AMT, 0) <> 0 THEN 1 ELSE 0 END AS bigint))
    FROM #AccountClaims a JOIN #RawTxn t
      ON t.CLM_NBR COLLATE DATABASE_DEFAULT = a.ClaimNumber
    GROUP BY a.CustomerNum, a.CustomerName
    UNION ALL
    SELECT a.CustomerNum, a.CustomerName, 'Filtered transactions', COUNT_BIG(*),
           COUNT(DISTINCT t.CLM_NBR),
           SUM(CAST(CASE WHEN ISNULL(t.CLM_TXN_AMT, 0) <> 0 THEN 1 ELSE 0 END AS bigint))
    FROM #AccountClaims a JOIN #FilteredTxn t
      ON t.CLM_NBR COLLATE DATABASE_DEFAULT = a.ClaimNumber
    GROUP BY a.CustomerNum, a.CustomerName
), Accounts AS (
    SELECT DISTINCT CustomerNum, CustomerName FROM #AccountClaims
)
SELECT a.CustomerNum AS CustomerNumber, a.CustomerName, s.SourceName,
       COALESCE(c.FinancialRows, 0) AS FinancialRows,
       COALESCE(c.MatchedClaims, 0) AS MatchedClaims,
       COALESCE(c.NonzeroAmountRows, 0) AS NonzeroAmountRows,
       DATEDIFF(SECOND, @StartedAt, SYSUTCDATETIME()) AS TotalStagingSeconds
FROM Accounts a
CROSS JOIN (VALUES ('LakeHouse snapshot'), ('Filtered snapshot'),
                   ('LakeHouse transactions'), ('Filtered transactions')) s(SourceName)
LEFT JOIN Counts c ON c.CustomerNum = a.CustomerNum AND c.SourceName = s.SourceName
ORDER BY a.CustomerNum, s.SourceName;

-- Grid 4: candidate key duplicates within the staged scope.
-- Zero duplicates here is not proof of a globally valid insertion key.
;WITH Candidates AS (
    SELECT 'LakeHouse snapshot' AS SourceName, COUNT_BIG(*) AS Copies
    FROM #RawSnapshot
    GROUP BY CLM_NBR, CLM_FTR_NBR, LOSS_TXN_PRCSS_DT_DIMKEY, FTR_KEY, CLM_KEY,
             CLM_RCRD_TYPE_CD, OFFSET_IND
    UNION ALL
    SELECT 'Filtered snapshot', COUNT_BIG(*) FROM #FilteredSnapshot
    GROUP BY CLM_NBR, CLM_FTR_NBR, LOSS_TXN_PRCSS_DT_DIMKEY, FTR_KEY, CLM_KEY,
             CLM_RCRD_TYPE_CD, OFFSET_IND
    UNION ALL
    SELECT 'LakeHouse transactions', COUNT_BIG(*) FROM #RawTxn
    GROUP BY TXN_FACT_KEY, ACCT_PRD
    UNION ALL
    SELECT 'Filtered transactions', COUNT_BIG(*) FROM #FilteredTxn
    GROUP BY TXN_FACT_KEY, ACCT_PRD
)
SELECT s.SourceName, COUNT(c.Copies) AS CandidateKeyGroups,
       COALESCE(SUM(CASE WHEN c.Copies > 1 THEN 1 ELSE 0 END), 0) AS DuplicateKeyGroups,
       COALESCE(MAX(c.Copies), 0) AS MaximumCopies
FROM (VALUES ('LakeHouse snapshot'), ('Filtered snapshot'),
             ('LakeHouse transactions'), ('Filtered transactions')) s(SourceName)
LEFT JOIN Candidates c ON c.SourceName = s.SourceName
GROUP BY s.SourceName
ORDER BY s.SourceName;

-- Grid 5: existing populated snapshot rows vs source rows with the same candidate key.
-- Missing/multiple source matches or different amounts require load-rule review.
SELECT f.CLM_NBR, f.CLM_FTR_NBR, f.FTR_KEY, f.CLM_KEY,
       f.LOSS_TXN_PRCSS_DT_DIMKEY, f.CLM_RCRD_TYPE_CD, f.OFFSET_IND,
       m.SourceMatches,
       f.FTR_CHNG_IN_OSLS_AMT AS FilteredReserve,
       m.MinReserve AS RawMinReserve, m.MaxReserve AS RawMaxReserve,
       f.FTR_PDLS_INCL_SLVG_SUBRO_AMT AS FilteredPaid,
       m.MinPaid AS RawMinPaid, m.MaxPaid AS RawMaxPaid,
       f.FTR_ALAE_AMT AS FilteredExpenses,
       m.MinExpenses AS RawMinExpenses, m.MaxExpenses AS RawMaxExpenses
FROM #ReferenceSnapshot f
OUTER APPLY (
    SELECT COUNT_BIG(*) AS SourceMatches,
           MIN(r.FTR_CHNG_IN_OSLS_AMT) AS MinReserve, MAX(r.FTR_CHNG_IN_OSLS_AMT) AS MaxReserve,
           MIN(r.FTR_PDLS_INCL_SLVG_SUBRO_AMT) AS MinPaid, MAX(r.FTR_PDLS_INCL_SLVG_SUBRO_AMT) AS MaxPaid,
           MIN(r.FTR_ALAE_AMT) AS MinExpenses, MAX(r.FTR_ALAE_AMT) AS MaxExpenses
    FROM #RawSnapshot r
    WHERE r.CLM_NBR = f.CLM_NBR COLLATE DATABASE_DEFAULT
      AND r.CLM_FTR_NBR = f.CLM_FTR_NBR COLLATE DATABASE_DEFAULT
      AND r.FTR_KEY = f.FTR_KEY AND r.CLM_KEY = f.CLM_KEY
      AND r.LOSS_TXN_PRCSS_DT_DIMKEY = f.LOSS_TXN_PRCSS_DT_DIMKEY
      AND (r.CLM_RCRD_TYPE_CD = f.CLM_RCRD_TYPE_CD COLLATE DATABASE_DEFAULT
           OR (r.CLM_RCRD_TYPE_CD IS NULL AND f.CLM_RCRD_TYPE_CD IS NULL))
      AND (r.OFFSET_IND = f.OFFSET_IND COLLATE DATABASE_DEFAULT
           OR (r.OFFSET_IND IS NULL AND f.OFFSET_IND IS NULL))
) m;

-- Grid 6: existing transaction amounts compared with their source candidates.
-- Pay particular attention to monetary type/scale differences from Grid 2.
SELECT f.TXN_FACT_KEY, f.ACCT_PRD, f.CLM_NBR, f.CLM_FTR_NBR,
       f.LOSS_TXN_TYPE_DESC, f.CLM_TXN_AMT AS FilteredAmount,
       m.SourceMatches, m.MinAmount AS RawMinAmount, m.MaxAmount AS RawMaxAmount
FROM #ReferenceTxn f
OUTER APPLY (
    SELECT COUNT_BIG(*) AS SourceMatches, MIN(r.CLM_TXN_AMT) AS MinAmount, MAX(r.CLM_TXN_AMT) AS MaxAmount
    FROM #RawTxn r
    WHERE r.TXN_FACT_KEY = f.TXN_FACT_KEY AND r.ACCT_PRD = f.ACCT_PRD
      AND r.CLM_NBR = f.CLM_NBR COLLATE DATABASE_DEFAULT
      AND r.CLM_FTR_NBR = f.CLM_FTR_NBR COLLATE DATABASE_DEFAULT
) m;

-- Grid 7: existing unique indexes/keys that may inform a safe insertion key.
SELECT 'SpecialAccounts' AS DatabaseName, o.name COLLATE DATABASE_DEFAULT AS TableName, i.name COLLATE DATABASE_DEFAULT AS IndexName,
       i.is_primary_key, i.is_unique, i.has_filter, i.filter_definition COLLATE DATABASE_DEFAULT AS filter_definition,
       ic.key_ordinal, c.name COLLATE DATABASE_DEFAULT AS KeyColumn
FROM sys.indexes i
JOIN sys.objects o ON o.object_id = i.object_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE o.name IN ('DW_FINCL_LOSS_FTR_SS_F_FILTERED', 'DW_FINCL_LOSS_TXN_SS_F_FILTERED')
  AND i.is_unique = 1 AND ic.key_ordinal > 0
UNION ALL
SELECT 'CLM_LakeHouse', o.name COLLATE DATABASE_DEFAULT, i.name COLLATE DATABASE_DEFAULT, i.is_primary_key, i.is_unique,
       i.has_filter, i.filter_definition COLLATE DATABASE_DEFAULT, ic.key_ordinal, c.name COLLATE DATABASE_DEFAULT
FROM CLM_LakeHouse.sys.indexes i
JOIN CLM_LakeHouse.sys.objects o ON o.object_id = i.object_id
JOIN CLM_LakeHouse.sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN CLM_LakeHouse.sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE o.name IN ('DW_FINCL_LOSS_FTR_SS_F', 'DW_FINCL_LOSS_TXN_SS_F')
  AND i.is_unique = 1 AND ic.key_ordinal > 0
ORDER BY DatabaseName, TableName, IndexName, key_ordinal;

-- Grid 8: SQL modules referencing the filtered copies. These can be readers,
-- not necessarily loaders. No rows can mean an external pipeline/job loads them,
-- or that its definition is not visible to your login.
SELECT SCHEMA_NAME(o.schema_id) AS SchemaName, o.name AS ObjectName, o.type_desc,
       CASE WHEN m.definition LIKE '%DW_FINCL_LOSS_FTR_SS_F_FILTERED%' THEN 1 ELSE 0 END AS ReferencesSnapshot,
       CASE WHEN m.definition LIKE '%DW_FINCL_LOSS_TXN_SS_F_FILTERED%' THEN 1 ELSE 0 END AS ReferencesTransactions
FROM sys.sql_modules m
JOIN sys.objects o ON o.object_id = m.object_id
WHERE m.definition LIKE '%DW_FINCL_LOSS_FTR_SS_F_FILTERED%'
   OR m.definition LIKE '%DW_FINCL_LOSS_TXN_SS_F_FILTERED%'
ORDER BY o.type_desc, o.name;

DROP TABLE #FilteredTxn;
DROP TABLE #RawTxn;
DROP TABLE #FilteredSnapshot;
DROP TABLE #RawSnapshot;
DROP TABLE #ReferenceTxn;
DROP TABLE #ReferenceSnapshot;
DROP TABLE #TxnScope;
DROP TABLE #SnapshotScope;
DROP TABLE #AccountClaims;

