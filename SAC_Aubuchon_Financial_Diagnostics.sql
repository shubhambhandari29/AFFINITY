USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;

-- Diagnostic only: no application tables are changed.
-- Run this entire file in one SSMS window, in the environment that produced
-- the uploaded W E Aubuchon reports dated October 30, 2026.
-- Uses the exact 324 claim/exposure keys from the standard report.
-- It does not rerun the expensive SAC_Loss_Run view.
-- Reading the underlying financial tables may still take time.
-- Financial row existence / nonzero amounts are coverage checks, not a new
-- calculation of report totals: these sources can contain multiple snapshots.

SELECT @@SERVERNAME AS ServerName, DB_NAME() AS DatabaseName,
       CustomerNum, CustomerName, AcctStatus, LossRunDistFreq
FROM dbo.tblAcctSpecial
WHERE CustomerNum = '1500744571';

DROP TABLE IF EXISTS #AubuchonReportKeys;
DROP TABLE IF EXISTS #AubuchonFinancialCoverage;
CREATE TABLE #AubuchonReportKeys (
    ClaimNumber varchar(50) COLLATE DATABASE_DEFAULT NOT NULL,
    ExposureNumber int NOT NULL,
    PRIMARY KEY (ClaimNumber, ExposureNumber)
);

INSERT INTO #AubuchonReportKeys (ClaimNumber, ExposureNumber)
VALUES
    ('19-00814860', 1),
    ('19-00814860', 2),
    ('19-00814926', 1),
    ('19-00816697', 1),
    ('19-00816697', 2),
    ('19-00816697', 3),
    ('19-00821740', 1),
    ('19-00829853', 1),
    ('19-00848766', 1),
    ('19-00860216', 1),
    ('19-00863357', 1),
    ('19-00865072', 1),
    ('19-00867510', 1),
    ('19-00867510', 2),
    ('19-00867510', 3),
    ('19-00870007', 1),
    ('19-00873181', 1),
    ('19-00873181', 2),
    ('19-00873209', 1),
    ('19-00895016', 1),
    ('19-00895016', 2),
    ('19-00895032', 1),
    ('19-00901068', 1),
    ('19-00910694', 1),
    ('19-00912732', 1),
    ('19-00912732', 2),
    ('19-00912978', 1),
    ('19-00913168', 1),
    ('19-00913168', 2),
    ('19-00924529', 1),
    ('78-00186582', 1),
    ('78-00199005', 1),
    ('78-00202276', 1),
    ('78-00209661', 1),
    ('78-00213863', 1),
    ('78-00220638', 1),
    ('78-00230324', 1),
    ('78-00242566', 1),
    ('78-00251462', 1),
    ('83-00028520', 1),
    ('83-00028520', 2),
    ('83-00073688', 1),
    ('83-00073688', 2),
    ('83-00077350', 1),
    ('83-00077350', 2),
    ('83-00077561', 1),
    ('83-00077561', 2),
    ('83-00078597', 1),
    ('83-00078597', 2),
    ('83-00078734', 1),
    ('83-00078734', 2),
    ('83-00079079', 1),
    ('83-00079079', 2),
    ('83-00079907', 1),
    ('83-00079907', 2),
    ('83-00080578', 1),
    ('83-00080578', 2),
    ('83-00080872', 1),
    ('83-00080872', 2),
    ('83-00081046', 1),
    ('83-00081046', 2),
    ('83-00081603', 1),
    ('83-00081603', 2),
    ('83-00082073', 1),
    ('83-00082073', 2),
    ('83-00082364', 1),
    ('83-00082364', 2),
    ('83-00082948', 1),
    ('83-00082948', 2),
    ('83-00083132', 1),
    ('83-00083132', 2),
    ('83-00083336', 1),
    ('83-00083336', 2),
    ('83-00083663', 1),
    ('83-00083663', 2),
    ('83-00083918', 1),
    ('83-00083918', 2),
    ('83-00084008', 1),
    ('83-00084008', 2),
    ('83-00084207', 1),
    ('83-00084207', 2),
    ('83-00084237', 1),
    ('83-00084237', 2),
    ('83-00084712', 1),
    ('83-00084712', 2),
    ('83-00085243', 1),
    ('83-00085243', 2),
    ('83-00085246', 1),
    ('83-00085246', 2),
    ('83-00085292', 1),
    ('83-00085292', 2),
    ('83-00085476', 1),
    ('83-00085476', 2),
    ('83-00085698', 1),
    ('83-00085698', 2),
    ('83-00085871', 1),
    ('83-00085871', 2),
    ('83-00086260', 1),
    ('83-00086260', 2),
    ('83-00086437', 1),
    ('83-00086437', 2),
    ('83-00086794', 1),
    ('83-00086794', 2),
    ('83-00087128', 1),
    ('83-00087128', 2),
    ('83-00087151', 1),
    ('83-00087151', 2),
    ('83-00087739', 1),
    ('83-00087739', 2),
    ('83-00087742', 1),
    ('83-00087742', 2),
    ('83-00087803', 1),
    ('83-00087803', 2),
    ('83-00087871', 1),
    ('83-00087871', 2),
    ('83-00088312', 1),
    ('83-00088312', 2),
    ('83-00088426', 1),
    ('83-00088426', 2),
    ('83-00089101', 1),
    ('83-00089101', 2),
    ('83-00089355', 1),
    ('83-00089355', 2),
    ('83-00089980', 1),
    ('83-00089980', 2),
    ('83-00090141', 1),
    ('83-00090141', 2),
    ('83-00091427', 1),
    ('83-00091427', 2),
    ('83-00091792', 1),
    ('83-00091792', 2),
    ('83-00091800', 1),
    ('83-00091800', 2),
    ('83-00092230', 1),
    ('83-00092230', 2),
    ('83-00092480', 1),
    ('83-00092480', 2),
    ('83-00092482', 1),
    ('83-00092482', 2),
    ('83-00093079', 1),
    ('83-00093079', 2),
    ('83-00093143', 1),
    ('83-00093143', 2),
    ('83-00094387', 1),
    ('83-00094387', 2),
    ('83-00095306', 1),
    ('83-00095306', 2),
    ('83-00095355', 1),
    ('83-00095355', 2),
    ('83-00095449', 1),
    ('83-00095449', 2),
    ('83-00095788', 1),
    ('83-00095788', 2),
    ('83-00096362', 1),
    ('83-00096362', 2),
    ('83-00096512', 1),
    ('83-00096512', 2),
    ('83-00097067', 1),
    ('83-00097067', 2),
    ('83-00097345', 1),
    ('83-00097345', 2),
    ('83-00097670', 1),
    ('83-00097670', 2),
    ('83-00098425', 1),
    ('83-00098425', 2),
    ('83-00099018', 1),
    ('83-00099018', 2),
    ('83-00099568', 1),
    ('83-00099568', 2),
    ('83-00099790', 1),
    ('83-00099790', 2),
    ('83-00100545', 1),
    ('83-00100545', 2),
    ('83-00100944', 1),
    ('83-00100944', 2),
    ('83-00101214', 1),
    ('83-00101214', 2),
    ('83-00101277', 1),
    ('83-00101277', 2),
    ('83-00101737', 1),
    ('83-00101737', 2),
    ('83-00102123', 1),
    ('83-00102123', 2),
    ('83-00102131', 1),
    ('83-00102131', 2),
    ('83-00102248', 1),
    ('83-00102248', 2),
    ('83-00102312', 1),
    ('83-00102312', 2),
    ('83-00102426', 1),
    ('83-00102426', 2),
    ('83-00102517', 1),
    ('83-00102517', 2),
    ('83-00102578', 1),
    ('83-00102578', 2),
    ('83-00102814', 1),
    ('83-00102814', 2),
    ('83-00103588', 1),
    ('83-00103588', 2),
    ('83-00104062', 1),
    ('83-00104062', 2),
    ('83-00104064', 1),
    ('83-00104064', 2),
    ('83-00104115', 1),
    ('83-00104115', 2),
    ('83-00104834', 1),
    ('83-00104834', 2),
    ('83-00105667', 1),
    ('83-00105667', 2),
    ('83-00105802', 1),
    ('83-00105802', 2),
    ('83-00105971', 1),
    ('83-00105971', 2),
    ('83-00106205', 1),
    ('83-00106205', 2),
    ('83-00106388', 1),
    ('83-00106388', 2),
    ('83-00106872', 1),
    ('83-00106872', 2),
    ('83-00106905', 1),
    ('83-00106905', 2),
    ('83-00107786', 1),
    ('83-00107786', 2),
    ('83-00107903', 1),
    ('83-00107903', 2),
    ('83-00107906', 1),
    ('83-00107906', 2),
    ('83-00108207', 1),
    ('83-00108207', 2),
    ('83-00109051', 1),
    ('83-00109051', 2),
    ('83-00109961', 1),
    ('83-00109961', 2),
    ('83-00110238', 1),
    ('83-00110238', 2),
    ('83-00110424', 1),
    ('83-00110424', 2),
    ('83-00110426', 1),
    ('83-00110426', 2),
    ('83-00110428', 1),
    ('83-00110428', 2),
    ('83-00111009', 1),
    ('83-00111009', 2),
    ('83-00111053', 1),
    ('83-00111053', 2),
    ('83-00111304', 1),
    ('83-00111304', 2),
    ('83-00111953', 1),
    ('83-00111953', 2),
    ('83-00112292', 1),
    ('83-00112292', 2),
    ('83-00112578', 1),
    ('83-00112578', 2),
    ('83-00112727', 1),
    ('83-00112727', 2),
    ('83-00112858', 1),
    ('83-00112858', 2),
    ('83-00112874', 1),
    ('83-00112874', 2),
    ('83-00113038', 1),
    ('83-00113038', 2),
    ('83-00113744', 1),
    ('83-00113744', 2),
    ('83-00113902', 1),
    ('83-00113902', 2),
    ('85-00028221', 1),
    ('85-00028221', 2),
    ('85-00030010', 1),
    ('85-00043206', 1),
    ('85-00045021', 1),
    ('85-00061855', 1),
    ('85-00073349', 1),
    ('85-00073349', 2),
    ('85-00090234', 1),
    ('85-00095003', 1),
    ('85-00102846', 1),
    ('85-00104751', 1),
    ('85-00104751', 2),
    ('85-00105829', 1),
    ('85-00110010', 1),
    ('85-00120508', 1),
    ('85-00120508', 2),
    ('85-00132737', 1),
    ('85-00132834', 1),
    ('85-00132834', 2),
    ('85-00133185', 1),
    ('85-00133185', 2),
    ('85-00134328', 1),
    ('85-00134328', 2),
    ('85-00141702', 1),
    ('85-00144957', 1),
    ('85-00150846', 1),
    ('85-00159605', 1),
    ('85-00159605', 2),
    ('85-00170471', 1),
    ('85-00170613', 1),
    ('85-00175116', 1),
    ('85-00182372', 1),
    ('85-00187619', 1),
    ('85-00187619', 2),
    ('85-00196137', 1),
    ('85-00196137', 2),
    ('85-00204506', 1),
    ('85-00204506', 2),
    ('85-00206679', 1),
    ('85-00225104', 1),
    ('85-00225104', 2),
    ('85-00228073', 1),
    ('85-00233376', 1),
    ('85-00243172', 1),
    ('85-00271547', 1),
    ('85-00276411', 1),
    ('85-00276451', 1),
    ('85-00276451', 2),
    ('85-00278912', 1),
    ('85-00280619', 1),
    ('85-00284161', 1),
    ('85-00289450', 1),
    ('85-00357179', 1),
    ('85-00357179', 2),
    ('85-00357179', 3),
    ('85-00375147', 1),
    ('85-00600217', 1),
    ('85-00639478', 1);

SELECT COUNT(*) AS ReportExposureCount, COUNT(DISTINCT ClaimNumber) AS ReportClaimCount
FROM #AubuchonReportKeys;

CREATE TABLE #AubuchonFinancialCoverage (
    SourceName varchar(50) NOT NULL,
    ClaimNumber varchar(50) COLLATE DATABASE_DEFAULT NOT NULL,
    ExposureNumber int NOT NULL,
    MatchingRows bigint NOT NULL,
    HasNonzeroLossAmount int NOT NULL,
    HasNonzeroExpenseAmount int NOT NULL
);

-- Normalizing the exposure number handles Excel's leading-zero formatting.
-- A matching claim but no matching exposure is investigated separately below.
INSERT INTO #AubuchonFinancialCoverage
SELECT 'SpecialAccounts filtered snapshot', k.ClaimNumber, k.ExposureNumber,
       COUNT_BIG(*),
       MAX(CASE WHEN ISNULL(f.FTR_CHNG_IN_OSLS_AMT, 0) <> 0
                     OR ISNULL(f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0) <> 0
                     OR ISNULL(f.FTR_INCUR_INCL_SLVG_SUBRO_AMT, 0) <> 0
                THEN 1 ELSE 0 END),
       MAX(CASE WHEN ISNULL(f.FTR_ALAE_AMT, 0) <> 0
                     OR ISNULL(f.expnse_recovry_amt, 0) <> 0
                THEN 1 ELSE 0 END)
FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED f
JOIN #AubuchonReportKeys k
  ON f.CLM_NBR COLLATE DATABASE_DEFAULT = k.ClaimNumber
 AND TRY_CONVERT(int, f.CLM_FTR_NBR) = k.ExposureNumber
GROUP BY k.ClaimNumber, k.ExposureNumber;

INSERT INTO #AubuchonFinancialCoverage
SELECT 'LakeHouse direct snapshot', k.ClaimNumber, k.ExposureNumber,
       COUNT_BIG(*),
       MAX(CASE WHEN ISNULL(f.FTR_CHNG_IN_OSLS_AMT, 0) <> 0
                     OR ISNULL(f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0) <> 0
                     OR ISNULL(f.FTR_INCUR_INCL_SLVG_SUBRO_AMT, 0) <> 0
                THEN 1 ELSE 0 END),
       MAX(CASE WHEN ISNULL(f.FTR_ALAE_AMT, 0) <> 0
                     OR ISNULL(f.expnse_recovry_amt, 0) <> 0
                THEN 1 ELSE 0 END)
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_FTR_SS_F f
JOIN #AubuchonReportKeys k
  ON f.CLM_NBR COLLATE DATABASE_DEFAULT = k.ClaimNumber
 AND TRY_CONVERT(int, f.CLM_FTR_NBR) = k.ExposureNumber
GROUP BY k.ClaimNumber, k.ExposureNumber;

-- Grid 3: Zero rows for a source are explicitly displayed as zero counts.
SELECT s.SourceName,
       COUNT(c.ClaimNumber) AS ReportExposuresWithFinancialRows,
       COUNT(DISTINCT c.ClaimNumber) AS ReportClaimsWithFinancialRows,
       COALESCE(SUM(c.MatchingRows), 0) AS FinancialRows,
       COALESCE(SUM(c.HasNonzeroLossAmount), 0) AS ExposuresWithAnyNonzeroLossAmount,
       COALESCE(SUM(c.HasNonzeroExpenseAmount), 0) AS ExposuresWithAnyNonzeroExpenseAmount
FROM (VALUES ('SpecialAccounts filtered snapshot'), ('LakeHouse direct snapshot')) s(SourceName)
LEFT JOIN #AubuchonFinancialCoverage c ON c.SourceName = s.SourceName
GROUP BY s.SourceName
ORDER BY s.SourceName;

-- Grid 4: Compare the transaction source used for expenses.
SELECT 'SpecialAccounts filtered transactions' AS SourceName,
       COUNT_BIG(*) AS MatchingTransactionRows,
       COALESCE(SUM(CAST(CASE WHEN ISNULL(t.CLM_TXN_AMT, 0) <> 0 THEN 1 ELSE 0 END AS bigint)), 0)
           AS NonzeroTransactionRows
FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t
JOIN #AubuchonReportKeys k
  ON t.CLM_NBR COLLATE DATABASE_DEFAULT = k.ClaimNumber
 AND TRY_CONVERT(int, t.CLM_FTR_NBR) = k.ExposureNumber
UNION ALL
SELECT 'LakeHouse direct transactions',
       COUNT_BIG(*),
       COALESCE(SUM(CAST(CASE WHEN ISNULL(t.CLM_TXN_AMT, 0) <> 0 THEN 1 ELSE 0 END AS bigint)), 0)
FROM CLM_LakeHouse.dbo.DW_FINCL_LOSS_TXN_SS_F t
JOIN #AubuchonReportKeys k
  ON t.CLM_NBR COLLATE DATABASE_DEFAULT = k.ClaimNumber
 AND TRY_CONVERT(int, t.CLM_FTR_NBR) = k.ExposureNumber;

-- Grid 5: Find which report keys have financial records in only one source.
SELECT k.ClaimNumber, k.ExposureNumber,
       COALESCE(f.MatchingRows, 0) AS FilteredSnapshotRows,
       COALESCE(f.HasNonzeroLossAmount, 0) AS FilteredHasNonzeroLoss,
       COALESCE(r.MatchingRows, 0) AS DirectSnapshotRows,
       COALESCE(r.HasNonzeroLossAmount, 0) AS DirectHasNonzeroLoss
FROM #AubuchonReportKeys k
LEFT JOIN #AubuchonFinancialCoverage f
  ON f.SourceName = 'SpecialAccounts filtered snapshot'
 AND f.ClaimNumber = k.ClaimNumber AND f.ExposureNumber = k.ExposureNumber
LEFT JOIN #AubuchonFinancialCoverage r
  ON r.SourceName = 'LakeHouse direct snapshot'
 AND r.ClaimNumber = k.ClaimNumber AND r.ExposureNumber = k.ExposureNumber
ORDER BY k.ClaimNumber, k.ExposureNumber;

-- Grid 6: Check the unnormalized feature join used by the current report view.
-- A normalized match above but no exact match here points to a key-format/join issue.
SELECT TOP (30) k.ClaimNumber, k.ExposureNumber,
       fd.CLM_FTR_NBR AS StoredFeatureNumber,
       exact_match.FinancialRowsUsingViewJoin
FROM #AubuchonReportKeys k
LEFT JOIN CLM_LakeHouse.dbo.DW_CLMS_FTR_DIMNSN fd
  ON fd.CLM_NBR COLLATE DATABASE_DEFAULT = k.ClaimNumber
 AND TRY_CONVERT(int, fd.CLM_FTR_NBR) = k.ExposureNumber
 AND fd.CURR_VERSN_IND = 'Y'
OUTER APPLY (
    SELECT COUNT_BIG(*) AS FinancialRowsUsingViewJoin
    FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED f
    WHERE f.CLM_NBR = fd.CLM_NBR COLLATE DATABASE_DEFAULT
      AND f.CLM_FTR_NBR = fd.CLM_FTR_NBR COLLATE DATABASE_DEFAULT
) exact_match
ORDER BY k.ClaimNumber, k.ExposureNumber;

DROP TABLE #AubuchonFinancialCoverage;
DROP TABLE #AubuchonReportKeys;

