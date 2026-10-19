USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;

-- Run this entire file in one SSMS query window, on the same environment
-- where you generated the reports. It changes no application tables.
-- The report view is evaluated once and can take as long as report generation.
-- Uses the default history rule: generate the test reports without a calendar date.

DROP TABLE IF EXISTS #SACThresholdAccounts;
DROP TABLE IF EXISTS #SACThresholdRows;
DROP TABLE IF EXISTS #SACThresholdExposures;
DROP TABLE IF EXISTS #SACThresholdClaims;
DROP TABLE IF EXISTS #SACThresholdSummary;

SELECT
    LTRIM(RTRIM(CONVERT(varchar(50), a.CustomerNum))) AS CustomerNumber,
    a.CustomerName,
    CONVERT(varchar(100), a.CRThresh) AS StoredThreshold,
    TRY_CONVERT(decimal(28, 10), NULLIF(
        REPLACE(REPLACE(LTRIM(RTRIM(CONVERT(varchar(100), a.CRThresh))), '$', ''), ',', ''),
        ''
    )) AS ThresholdAmount
INTO #SACThresholdAccounts
FROM dbo.tblAcctSpecial a
WHERE a.LossRunDistFreq <> 'Not Needed'
  AND a.LossRunDistFreq <> '';

SELECT
    a.CustomerNumber,
    LTRIM(RTRIM(CONVERT(varchar(50), v.[Claim Number]))) AS ClaimNumber,
    CONVERT(varchar(50), v.Exposure) AS Exposure,
    v.[Record Only Indicator] AS RecordOnlyIndicator,
    COALESCE(CONVERT(decimal(28, 10), v.[Outstanding Loss Reserve]), 0)
      + COALESCE(CONVERT(decimal(28, 10), v.[Total Paid Loss Net Salvage/Subro/Loss Recovery]), 0)
      AS ExposureLoss
INTO #SACThresholdRows
FROM dbo.SAC_Loss_Run v
JOIN #SACThresholdAccounts a
    ON a.CustomerNumber = LTRIM(RTRIM(CONVERT(varchar(50), v.[Customer Number])));

-- Aggregate exposures first, then claims. Record Only rows do not participate
-- in the claim-review threshold. Expenses are deliberately not included.
SELECT CustomerNumber, ClaimNumber, Exposure, SUM(ExposureLoss) AS ExposureTotal
INTO #SACThresholdExposures
FROM #SACThresholdRows
WHERE ISNULL(RecordOnlyIndicator, 'N') <> 'Y'
GROUP BY CustomerNumber, ClaimNumber, Exposure;

SELECT
    CustomerNumber,
    ClaimNumber,
    COUNT(*) AS ExposureCount,
    SUM(ExposureTotal) AS ClaimTotal,
    MAX(ExposureTotal) AS LargestExposureTotal
INTO #SACThresholdClaims
FROM #SACThresholdExposures
GROUP BY CustomerNumber, ClaimNumber;

SELECT
    a.CustomerNumber,
    a.CustomerName,
    a.ThresholdAmount,
    COUNT(*) AS ClaimCount,
    SUM(CASE WHEN c.ClaimTotal >= a.ThresholdAmount THEN 1 ELSE 0 END) AS ClaimsMeetingThreshold,
    SUM(CASE WHEN c.ClaimTotal < a.ThresholdAmount THEN 1 ELSE 0 END) AS ClaimsBelowThreshold,
    SUM(CASE WHEN c.ExposureCount > 1 THEN 1 ELSE 0 END) AS MultiExposureClaims
INTO #SACThresholdSummary
FROM #SACThresholdAccounts a
JOIN #SACThresholdClaims c ON c.CustomerNumber = a.CustomerNumber
GROUP BY a.CustomerNumber, a.CustomerName, a.ThresholdAmount;

-- Grid 1: Pick one account here with a non-default threshold and claims on both sides.
SELECT TOP (5) '1. Non-default threshold' AS TestCase, *
FROM #SACThresholdSummary
WHERE ThresholdAmount > 0 AND ThresholdAmount <> 50000
  AND ClaimsMeetingThreshold > 0 AND ClaimsBelowThreshold > 0
ORDER BY MultiExposureClaims DESC, ClaimCount DESC, CustomerNumber;

-- Grid 2: Pick one default-$50,000 account, preferably with multiple exposures.
SELECT TOP (5) '2. Default $50,000 threshold' AS TestCase, *
FROM #SACThresholdSummary
WHERE ThresholdAmount = 50000
  AND ClaimsMeetingThreshold > 0 AND ClaimsBelowThreshold > 0
ORDER BY MultiExposureClaims DESC, ClaimCount DESC, CustomerNumber;

-- Grid 3: No matching report rows, including no Record Only rows.
-- This does not establish that the account has never had claims.
SELECT TOP (5)
    '3. No matching report records' AS TestCase,
    a.CustomerNumber, a.CustomerName, a.ThresholdAmount
FROM #SACThresholdAccounts a
WHERE NOT EXISTS (
    SELECT 1 FROM #SACThresholdRows r WHERE r.CustomerNumber = a.CustomerNumber
)
ORDER BY a.CustomerNumber;

-- Grid 4: Optional equality test. These claims must qualify with >=.
SELECT TOP (10)
    '4. Claim exactly equals threshold' AS TestCase,
    a.CustomerNumber, a.CustomerName, a.ThresholdAmount,
    c.ClaimNumber, c.ExposureCount, c.ClaimTotal
FROM #SACThresholdAccounts a
JOIN #SACThresholdClaims c ON c.CustomerNumber = a.CustomerNumber
WHERE a.ThresholdAmount > 0 AND c.ClaimTotal = a.ThresholdAmount
ORDER BY a.CustomerNumber, c.ClaimNumber;

-- Grid 5: Optional missing-setting test. Do not substitute $50,000 for these.
SELECT TOP (10)
    '5. Threshold not configured' AS TestCase,
    CustomerNumber, CustomerName, StoredThreshold
FROM #SACThresholdAccounts
WHERE NULLIF(LTRIM(RTRIM(StoredThreshold)), '') IS NULL
ORDER BY CustomerNumber;

-- Grid 6: Particularly useful proof of claim-level aggregation:
-- no individual exposure meets the threshold, but their combined total does.
SELECT TOP (10)
    '6. Combined exposures meet threshold' AS TestCase,
    a.CustomerNumber, a.CustomerName, a.ThresholdAmount,
    c.ClaimNumber, c.ExposureCount, c.LargestExposureTotal, c.ClaimTotal
FROM #SACThresholdAccounts a
JOIN #SACThresholdClaims c ON c.CustomerNumber = a.CustomerNumber
WHERE a.ThresholdAmount > 0
  AND c.ExposureCount > 1
  AND c.ClaimTotal >= a.ThresholdAmount
  AND c.LargestExposureTotal < a.ThresholdAmount
ORDER BY a.CustomerNumber, c.ClaimNumber;

DROP TABLE #SACThresholdSummary;
DROP TABLE #SACThresholdClaims;
DROP TABLE #SACThresholdExposures;
DROP TABLE #SACThresholdRows;
DROP TABLE #SACThresholdAccounts;
