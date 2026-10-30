USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;

-- Read-only. Run in the same environment where you will generate reports.
-- Uses the existing SAC report view, including its current history/claim rules
-- and FILTERED financial sources. It can take several minutes to execute.
-- No calendar date should be selected when generating these test reports.

SELECT TOP (10)
    @@SERVERNAME AS ServerName,
    a.CustomerNum AS CustomerNumber,
    a.CustomerName,
    a.AcctStatus,
    a.LossRunDistFreq,
    a.CRThresh,
    COUNT_BIG(*) AS ClaimExposureRows,
    COUNT(DISTINCT v.[Claim Number]) AS DistinctClaims,
    SUM(CAST(CASE
        WHEN ISNULL(v.[Outstanding Loss Reserve], 0) <> 0
          OR ISNULL(v.[Total Paid Loss Net Salvage/Subro/Loss Recovery], 0) <> 0
        THEN 1 ELSE 0 END AS bigint)) AS RowsWithNonzeroLossFinancials,
    SUM(CONVERT(decimal(28, 10), ISNULL(v.[Outstanding Loss Reserve], 0)))
        AS TotalOutstandingLossReserve,
    SUM(CONVERT(decimal(28, 10), ISNULL(v.[Total Paid Loss Net Salvage/Subro/Loss Recovery], 0)))
        AS TotalPaidLoss,
    SUM(CONVERT(decimal(28, 10), ISNULL(v.[ALAE Paid], 0))) AS TotalPaidExpenses
FROM dbo.tblAcctSpecial a
JOIN dbo.SAC_Loss_Run v
    ON v.[Customer Number] = a.CustomerNum
WHERE a.AcctStatus = 'Inactive'
  AND a.LossRunDistFreq <> 'Not Needed'
  AND a.LossRunDistFreq <> ''
  AND ISNULL(v.[Record Only Indicator], 'N') <> 'Y'
GROUP BY a.CustomerNum, a.CustomerName, a.AcctStatus, a.LossRunDistFreq, a.CRThresh
HAVING MAX(CASE
    WHEN ISNULL(v.[Outstanding Loss Reserve], 0) <> 0
      OR ISNULL(v.[Total Paid Loss Net Salvage/Subro/Loss Recovery], 0) <> 0
    THEN 1 ELSE 0 END) = 1
ORDER BY RowsWithNonzeroLossFinancials DESC, DistinctClaims DESC, a.CustomerNum;
