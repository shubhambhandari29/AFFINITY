-- READ-ONLY source validation before changing the Affinity financial view.
-- Run in PREPROD, with the worker idle. No views/business records are changed.
-- The existing Claims view is read ONCE; this can take about 30 minutes.
-- Session-local temporary tables disappear when this query window closes.
USE [CLMAA_SpecialAccounts];
GO
DECLARE @ProgramId int = 19; -- Local 409; repeat with 17 for Kipp.

-- Grid 1: ensure the intended program/agents are selected.
SELECT DISTINCT p.ProgramName, a.AgentCode, a.AgentName
FROM dbo.tblAcctAffinityProgram p
JOIN dbo.tblAffinityAgents a ON a.ProgramName = p.ProgramName
WHERE p.AcctAffinityProgramKey = @ProgramId AND a.LossRunReq = 'Yes';

DROP TABLE IF EXISTS #AffinityFinancialCheck;
SELECT v.[Claim Number], v.[Exposure], v.[Current Loss Reserve],
       v.[Paid Loss Net Salvage Subro], v.[Total Incurred]
INTO #AffinityFinancialCheck
FROM dbo.Affinity_Loss_Run_Claims v
WHERE v.[Producing Agent Code] IN (
    SELECT LTRIM(RTRIM(a.AgentCode))
    FROM dbo.tblAcctAffinityProgram p
    JOIN dbo.tblAffinityAgents a ON a.ProgramName = p.ProgramName
    WHERE p.AcctAffinityProgramKey = @ProgramId AND a.LossRunReq = 'Yes'
);

-- Grid 2: duplicate exposure rows need investigation before claim-level totals.
SELECT [Claim Number], [Exposure], COUNT_BIG(*) AS [RowCount]
FROM #AffinityFinancialCheck
GROUP BY [Claim Number], [Exposure]
HAVING COUNT_BIG(*) > 1;

-- Grid 3: inspect available SAC financial fields (including ALAE reserve).
SELECT OBJECT_NAME(c.object_id) AS TableName, c.name AS ColumnName,
       TYPE_NAME(c.user_type_id) AS DataType
FROM sys.columns c
WHERE c.object_id IN (
    OBJECT_ID('dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED'),
    OBJECT_ID('dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED'))
ORDER BY TableName, c.column_id;

-- Grid 4: financial-source coverage by report claim/exposure.
-- Missing filtered feature rows are NOT interpreted as zero-dollar claims.
SELECT r.[Claim Number], r.[Exposure],
       r.[Current Loss Reserve] AS CurrentReportReserve,
       r.[Paid Loss Net Salvage Subro] AS CurrentReportPaid,
       r.[Total Incurred] AS CurrentReportIncurred,
       f.FeatureRows, f.Reserve AS SACSourceReserve, f.Paid AS SACSourcePaid,
       f.Reserve + f.Paid AS SACSourceIncurredWithoutALAE,
       t.ExpenseRows, t.ExpenseTotal AS SACSourceExpenseFallback
FROM #AffinityFinancialCheck r
OUTER APPLY (
    SELECT COUNT_BIG(*) AS FeatureRows,
           SUM(ISNULL(f.FTR_CHNG_IN_OSLS_AMT, 0)) AS Reserve,
           SUM(ISNULL(f.FTR_PDLS_INCL_SLVG_SUBRO_AMT, 0)
               - ISNULL(f.DEDTBL_RECOVRY_AMT, 0)) AS Paid
    FROM dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED f
    WHERE f.CLM_NBR = r.[Claim Number] COLLATE DATABASE_DEFAULT
      AND CONVERT(varchar(50), f.CLM_FTR_NBR) COLLATE DATABASE_DEFAULT
          = CONVERT(varchar(50), r.[Exposure]) COLLATE DATABASE_DEFAULT
) f
OUTER APPLY (
    SELECT COUNT_BIG(*) AS ExpenseRows, SUM(t.CLM_TXN_AMT) AS ExpenseTotal
    FROM dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED t
    WHERE t.CLM_NBR = r.[Claim Number] COLLATE DATABASE_DEFAULT
      AND CONVERT(varchar(50), t.CLM_FTR_NBR) COLLATE DATABASE_DEFAULT
          = CONVERT(varchar(50), r.[Exposure]) COLLATE DATABASE_DEFAULT
      AND t.LOSS_TXN_TYPE_DESC LIKE '%Expense%'
      AND (t.COST_CATGRY_DESC <> 'State Penalties' OR t.COST_CATGRY_DESC IS NULL)
) t
ORDER BY r.[Claim Number], r.[Exposure];

-- Share all four grids (mention if grid 2 is empty) and any Messages errors.
-- Coverage alone does not prove equivalent freshness or financial semantics;
-- the sums above also need comparison before deploying a source change.
