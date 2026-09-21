-- Run in each environment before deploying the Loss Date From release.
-- Stop/drain old API/worker instances first. Historical policy dates are NOT
-- copied: a policy effective date and a loss date have different meanings.
USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;
IF OBJECT_ID(N'dbo.tblLossRunJob', N'U') IS NULL
    THROW 50001, 'dbo.tblLossRunJob does not exist.', 1;
IF COL_LENGTH('dbo.tblLossRunJob', 'PolicyEffectiveDateFrom') IS NULL
    ALTER TABLE dbo.tblLossRunJob ADD PolicyEffectiveDateFrom date NULL;
IF COL_LENGTH('dbo.tblLossRunJob', 'LossDateFrom') IS NULL
    ALTER TABLE dbo.tblLossRunJob ADD LossDateFrom date NULL;
GO
SELECT name AS ColumnName, TYPE_NAME(user_type_id) AS DataType, is_nullable
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.tblLossRunJob')
  AND name IN ('PolicyEffectiveDateFrom', 'LossDateFrom');
