-- Run once in each environment before deploying extended-history backend code.
USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;

IF OBJECT_ID(N'dbo.tblLossRunJob', N'U') IS NULL
    THROW 50001, 'dbo.tblLossRunJob does not exist.', 1;

IF COL_LENGTH('dbo.tblLossRunJob', 'PolicyEffectiveDateFrom') IS NULL
BEGIN
    ALTER TABLE dbo.tblLossRunJob
        ADD PolicyEffectiveDateFrom date NULL;
END;
GO

SELECT name AS ColumnName, TYPE_NAME(user_type_id) AS DataType, is_nullable
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.tblLossRunJob')
  AND name = 'PolicyEffectiveDateFrom';
