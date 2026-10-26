-- Run once in SSMS against CLMAA_SpecialAccounts BEFORE deploying the new backend.
-- Stop old workers before deployment; they cannot process Affinity jobs.
USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF COL_LENGTH('dbo.tblLossRunJob', 'BusinessType') IS NULL
BEGIN
    ALTER TABLE dbo.tblLossRunJob ADD BusinessType varchar(10) NOT NULL
        CONSTRAINT DF_tblLossRunJob_BusinessType DEFAULT ('sac') WITH VALUES;
    ALTER TABLE dbo.tblLossRunJob ADD CONSTRAINT CK_tblLossRunJob_BusinessType
        CHECK (BusinessType IN ('sac', 'affinity'));
END;
COMMIT;
GO
-- Existing jobs and SAC schedules remain SAC through the default.
-- For Affinity ONLY, tblLossRunJobAccount.CustomerNumber holds the numeric
-- AcctAffinityProgramKey as text, and CustomerName holds ProgramName.
-- No changes to account/program business data, views or existing schedules.
SELECT BusinessType, COUNT(*) AS JobCount FROM dbo.tblLossRunJob GROUP BY BusinessType;
