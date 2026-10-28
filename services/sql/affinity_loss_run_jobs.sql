-- Run once in SSMS against CLMAA_SpecialAccounts BEFORE deploying the new backend.
-- Stop old workers before deployment; they cannot process Affinity jobs.
USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;
BEGIN TRY
    BEGIN TRANSACTION;
    IF COL_LENGTH('dbo.tblLossRunJob', 'BusinessType') IS NULL
    BEGIN
        ALTER TABLE dbo.tblLossRunJob ADD BusinessType varchar(10) NOT NULL
            CONSTRAINT DF_tblLossRunJob_BusinessType DEFAULT ('sac') WITH VALUES;
    END;

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID('dbo.tblLossRunJob')
          AND name = 'CK_tblLossRunJob_BusinessType'
    )
    BEGIN
        -- Compile only after the new column exists. Also handles partial setup.
        EXEC sys.sp_executesql N'
            ALTER TABLE dbo.tblLossRunJob WITH CHECK
            ADD CONSTRAINT CK_tblLossRunJob_BusinessType
            CHECK (BusinessType IN (''sac'', ''affinity''));
        ';
    END;
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
-- Existing jobs and SAC schedules remain SAC through the default.
-- For Affinity ONLY, tblLossRunJobAccount.CustomerNumber holds the numeric
-- AcctAffinityProgramKey as text, and CustomerName holds ProgramName.
-- No changes to account/program business data, views or existing schedules.
SELECT BusinessType, COUNT(*) AS JobCount FROM dbo.tblLossRunJob GROUP BY BusinessType;
