USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;

-- Stop the API worker(s) first. Run this migration before deploying the new code.
-- Adds one flag and extends status checks; existing job rows are preserved.
BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID('dbo.tblLossRunJob', 'U') IS NULL
       OR OBJECT_ID('dbo.tblLossRunJobAccount', 'U') IS NULL
        THROW 50001, 'Loss-run job tables must exist before this migration.', 1;

    IF COL_LENGTH('dbo.tblLossRunJob', 'CancelRequested') IS NULL
        ALTER TABLE dbo.tblLossRunJob ADD CancelRequested bit NOT NULL
            CONSTRAINT DF_tblLossRunJob_CancelRequested DEFAULT (0) WITH VALUES;

    -- Existing installations can have different names for their status checks.
    -- Refuse to replace checks that also constrain another column.
    IF EXISTS (
        SELECT 1
        FROM sys.check_constraints c
        JOIN sys.sql_expression_dependencies d ON d.referencing_id = c.object_id
        JOIN sys.columns col ON col.object_id = d.referenced_id
                            AND col.column_id = d.referenced_minor_id
        WHERE c.parent_object_id IN (OBJECT_ID('dbo.tblLossRunJob'), OBJECT_ID('dbo.tblLossRunJobAccount'))
          AND LOWER(c.definition) LIKE '%status%'
          AND col.name <> 'Status'
    )
        THROW 50002, 'A status check also constrains another column. Ask the DB team to extend it to allow cancelled.', 1;

    DECLARE @DropChecks nvarchar(max) = N'';
    SELECT @DropChecks = @DropChecks + N'ALTER TABLE '
        + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.'
        + QUOTENAME(OBJECT_NAME(parent_object_id)) + N' DROP CONSTRAINT '
        + QUOTENAME(name) + N';'
    FROM sys.check_constraints
    WHERE parent_object_id IN (OBJECT_ID('dbo.tblLossRunJob'), OBJECT_ID('dbo.tblLossRunJobAccount'))
      AND LOWER(definition) LIKE '%status%';
    IF @DropChecks <> N'' EXEC sys.sp_executesql @DropChecks;

    ALTER TABLE dbo.tblLossRunJob WITH CHECK ADD CONSTRAINT CK_tblLossRunJob_Status
        CHECK (Status IN ('queued', 'processing', 'completed', 'partially_completed', 'failed', 'cancelled'));
    ALTER TABLE dbo.tblLossRunJobAccount WITH CHECK ADD CONSTRAINT CK_tblLossRunJobAccount_Status
        CHECK (Status IN ('queued', 'processing', 'completed', 'failed', 'cancelled'));

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO

SELECT name, definition FROM sys.check_constraints
WHERE parent_object_id IN (OBJECT_ID('dbo.tblLossRunJob'), OBJECT_ID('dbo.tblLossRunJobAccount'));
SELECT name, TYPE_NAME(user_type_id) AS DataType, is_nullable
FROM sys.columns WHERE object_id = OBJECT_ID('dbo.tblLossRunJob') AND name = 'CancelRequested';
