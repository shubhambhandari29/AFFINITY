-- STEP 1: Run the whole file in PREPROD before deploying scheduling code.
-- Existing jobs become standard/manual jobs. Schedules start DISABLED.
-- No reports are generated and no business/view tables are changed.
USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;
BEGIN TRANSACTION;

IF OBJECT_ID(N'dbo.tblLossRunJob', N'U') IS NULL
    THROW 50001, 'dbo.tblLossRunJob must exist before this migration.', 1;

IF COL_LENGTH('dbo.tblLossRunJob', 'ReportType') IS NULL
    ALTER TABLE dbo.tblLossRunJob ADD ReportType varchar(20) NOT NULL
        CONSTRAINT DF_tblLossRunJob_ReportType DEFAULT ('standard') WITH VALUES;

IF COL_LENGTH('dbo.tblLossRunJob', 'TriggerSource') IS NULL
    ALTER TABLE dbo.tblLossRunJob ADD TriggerSource varchar(10) NOT NULL
        CONSTRAINT DF_tblLossRunJob_TriggerSource DEFAULT ('manual') WITH VALUES;

IF COL_LENGTH('dbo.tblLossRunJob', 'ScheduleId') IS NULL
    ALTER TABLE dbo.tblLossRunJob ADD ScheduleId varchar(40) NULL;

-- Local calendar date of the scheduled occurrence, not the actual execution date.
IF COL_LENGTH('dbo.tblLossRunJob', 'ScheduledForDate') IS NULL
    ALTER TABLE dbo.tblLossRunJob ADD ScheduledForDate date NULL;

IF OBJECT_ID(N'dbo.tblLossRunSchedule', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.tblLossRunSchedule
    (
        ScheduleId varchar(40) NOT NULL
            CONSTRAINT PK_tblLossRunSchedule PRIMARY KEY,
        ReportType varchar(20) NOT NULL,
        DayOfMonth tinyint NOT NULL,
        RunAtLocalTime time(0) NOT NULL
            CONSTRAINT DF_tblLossRunSchedule_RunAt DEFAULT ('00:00:00'),
        -- IANA name for Python zoneinfo; e.g. America/New_York IF Eastern is agreed.
        TimeZoneName varchar(100) NULL,
        -- First date eligible for scheduling. Prevents catch-up before activation.
        ActiveFromDate date NULL,
        IsEnabled bit NOT NULL
            CONSTRAINT DF_tblLossRunSchedule_Enabled DEFAULT (0),
        CONSTRAINT CK_tblLossRunSchedule_ReportType
            CHECK (ReportType IN ('standard', 'claim_review')),
        CONSTRAINT CK_tblLossRunSchedule_Day CHECK (DayOfMonth BETWEEN 1 AND 28),
        CONSTRAINT CK_tblLossRunSchedule_Activation CHECK
            (IsEnabled = 0 OR
             (ActiveFromDate IS NOT NULL AND TimeZoneName IS NOT NULL
              AND LEN(LTRIM(RTRIM(TimeZoneName))) > 0))
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.tblLossRunSchedule WHERE ScheduleId = 'monthly_standard')
    INSERT dbo.tblLossRunSchedule (ScheduleId, ReportType, DayOfMonth)
    VALUES ('monthly_standard', 'standard', 1);

IF NOT EXISTS (SELECT 1 FROM dbo.tblLossRunSchedule WHERE ScheduleId = 'monthly_claim_review')
    INSERT dbo.tblLossRunSchedule (ScheduleId, ReportType, DayOfMonth)
    VALUES ('monthly_claim_review', 'claim_review', 20);

-- Dynamic DDL compiles after the columns above have been added.
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE parent_object_id = OBJECT_ID('dbo.tblLossRunJob')
                 AND name = 'CK_tblLossRunJob_ReportType')
    EXEC(N'ALTER TABLE dbo.tblLossRunJob WITH CHECK ADD
        CONSTRAINT CK_tblLossRunJob_ReportType
        CHECK (ReportType IN (''standard'', ''claim_review''));');

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE parent_object_id = OBJECT_ID('dbo.tblLossRunJob')
                 AND name = 'CK_tblLossRunJob_TriggerSource')
    EXEC(N'ALTER TABLE dbo.tblLossRunJob WITH CHECK ADD
        CONSTRAINT CK_tblLossRunJob_TriggerSource
        CHECK (TriggerSource IN (''manual'', ''scheduled''));');

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE parent_object_id = OBJECT_ID('dbo.tblLossRunJob')
                 AND name = 'CK_tblLossRunJob_ScheduledOccurrence')
    EXEC(N'ALTER TABLE dbo.tblLossRunJob WITH CHECK ADD
        CONSTRAINT CK_tblLossRunJob_ScheduledOccurrence CHECK
        ((TriggerSource = ''manual'' AND ScheduleId IS NULL AND ScheduledForDate IS NULL)
         OR (TriggerSource = ''scheduled'' AND ScheduleId IS NOT NULL
             AND ScheduledForDate IS NOT NULL));');

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys
               WHERE parent_object_id = OBJECT_ID('dbo.tblLossRunJob')
                 AND name = 'FK_tblLossRunJob_Schedule')
    EXEC(N'ALTER TABLE dbo.tblLossRunJob WITH CHECK ADD
        CONSTRAINT FK_tblLossRunJob_Schedule FOREIGN KEY (ScheduleId)
        REFERENCES dbo.tblLossRunSchedule (ScheduleId);');

-- One job per schedule occurrence, including failed jobs. Retries reuse that job.
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID('dbo.tblLossRunJob')
                 AND name = 'UX_tblLossRunJob_ScheduledOccurrence')
    EXEC(N'CREATE UNIQUE INDEX UX_tblLossRunJob_ScheduledOccurrence
        ON dbo.tblLossRunJob (ScheduleId, ScheduledForDate)
        WHERE ScheduleId IS NOT NULL;');

COMMIT TRANSACTION;
GO

-- Verify two disabled schedule rows and the four new job columns.
SELECT ScheduleId, ReportType, DayOfMonth, RunAtLocalTime,
       TimeZoneName, ActiveFromDate, IsEnabled
FROM dbo.tblLossRunSchedule
ORDER BY DayOfMonth;

SELECT name AS ColumnName, TYPE_NAME(user_type_id) AS DataType, is_nullable
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.tblLossRunJob')
  AND name IN ('ReportType', 'TriggerSource', 'ScheduleId', 'ScheduledForDate');
