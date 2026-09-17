-- Run AFTER deploying the updated API/workers and verifying both templates.
-- Activation starts September 17, 2026 (Eastern date). Update if running on a later day.
USE [CLMAA_SpecialAccounts];
GO
SET XACT_ABORT ON;
DECLARE @ActiveFromDate date = '2026-09-17';

IF @ActiveFromDate IS NULL
    THROW 50001, 'Set @ActiveFromDate before enabling schedules.', 1;

-- Prevent accidental historical catch-up on initial activation.
IF @ActiveFromDate < CONVERT(date, SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')
    THROW 50002, 'Choose today or a future Eastern date.', 1;

BEGIN TRANSACTION;
IF (SELECT COUNT(*) FROM dbo.tblLossRunSchedule
    WHERE ScheduleId IN ('monthly_standard', 'monthly_claim_review')) <> 2
BEGIN
    ROLLBACK;
    THROW 50003, 'Run Loss_Run_Scheduling_Migration.sql first.', 1;
END;

UPDATE dbo.tblLossRunSchedule
SET ReportType = CASE ScheduleId WHEN 'monthly_standard' THEN 'standard' ELSE 'claim_review' END,
    DayOfMonth = CASE ScheduleId WHEN 'monthly_standard' THEN 1 ELSE 20 END,
    RunAtLocalTime = '00:00:00',
    TimeZoneName = 'America/New_York',
    ActiveFromDate = @ActiveFromDate,
    IsEnabled = 1
WHERE ScheduleId IN ('monthly_standard', 'monthly_claim_review');
COMMIT;

SELECT * FROM dbo.tblLossRunSchedule;

-- To pause NEW automatic jobs (does not cancel already queued/running jobs):
-- UPDATE dbo.tblLossRunSchedule SET IsEnabled = 0
-- WHERE ScheduleId IN ('monthly_standard', 'monthly_claim_review');
