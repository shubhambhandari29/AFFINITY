# Automatic monthly loss runs

| Eastern local date/time | Report |
| --- | --- |
| 1st, 12:00 AM | Standard loss run |
| 20th, 12:00 AM | Claim Review using the client template |

`America/New_York` follows daylight saving: midnight is 05:00 UTC in winter
and 04:00 UTC in summer. It is not a fixed UTC-5 clock.

## Enable in preproduction

1. The existing `Loss_Run_Scheduling_Migration.sql` must already have run.
2. Verify both templates in the configured Databricks `gold/statics` volume:
   `SACLossRunTemplate.xlsx` and `SACClaimReviewTemplate.xlsx`.
3. Deploy updated code and requirements to **every** API/worker sharing this DB.
   Stop old workers; old code can still take jobs and generate the wrong report.
4. Keep Azure `ENVIRONMENT=PREPROD` (PROD for production). No new environment
   variables are required. Keep the App Service running/Always On; the scheduler
   lives in the existing worker, not an external timer service.
5. Open `Loss_Run_Enable_Schedules.sql` in SSMS, set `@ActiveFromDate` explicitly,
   and execute against the intended instance. Choose today or a future Eastern date.
   If today is a scheduled day and midnight has passed, it becomes due immediately.
6. Verify both schedule rows show `IsEnabled=1`, `America/New_York`, midnight,
   and the intended activation date. Existing jobs APIs/downloads need no new endpoint.

Deploying code alone does not enable disabled schedule rows. Enable separately in
each environment's database. Do not point production and preproduction workers at
the same queue expecting isolation.

## What happens

The worker checks schedules immediately on startup, then approximately every 60
seconds while idle. Job queue pickup remains every 10 seconds. While processing a
report, polling waits; an overdue schedule check runs when processing finishes.
When due, it inserts an `all` job with the correct `ReportType`,
`TriggerSource=scheduled`, `ScheduleId`, and Eastern `ScheduledForDate`.
The usual worker generates files, saves to Databricks, and updates the existing
job counts/status. Users download from the jobs page; there is no email delivery
or automatic browser download when no user is online.

An active queued/processing job delays automatic submission until the queue is
available. After downtime, missed occurrences since `ActiveFromDate` are queued
oldest first, one at a time. These use current data, not a historical snapshot.
An occurrence already recorded in the job table is not submitted again, even if
failed. Existing lease/retry logic handles interrupted jobs; exhausted failures
require investigation and a manual run. Retain scheduled job rows to preserve
duplicate protection. Re-enabling an old activation date catches up missed months.

The same SQL transaction/range lock as manual submission plus the existing unique
scheduled-occurrence index protects against concurrent submissions. Midnight is
the due time, not a guarantee of exact start/completion time.

Local `ENVIRONMENT=local` neither creates nor claims automatic jobs, including
expired scheduled jobs. Manual jobs still share the database queue: use an isolated
test DB when testing local worker ownership. Restart API and worker after updates.

## Verify safely

For the temporary five-minute local all-account test, follow
`Loss_Run_Local_Automatic_Test.md`. It does not enable the real monthly schedules.

Run scheduler unit tests locally; do not change your local environment to PREPROD
against the shared DB just to test the clock. In an isolated test DB, enable a
schedule due earlier today and verify one scheduled job appears, uses the right
template, and is not repeated after worker restart. Repeat with an active manual
job to confirm deferral. Real SQL concurrency and Azure overnight execution must
be verified in the target environment.

Existing account eligibility is unchanged: Active accounts with nonblank loss-run
frequency other than `Not Needed`. Inactive-account support and custom historical
cutoff changes remain separate work, as does SQL performance optimization.

Timezone implementation: [Python zoneinfo](https://docs.python.org/3.12/library/zoneinfo.html).
Windows installations use the explicit `tzdata` dependency.
