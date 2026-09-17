# Temporary local automatic test

This queues **one all-eligible-account report per worker startup**, due five minutes
after startup. It does not enable or edit the real 1st/20th schedules.

1. Stop the Azure instance and any other workers sharing this database. Use just
   one local worker. This test writes real jobs and Excel files to your configured
   database and Databricks location, not a sandbox or dry run.
2. In your existing local `.env`, add/update:

   ```dotenv
   ENVIRONMENT=local
   LOSS_RUN_SCHEDULE_TEST_MODE=true
   LOSS_RUN_SCHEDULE_TEST_REPORT_TYPE=standard
   ```

   To test the client template, use `claim_review` instead of `standard`. Both
   templates and your usual local DB/Databricks authentication must work first.
3. Start the API using your normal command. In another terminal, from the repo
   root, start the worker:

   ```powershell
   py -m services.loss_run.loss_run_worker
   ```

4. Do not click Generate. After roughly five minutes, check the jobs page or jobs
   API. Expect `jobType=all`, the chosen `reportType`, and
   `requestedBy=local-schedule-test`. The source is `manual`, with no schedule ID:
   this deliberately reuses the existing queue without changing production
   schedule records or allowing local workers to claim real scheduled jobs.
5. Follow progress, confirm the generated workbook format, and test download.
   Five minutes is the submission delay, not the report completion time.

If another job is queued/running, this test waits rather than treating that job as
its own successful submission. It checks again on the next schedule check (about
60 seconds when idle), or after processing finishes. Once submitted, it will not
submit again during that worker run, even if generation fails. Restarting the
worker with test mode enabled arms another test. Do not use multiple local workers.

To test the other report type, finish the first run, stop the worker, change the
report-type setting, and restart it. Ordinary worker lease/retry behavior remains
unchanged.

## Finish testing

Set `LOSS_RUN_SCHEDULE_TEST_MODE=false` (or remove both test settings), then restart
the worker. Finish pending test jobs and stop the local worker before restarting
Azure. No SQL activation script is needed for this test. Non-local environments
ignore this flag; do not change Azure settings for testing.

This verifies timed submission, queue processing, storage and downloads. Calendar
dates, DST conversion and scheduled-occurrence duplicate protection remain covered
by the separate scheduler tests; this test is not a substitute for enabling and
verifying the real schedules in Azure.

Temporary code is confined to the two test settings in `core/config.py` and the
local-test branch/constant in `loss_run_worker.py`. It can be removed after testing
without changing the real scheduler or database schema.
