# Next phase: validate account-first SQL and extended history

## What is ready for testing

Use `SAC_Loss_Run_Batch_Test.sql`, not `SAC_Loss_Run_Procedure.sql` or
`SQLQuery4.sql`. The latter files contain CREATE/ALTER statements; do not run them
for this test. This route does not require creating a stored procedure.

The batch reads the source databases and creates/drops only session-local
temporary tables in tempdb. It does not change the existing view, business data,
job tables, or Databricks files. It still consumes database resources: start with
one account in an agreed preprod testing window, not an all-accounts run.

The API still uses the existing view. No API/worker/UI behavior changes in this
step. SSMS runs the batch directly, so the Azure worker cannot claim this test.
Local/preprod job isolation must still be addressed before later API testing.

## Rules in the candidate query

- Select frequency-eligible accounts first, then their policies/claims and scoped
  financial rows, before the final joins and aggregation.
- No `AcctStatus = 'Active'` restriction in this batch.
- Retain `LossRunDistFreq <> 'Not Needed'` and `LossRunDistFreq <> ''`.
  As before, NULL frequency is excluded too; inactive does not mean every account
  in tblAcctSpecial is automatically eligible.
- NULL cutoff preserves the current strict policy-effective-date comparison:
  closed claims with policy effective date **after** January 1 six years ago.
- A supplied cutoff includes closed claims whose policy effective date is
  **on or after** that date. There is no hard-coded 2004 lower bound in the batch;
  actual SQL date limits and available historical source data still apply.
- Open claims bypass the date cutoff. The existing positive-outstanding-reserve
  exception is also retained; do not silently remove it while optimizing SQL.
- Existing current-version and REKEY exclusions, output columns and financial
  calculations are retained. Any upstream filtered views may impose additional
  restrictions; unexpected missing accounts/history need further investigation.

Custom cutoff support will be for standard reports. Claim Review will keep its
default history rules when the validated SQL is integrated into the backend.

## First SSMS run

1. Connect to the same preprod SQL server in SSMS.
2. Use **File > Open > File** and select `SAC_Loss_Run_Batch_Test.sql`.
3. Confirm the server/database; the file contains `USE [CLMAA_SpecialAccounts]`.
4. Keep the starting inputs:

   ```sql
   DECLARE @CustomerNumbersJson nvarchar(max) = N'["1521764304"]';
   DECLARE @PolicyEffectiveDateFrom date = NULL;
   DECLARE @CompareCurrentView bit = 0;
   ```

   This tests Amarok, the account used in the recent report investigation.
   Customer numbers must remain quoted strings, including leading zeros.
5. Click in the editor without selecting a fragment and press **F5** to run the
   entire file. Do not enable SQLCMD mode or request an execution plan.
6. Save the four Results grids separately with headers (right-click each grid,
   **Save Results As**), and copy all text from the **Messages** tab.
   If CSV headers are absent, enable them under Tools > Options > Query Results >
   SQL Server > Results to Grid, then use a new query window and rerun as needed.

## What the results mean

1. **Timing/input summary:** staged SQL elapsed seconds, requested cutoff and
   default cutoff. This excludes sending report rows and the optional old-view
   comparison, so it is not the full SSMS wall-clock time.
2. **Scope counts:** eligible customers, policies, claims, distinct claims,
   financial rows and report rows. For this first run, expect one eligible
   customer; zero means we must investigate account eligibility before performance.
3. **Account summary:** row/claim counts, reserve, net paid loss and incurred
   excluding expenses. Eligible accounts without records still appear, with zero
   report rows and NULL financial totals.
4. **Report rows:** the actual output in the existing column layout.

Messages contain SQL Server IO/CPU/elapsed diagnostics. No SHOWPLAN permission is
required for these statistics. See Microsoft's
[STATISTICS TIME documentation](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-statistics-time-transact-sql)
and [STATISTICS IO documentation](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-statistics-io-transact-sql).

## Compare with the existing view

For an active, frequency-eligible account and NULL cutoff, change only:

```sql
DECLARE @CompareCurrentView bit = 1;
```

Rerun the complete file. This deliberately runs the old slow view too, so allow
extra time. Two additional grids show its elapsed time/row counts and full-row
differences. An empty differences grid means no differences were found in column
values or duplicate counts under SQL comparison rules. A duplicate ordinal is
included because EXCEPT alone removes duplicates.

Expected: matching row counts and no differences for the default active-account
case. Investigate any differences before rollout. Source updates between the two
queries can cause differences; this is not a frozen snapshot. Cache state, load
and tempdb overhead also influence timings, so one faster run is not proof of a
general performance improvement.

Do not use this comparison for extended dates (the script rejects that setting).
The old view is also not an equality baseline for inactive accounts: those are
intentionally new behavior. See Microsoft's
[EXCEPT documentation](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/set-operators-except-and-intersect-transact-sql).

## Follow-up cases after the first run succeeds

- Two or more known active accounts; compare against the current view.
- An inactive account with eligible frequency and known claims, comparison off.
- Standard report with `'20040101'`, and an earlier date where data exists.
- A policy effective exactly on a selected cutoff: its closed claims qualify.
- An old open claim before the cutoff: it must still qualify.
- An old closed claim with positive reserve: retain the existing exception.
- An account with no claims, an ineligible-frequency account, leading-zero IDs,
  and an unknown customer number. Do not substitute another account on no match.
- All eligible accounts only after selected-account correctness and load testing.

For custom-date tests, save results with and without the cutoff and check known
claims. Do not assume that zero older rows proves that historical data is absent.

## What to send back first

Send the four Results grids and Messages from the default one-account run. If
it succeeds, send the comparison timing and differences grids from the second
run. On any SQL error, send its full message and line number; do not run the
procedure/view scripts as a workaround.

## After SQL validation

Next implementation will store the optional cutoff on the job, accept it on the
standard generation APIs, pass it through the worker into a parameterized SQL
batch on a single DB connection, update the cover text, and align the accounts
endpoint with the new eligibility rules. The SSMS diagnostics/comparison queries
must not be sent to the application as production report result sets.

Automatic 1st/20th scheduling and the EST-versus-daylight-saving timezone choice
remain a separate later phase. Do not enable schedule rows yet.
