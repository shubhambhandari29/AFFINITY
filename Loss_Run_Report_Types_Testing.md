# Manual report-type testing

Prerequisite: run `Loss_Run_Scheduling_Migration.sql` on the target database.
The application now reads ReportType, TriggerSource, ScheduleId and ScheduledForDate.
Leave both schedule rows disabled: automatic scheduling is not part of this step.

## Upload the Claim Review template first

The root file `SACClaimReviewTemplate.xlsx` is the converted client file
`PROPOSED 20th of month LR Burst.xls`. Conversion was performed locally with
LibreOffice, preserving all eight sheets, the cover image, and existing pivots.
The original XLS remains unchanged. No conversion software is needed in Azure.

Upload **SACClaimReviewTemplate.xlsx** with that exact name to:

```text
/Volumes/<LOSS_RUN_DATABRICKS_CATALOG>/gold/statics/SACClaimReviewTemplate.xlsx
```

For the current preproduction catalog this is:

```text
/Volumes/claims_data_pre_prod/gold/statics/SACClaimReviewTemplate.xlsx
```

Keep `SACLossRunTemplate.xlsx` in the same folder for standard loss runs. Do not
replace it. Local and Azure use the same filenames and existing authentication;
no new environment variables or DB changes are needed for this template change.
The worker downloads the appropriate template once per job. There is no local
template fallback: a missing/inaccessible file fails the job.

Start/restart the API and, for local development, the separate worker:

```powershell
py -m services.loss_run.loss_run_worker
```

Use Swagger with the usual application authentication.

## Generate a Claim Review for selected accounts

`POST /loss_run/generate`

```json
{
  "customerNumbers": ["1514748014"],
  "reportType": "claim_review"
}
```

## Generate Claim Review for all currently eligible accounts

`POST /loss_run/generate-all`

```json
{"reportType": "claim_review"}
```

For standard reports use `"reportType": "standard"`, or omit it. Generate-all
still accepts no body or `{}`. Unknown report types return HTTP 422.

Both modes return the existing HTTP 202 job response and share the active-job
guard. Poll `GET /loss_run/jobs/{job_id}`, then download with
`GET /loss_run/jobs/{job_id}/download`. No browser download changes are required.
Job responses additionally expose reportType, triggerSource, scheduleId and
scheduledForDate (date-only display format, or null).

## Workbook checks

- Visible sheets: Cover Page, Review, Claims Details, from the client's template.
- Branding, headers, widths and row styles come
  from SACClaimReviewTemplate.xlsx; the standard workbook is not generated first.
  Per client feedback, Claimant Name-Company and Adjuster headers now match the
  other headers, without yellow highlighting. Replace the Databricks template
  with the updated root file using the same name/path. The code also applies
  this correction when reading an older template. Existing reports are unchanged.
- One row per claim; distinct claimant/adjuster values are joined with semicolons.
- Total = sum of Outstanding Loss Reserve + paid loss net recovery across the
  claim's exposure rows; expenses are excluded. No threshold flag is included.
- Record-only rows are excluded, consistent with the standard Claims Data sheet.
- Claims Details keeps the client's legacy headings and supporting exposure rows.
  New view names are mapped to the old headings (for example, Outstanding Loss
  Reserve maps to Current Loss Reserve). Incurred is calculated excluding expenses.
  Legacy fields not returned by the view stay blank, not copied from the sample:
  agent/contact/source fields, Expense, ALAE, Expense Column and Recoveries, for
  example. The unused Claim Above 25K helper is also blank. If these legacy detail
  fields are required, their source mappings must be confirmed separately.
- The existing Review pivot is retained. Python fills both its displayed totals
  and cache, so initial viewing does not require Refresh. Automatic refresh is
  disabled to keep the supplied layout on first open. A hidden Review Source
  sheet supplies one row per claim for subsequent manual pivot refreshes.
- Accounts is hidden and updated for the current customer. Obsolete hidden
  Charts, Summary By Policy Year, Record Only and XDO_METADATA sheets and legacy
  defined names are removed from generated reports to avoid retaining unrelated
  sample records. Generation does not modify the uploaded template.
- Claim Review filenames include customer number and timestamp to distinguish
  them from standard reports. Storage and ZIP downloads follow the existing flow.

Compare one multi-exposure claim's Review total with its Claims Details rows,
verify multiple claimant/adjuster names, cover branding, and Grand Total.
Open a generated file in **Microsoft Excel**, confirm there is no repair prompt,
and use Review > right-click the pivot > Refresh to verify totals remain the same.
Conversion and a generated workbook were checked locally with openpyxl and a
LibreOffice open/save round-trip; Microsoft Excel rendering/refresh still needs
this user-side check. Different spreadsheet applications may restyle a refreshed
pivot, even when the totals and grouping remain correct.

## Still pending

The source remains dbo.SAC_Loss_Run. Its current date/reserve rules and Active
restriction still apply. Extended history, inactive-account eligibility, and
staged SQL integration remain paused until the SQL batch is validated.
The scheduler, timezone/activation configuration, and automatic 1st/20th runs
are the next implementation stage; adding report types does not enable them.
