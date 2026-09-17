# Loss Run — Production Rollout Checklist

Prepared 17 September 2026. Based on this project’s discussion and repository at commit 2a35fd7. This is a deployment handover, not confirmation that production has been configured. Replace every PROD placeholder; preproduction values are reference only. Owner and completion evidence must be recorded for each step.

## 1. Release scope and lessons from preproduction

- Implemented: selected-account array and all-account generation; standard and client-template Claim Review reports; Databricks template reads, report writes and downloads; SQL-backed asynchronous jobs; counts, failures and job history; automatic 1st/20th schedules.
- The original synchronous generate-all call exceeded the Azure gateway timeout even though files eventually completed. Generation now returns a queued job; the worker performs the long query and Excel work independently of the request. Do not restore the original synchronous endpoint behavior.
- The measured bottleneck was SQL: one selected account took 887.06 seconds in the view query versus 0.99 seconds for Excel creation. SQL optimization is deliberately deferred. Queueing solves the HTTP wait, not the query’s duration.
- Local personal authentication working did not prove App Service identity access. Azure Databricks previously returned 403 until the platform team corrected its identity/access configuration. Revalidate PROD separately.
- A local job was previously taken by an older Azure worker and produced the wrong report type. Deploy a consistent version to every worker; do not share production queues with local/preproduction workers.
- The temporary local five-minute trigger was successfully tested and has now been removed, including its configuration flags. Do not deploy or restore it.
- Runtime downloads templates from Databricks for each job. A workbook once inspected in development is not enough: both actual template files must exist in PROD.
- Only operational job/account/schedule state is written to SQL. The API does not reproduce legacy business-table distribution writes or automatic email delivery. Reports are stored in Databricks and downloaded by users.

## 2. Items still requiring an explicit release decision

- Inactive accounts: requested by the client, but current service and supplied view still require AcctStatus = 'Active'. Do not claim inactive support is delivered. Either implement/test it separately or obtain approval to release the current scope.
- Custom history/cutoff calendar: discussed but not implemented in current request models. Payloads accept reportType and, for selected runs, customerNumbers—not a cutoff date. Do not expose a calendar that the API ignores.
- Current view is NOT a simple six-year filter on every claim. Its HAVING includes closed claims whose policy effective date is strictly after the year-start-minus-72-month cutoff, OR positive outstanding reserve, OR open claims. Confirm this is the approved production baseline.
- Account-first optimization, stored-procedure alternatives and batch SQL tests are deferred. Do not run experimental SQL as part of this release.
- Output folder still ends in specialaccounts_lossruns_temporary. Business/storage owners must approve it for production or request a reviewed code/path change before deployment.
- Standard filenames use customer name plus date with overwrite enabled. Repeated same-day runs can overwrite a file referenced by older jobs; duplicate customer names can also collide. Decide whether immutable per-job storage is required before sign-off.
- Browser download packaging still occurs in an HTTP request. Test the full production ZIP size against gateway limits and temporary disk capacity; background generation alone does not eliminate download timeouts.
- The UI “View Existing Jobs” navigation issue remains unresolved in this chat. The component targets /loss-run-jobs, but the actual frontend route configuration is not in this repo. Resolve and verify before go-live.
- Security owner must review job visibility/download authorization: routes require authentication, but current handlers do not themselves restrict access to the requester’s own jobs. Confirm the intended shared-account report access policy.

## 3. Production values and responsible teams

Release coordinator: collect the values below in the approved deployment ticket. Never copy credentials, tokens or secret values into this document.

| Item | Production value to record | Owner |
| --- | --- | --- |
| App Service / subscription / resource group / slot | PROD placeholders | Azure platform |
| System-assigned identity object/principal ID | PROD identity, not PREPROD | Azure / identity |
| Same identity application/client ID and tenant ID | Record for identity mapping, not new app secrets | Identity / Databricks |
| SQL managed instance hostname | PROD server | DBA |
| Application database | CLMAA_SpecialAccounts; verify in PROD | DBA |
| Cross-database source | CLM_LakeHouse on same instance; verify | DBA |
| Databricks workspace HTTPS host | PROD workspace host | Databricks |
| Unity Catalog catalog and volumes | PROD catalog; gold.statics and gold.external_volume | Databricks |
| Approved template versions / hashes | Standard and Claim Review | Business / backend |
| Activation date and cutover window | Approved Eastern calendar date | Business / operations |
| Release commit and rollback artifact | Approved matching backend and UI versions | Release engineering |

Preproduction reference ONLY: workspace https://adb-8608532567795739.19.azuredatabricks.net; catalog claims_data_pre_prod; previously supplied system identity principal ID bb8645bb-88c9-4a65-bb39-adf60bd93fd8. None is a confirmed production value. No production application/client ID was supplied in this chat.

## 4. Identity and network preparation

Owner: Azure platform, identity team, network team. Why: the deployed process must authenticate and reach both SQL and Databricks without a developer’s interactive login.

1. In Azure Portal, open the actual PROD App Service/slot, then Identity > System assigned. Verify Status On and record Object (principal) ID. Enabling or recreating an identity is an administrative change; coordinate it rather than replacing an existing identity accidentally.
2. Have the identity team resolve that service principal’s application/client ID and tenant. Object ID identifies the Entra service-principal object; application/client ID is a different identifier for the SAME managed identity. SQL access and Databricks registration must resolve to that same PROD identity.
3. Optional read-only lookup by an authorized operator: az ad sp show --id <PROD-OBJECT-ID> --query "{objectId:id,applicationId:appId,displayName:displayName}". Azure/Entra read permission is required. Do not invent a second app registration or a client secret for this flow.
4. From the deployed app’s SSH/console, check DNS/TLS reachability to the PROD Databricks host, for example curl -I --connect-timeout 15 https://<PROD-DATABRICKS-HOST>. A 404 on this root request can show that HTTP connectivity works; it does NOT validate identity, file permissions or a successful file API request.
5. Verify SQL network access from that app using the managed-instance network design. Confirm private DNS, VNet integration, firewall/routing and any proxy restrictions. Do not assume local VPN connectivity proves App Service connectivity.
6. Capture success evidence for authenticated template read, report write and report read using the deployed managed identity. Do not put bearer tokens in tickets/logs.

Current Databricks code uses ManagedIdentityCredential() in non-local mode and obtains a token for 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default. This is the Azure Databricks resource scope, not our app ID or a URL to replace. It does not need a client secret or a Databricks-specific tenant/client-ID setting for the current system-assigned flow. Existing AZURE_CLIENT_ID/TENANT_ID/SECRET settings serve the application’s SSO flow; do not repurpose them.

References: [Managed identities in App Service](https://learn.microsoft.com/en-us/azure/app-service/overview-managed-identity); [Databricks managed-identity authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi).

## 5. SQL objects, dependency access and script order

Owner: DBA with backend developer review. Why: PROD requires the same validated schema, view dependencies and runtime permissions as PREPROD—not merely a connection to the main DB.

### 5.1 Capture the baseline before changing PROD

The current repo does not contain the original CREATE TABLE scripts for dbo.tblLossRunJob and dbo.tblLossRunJobAccount. The scheduling migration expects tblLossRunJob to exist. This is a rollout prerequisite, not something the migration will fix.

In SSMS on validated PREPROD: right-click CLMAA_SpecialAccounts > Tasks > Generate Scripts > Select specific database objects > select both job tables. Under Advanced choose Schema only and include keys, defaults, check constraints, foreign keys and indexes. Save to the approved release package. DBA should verify the exported script includes all of them. Do NOT export/preload preproduction job or account data into PROD.

Compare this baseline with loss_run_job_repository.py: job columns include IDs/type/status/phase, counts, requester, CreatedAt/StartedAt/UpdatedAt/CompletedAt, WorkerId, LeaseUntil, LastHeartbeatAt, AttemptCount and ErrorMessage. Account rows need customer number/name, status, failure reason and OutputPath. Preserve the exact PREPROD types/defaults/keys instead of reconstructing DDL from this descriptive list.

### 5.2 Confirm business view and dependencies

SQLQuery4.sql is an ALTER VIEW reference for dbo.SAC_Loss_Run in CLMAA_SpecialAccounts, not a fresh-install or blanket production migration. Compare it with the approved live view; an ALTER script fails if the view does not exist. DBA must package the appropriate reviewed CREATE/ALTER and dependencies.

Known application-DB dependencies: dbo.tblAcctSpecial, dbo.DW_FINCL_LOSS_TXN_SS_F_FILTERED and dbo.DW_FINCL_LOSS_FTR_SS_F_FILTERED. Determine whether each filtered object is a table/view and inspect its own dependencies/data refresh. Their definitions are not fully supplied by SQLQuery4.sql.

Known CLM_LakeHouse objects referenced by the supplied view:

- DW_CLMS_CLM_CNTCT_DIMNSN; DW_CLMS_FTR_DIMNSN; DW_CLMS_LGL_FACT.
- DW_CLMS_CLM_DIMNSN; DW_POL_DIMNSN; DW_CLMS_LGL_DIMNSN.
- DM_RETNTN_PRDCNG_AGNT_HR_D; DW_CLMS_CLMNT_DIMNSN; DW_CLMS_DRVR_DIMNSN.
- DW_CLMS_VEH_DIMNSN; DW_REF_MAJ_PERIL.

Verify the actual dependency graph in PROD, including nested views/synonyms. Only these two databases are confirmed from the supplied definition; do not assume uninspected dependencies stop there. Verify required engine features (including DATETRUNC), compatibility and cross-DB collation. PREPROD LakeHouse collation supplied was SQL_Latin1_General_CP1_CI_AS; do not change production collation just for this deployment.

### 5.3 Create/map the identity and grant least privilege

Ask DBA to map the PROD managed identity into the required databases using the instance’s supported Entra login/user approach. Use the PROD principal/object ID to verify the mapping. A friendly database username is only an alias; CREATE USER [<GUID>] FROM EXTERNAL PROVIDER does not by itself establish that a GUID string will be resolved as an object ID. DBA must validate the mapping/SID or its approved group-based equivalent, including cross-database execution context. Do not add the application/client ID as an unrelated second SQL identity.

Runtime needs: SELECT on account/view data and the permitted source dependency chain; SELECT/INSERT/UPDATE on the two job tables; SELECT on tblLossRunSchedule. Schedule activation updates are performed by DBA/deployment operator, not ordinary API users. No blanket db_owner, source-table writes or server-wide cross-database ownership-chaining change is requested. Final object grants depend on the approved view security model; test as the actual application identity.

### 5.4 Execute in this order

1. Back up/capture existing production schema and permissions; record target instance/database and release approval.
2. Provision/verify the approved source objects and SAC_Loss_Run view.
3. Create the base job/account tables from the reviewed baseline if absent. Compare existing tables rather than dropping them.
4. Run Loss_Run_Scheduling_Migration.sql against the PROD CLMAA_SpecialAccounts database. Its PREPROD header is historical; verify the target connection before execution. It adds ReportType, TriggerSource, ScheduleId, ScheduledForDate; creates/seeds tblLossRunSchedule; adds constraints/FK and UX_tblLossRunJob_ScheduledOccurrence.
5. Verify migration result grids, new columns, unique filtered index and both schedule rows. On first installation they are disabled. Re-running the migration does not forcibly disable rows already enabled.
6. Grant/verify runtime permissions after objects exist. Test app identity access to both business data and job operations.
7. Do not run Loss_Run_Enable_Schedules.sql until deployment and manual smoke tests are complete. See section 9.

Do not execute SAC_Loss_Run_Batch_Test.sql, performance experiment output files, CSVs, or proposed optimization/SP changes as production migrations. Existing temporary local test required no SQL and has been removed.

Reference: [Microsoft Entra authentication with the ODBC driver](https://learn.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory?view=sql-server-ver17).

## 6. Databricks permissions, directories and templates

Owner: Databricks administrator and business template owner. Why: Azure RBAC or access through a developer’s browser alone does not grant the app access to Unity Catalog files.

Register/verify the Microsoft Entra-managed service principal using the PROD managed identity’s application/client ID, assign it to the PROD workspace, and verify it is active. Grant Unity Catalog privileges to that principal (or the approved group containing it). For the current layout: USE CATALOG on the PROD catalog; USE SCHEMA on gold; READ VOLUME on statics; READ VOLUME and WRITE VOLUME on external_volume. Runtime does not need to create volumes or own the catalog. Administrator, not the runtime app, uploads approved templates.

Databricks administrator should confirm underlying external storage credentials/access connector and external-location configuration are healthy. The app uses Databricks file APIs; it is not mounting /Volumes on App Service or accessing ADLS directly. No SQL warehouse/cluster/serverless setting is selected by this file-transfer code. /api/2.0/fs/files is the REST endpoint prefix; /Volumes/... remains the file’s logical path.

| Purpose | Required current-code location |
| --- | --- |
| Standard template | /Volumes/<PROD-CATALOG>/gold/statics/SACLossRunTemplate.xlsx |
| Claim Review template | /Volumes/<PROD-CATALOG>/gold/statics/SACClaimReviewTemplate.xlsx |
| Generated reports | /Volumes/<PROD-CATALOG>/gold/external_volume/specialaccounts_lossruns_temporary/ |

Create/verify the output subdirectory and approve the path. Only host/catalog are configurable today; schema, volume names, output subfolder and template filenames are code-defined. If PROD uses a different layout, make a reviewed code/configuration change before release. Do not assume setting the catalog fixes all path differences.

Upload the current approved Standard template from validated PREPROD (the current root does not contain that standard workbook). Upload SACClaimReviewTemplate.xlsx from the approved release. The client XLS was converted to XLSX previously; do not rename .xls to .xlsx or depend on conversion software in Azure. Keep versioned, non-customer-filled template masters under the team’s approved policy; runtime still reads the Databricks copies.

Open generated files in desktop Excel and verify: cover-page image, correct customer, dates/currency, standard sheets, Claim Review visible sheets and pivot, no stale sample-account data, approved header styling. Claim Review code validates Review row 4/pivot layout, rebuilds claim-level pivot data and excludes Record Only rows. Validate its incurred totals against accepted samples—not only the filename. Template changes must be reviewed against code assumptions.

References: [Manage Databricks service principals](https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/manage-service-principals); [Unity Catalog volume privileges](https://learn.microsoft.com/en-us/azure/databricks/volumes/privileges).

## 7. Application settings and worker deployment

Owner: backend/release engineer with Azure platform team. Why: environment mode controls authentication AND worker startup; the wrong value can stop automatic processing or point PROD at PREPROD.

| Setting | Production action |
| --- | --- |
| ENVIRONMENT | PROD; exact key spelling matters. Do not use ENVIORNMENT or local. |
| DATABRICKS_HOST | PROD workspace HTTPS URL, not a volume path. |
| LOSS_RUN_DATABRICKS_CATALOG | PROD catalog name. |
| DB_SERVER / DB_NAME | PROD instance / CLMAA_SpecialAccounts. |
| DB_DRIVER | Installed supported ODBC driver name; match image exactly. |
| DB_AUTH | DBA/platform-approved managed-identity mode; current ODBC supports ActiveDirectoryMsi. Not ActiveDirectoryInteractive in Azure. |
| DATABRICKS_PROFILE | Local only; not required for the Azure branch. |
| Existing SSO/JWT/CORS/cookie settings | Preserve and verify existing PROD values; do not copy local secrets or localhost origins. |

There are no new scheduler environment variables. Schedule dates/enabled state reside in SQL. LOSS_RUN_SCHEDULE_TEST_MODE and LOSS_RUN_SCHEDULE_TEST_REPORT_TYPE were removed; remove obsolete local entries and never add them to PROD. Do not package .env, local CLI credentials or generated customer spreadsheets into the release artifact.

Deploy pinned requirements, including databricks-sdk, azure-identity, requests, pyodbc, pandas, openpyxl, Pillow and tzdata. Verify each pinned package is available from the approved feed and the native ODBC driver is installed. Preserve the Python/runtime version validated in PREPROD; code uses modern Python syntax and existing SQL features. Run the full agreed CI checks, not only scheduler unit tests.

app.py starts LossRunWorker through FastAPI lifespan whenever ENVIRONMENT is not local. Keep lifespan enabled in the production ASGI server. Do not launch a second independent scheduler/cron/SQL Agent job in addition to this. Confirm worker-start logs after deployment. Each ASGI process may start a worker; leases/SQL locks support coordination, but validate concurrency and avoid mixed code versions.

Use App Service Always On where supported and operational monitoring. Set it in App Service configuration/general settings through the platform team. Prevent accidental slot/background processes from pointing to the same production queue; identity is also slot-specific. Do not assume a slot warmup with PROD settings is harmless: it can start a worker before traffic is swapped.

The worker checks the queue every 10 seconds while idle; monthly schedules immediately on startup then about every 60 seconds while idle. During generation the loop waits. Heartbeats run every 60 seconds with a five-minute lease and a three-attempt retry limit. Deployments can interrupt work; verify lease recovery and do not treat this as guaranteed exactly-once file writing.

Reference: [App Service settings and Always On](https://learn.microsoft.com/en-us/azure/app-service/configure-common).

## 8. UI integration and manual production smoke tests

Owner: UI developer, backend engineer and QA. Why: API success does not prove that users can navigate, observe progress and download.

The root LossRuns.js, LossRunJobs.js and JobDetails.js are copied UI components, not a complete frontend project. Incorporate the approved changes into the real UI repo/build. Verify /loss-runs and /loss-run-jobs routing, View Existing Jobs, back navigation, report-type selection and the informational monthly banner. Banner text is static/conditional wording, not a live enabled-state indicator.

Current API contract (under the existing loss_run route prefix):

| Endpoint | Verify |
| --- | --- |
| GET /loss_run/accounts | Eligible accounts, including On Board Date; shared search_sac_account API unchanged. |
| POST /loss_run/generate | customerNumbers array (even one account), optional reportType. |
| POST /loss_run/generate-all | reportType standard or claim_review; default standard. |
| GET /loss_run/jobs | All jobs, CreatedAt descending. |
| GET /loss_run/jobs/{job_id} | Status, phase, counts, failure details, report type and schedule metadata. |
| GET /loss_run/jobs/{job_id}/download | One output XLSX, multiple outputs ZIP; completed/partially_completed jobs only. |

Example selected request: {"customerNumbers":["<APPROVED-TEST-CUSTOMER>"],"reportType":"claim_review"}. New submissions return HTTP 202 with jobId/status/message rather than waiting for the query. When a job is already queued/processing, existing code returns the existing job information instead of inserting another. UI should display that response rather than assume every 202 created a new job.

Smoke-test checklist:

- Generate one approved account as Standard, then Claim Review; confirm job reportType matches the downloaded template/layout. Coordinate real PROD outputs with the business.
- Attempt another submission during processing; verify existing-job protection, no duplicate job and meaningful UI feedback.
- Expect a long querying_loss_run_data phase with zero generated count; do not show a fabricated percentage or let the browser request wait for completion.
- Verify all-account success/partial failures against PROD eligibility. PREPROD counts of 239/241/245 are not production acceptance targets. Expected no-data accounts should show understandable failures.
- Confirm job date-time strings are MM-DD-YYYY hh:mm:ss AM/PM. Formatting alone does not convert timestamps to Eastern; database job times are written in UTC. Agree UI timezone presentation separately.
- Verify authenticated browser downloads, Content-Disposition filename handling, production CORS/origins and exposed Content-Disposition header. The browser controls Downloads folder/prompt; backend cannot force a user's filesystem directory.
- Test largest expected ZIP, timeout, memory/disk usage, cleanup and failed-read behavior. The backend prepares ZIPs using temporary local storage and removes successful response files afterward; crashes need operational cleanup monitoring.
- Current Jobs component requests polling through useIntervalFetch(..., 300000), i.e. five minutes. Decide whether that refresh rate is acceptable; the hook and route configuration are outside this repo.

## 9. Enable the real monthly schedules last

Owner: DBA/release operator after business/QA approval. Why: deployment alone does not enable disabled schedule rows, and enabling too early can launch reports immediately.

1. Verify both manual report types and downloads first; stop old workers. Confirm no incompatible deployment remains attached to this queue.
2. Open Loss_Run_Enable_Schedules.sql in SSMS connected to PROD. It currently contains @ActiveFromDate = '2026-09-17'. Replace this with the approved production Eastern start date before execution; do not blindly reuse the development date. The script rejects dates before Eastern today.
3. Run the complete script and verify both schedule rows: monthly_standard / standard / day 1; monthly_claim_review / claim_review / day 20; RunAtLocalTime 00:00:00; TimeZoneName America/New_York; chosen ActiveFromDate; IsEnabled 1.
4. Save the result grid and next expected occurrence in the deployment ticket. If activated on the 1st/20th after midnight, that day's run is immediately due. If a job is active it waits. No runs before ActiveFromDate are intended.
5. At the first real occurrence confirm one scheduled job, correct reportType, ScheduleId and local ScheduledForDate, expected requester system:loss-run-scheduler, counts and downloadable correct-format reports.

Midnight means Eastern local time year-round: daylight saving is followed, not permanent UTC-5. Busy workers/downtime can delay execution. On restart, missed occurrences since activation are considered oldest first. Catch-up uses current data, not historical snapshot data. Existing occurrences, including failed jobs, are not recreated; retries use existing job/lease behavior. Retain occurrence history to preserve duplicate protection.

No UI needs to be open. No Azure Queue, Azure timer service or SQL Agent job is used. Automatic output is stored for user download; it is not emailed and does not download to an offline user's computer.

Read-only verification in SSMS:

```sql
USE [CLMAA_SpecialAccounts];
SELECT ScheduleId, ReportType, DayOfMonth, RunAtLocalTime,
       TimeZoneName, ActiveFromDate, IsEnabled
FROM dbo.tblLossRunSchedule ORDER BY DayOfMonth;

SELECT TOP (20) JobId, ReportType, TriggerSource, ScheduleId,
       ScheduledForDate, Status, Phase, CreatedAt, WorkerId,
       LastHeartbeatAt, LeaseUntil, AttemptCount
FROM dbo.tblLossRunJob ORDER BY CreatedAt DESC;
```

## 10. Monitoring, incident handling and rollback

Owner: operations with DBA/backend/Databricks escalation. Assign named support contacts and an agreed completion-time alert threshold before release.

| Symptom | First checks |
| --- | --- |
| Job remains queued | Worker startup/lifespan, ENVIRONMENT, DB permissions, old workers/slots, logs. |
| Query phase stays unchanged | View execution/load/blocking; compare known long baseline. Do not repeatedly submit. |
| 403 from Databricks | Actual PROD identity mapping, workspace assignment, catalog/schema/volume permissions, authenticated file operation. |
| Local works but Azure fails | Different identities/auth/network; personal CLI login proves only the user account. |
| Wrong report layout | Job ReportType, worker version/hostname, template contents/path; stop mixed-version workers. |
| 502 download failure | Underlying Databricks response, OutputPath existence/access, temporary disk and full ZIP size; inspect server logs securely. |
| Gateway timeout | Identify generation versus ZIP download; ensure generate endpoints only queue. Never assume timeout means processing stopped. |
| Duplicate or overwritten files | Multiple producers/legacy script, worker retries, standard filename collisions and overwrite behavior. |

Monitor failed/partially_completed jobs, expected scheduled occurrences, worker heartbeat age, SQL duration, template/volume availability, disk usage and completed download behavior. Confirm whether the old Databricks/script schedule still runs: business must approve disabling or coordinating it so it does not duplicate the new 1st/20th reports.

To pause future automatic submissions, an authorized DBA can run:

```sql
USE [CLMAA_SpecialAccounts];
UPDATE dbo.tblLossRunSchedule SET IsEnabled = 0
WHERE ScheduleId IN ('monthly_standard', 'monthly_claim_review');
```

This does not cancel queued/running jobs. Coordinate worker shutdown/draining and incident recovery; there is no cancellation API in the current scope. Retain job/account rows and outputs for audit. Do not delete scheduled rows or manually reset statuses as a routine rollback.

Rollback to a known schema-compatible application build. An older worker that ignores ReportType can generate the wrong file. Do not drop new columns/tables to roll back without DBA review. Re-enabling an old ActiveFromDate can cause catch-up; decide explicitly whether to preserve missed occurrences or advance activation with an audit record.

## 11. Local-only setup: reference, not production steps

These steps were needed for developer testing and must NOT be repeated as PROD authentication setup: install databricks-sdk from requirements; separately install the Databricks CLI; restart PowerShell/VS Code so PATH refreshes; run databricks version, then databricks auth login --host <DEV-WORKSPACE-URL> --profile claims-preprod. The CLI is a separate executable, not py -m databricks. Expired local login may require reauthentication. Personal workspace/volume permissions remain necessary.

Local mode uses .env and the SDK profile, and starts the worker separately with py -m services.loss_run.loss_run_worker from repo root. Module separators are dots, not slashes. Azure uses app settings and system-assigned identity; no personal CLI/profile login is needed there. Local manual testing must not target the production queue or production output volume. The monthly scheduler is disabled locally.

## 12. Final go/no-go record

- [ ] Production resources, identity IDs, ownership and release commit recorded.
- [ ] Base job-table DDL exported/reviewed and PROD schema verified; no PREPROD job data copied.
- [ ] View, filtered dependencies, cross-DB access and account/history scope approved.
- [ ] SQL migration applied, constraints/indexes verified; runtime grants tested as PROD identity.
- [ ] Databricks identity registered/assigned; template read plus output write/read proven from Azure.
- [ ] Both approved templates deployed; output path, overwrite/retention and privacy policy approved.
- [ ] Correct PROD settings/dependencies/ODBC installed; worker running; old workers/legacy schedules coordinated.
- [ ] Standard and Claim Review generation, queue conflict, partial failures and largest download verified.
- [ ] UI navigation issue resolved; report selection, status, banner and downloads tested in actual frontend.
- [ ] Pending inactive/history/performance requirements explicitly excluded or separately completed and accepted.
- [ ] Activation date approved, schedules enabled, next expected occurrences recorded.
- [ ] Monitoring owner, rollback build and first scheduled-run review assigned.

Deployment owner: ____________________  DBA: ____________________
Databricks owner: ____________________  QA/business approval: ____________________
Activation date / change ticket / evidence links: __________________________________

This document does not replace deployment approvals. No production changes were executed while preparing it. Supporting repo files: Loss_Run_Scheduling_Migration.sql, Loss_Run_Enable_Schedules.sql, Loss_Run_Automatic_Scheduling.md, Loss_Run_Report_Types_Testing.md, SQLQuery4.sql and the current backend/UI components.
