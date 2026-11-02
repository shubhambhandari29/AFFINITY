USE [CLMAA_SpecialAccounts];
SET NOCOUNT ON;

-- Read-only inventory: only temporary tables are written.
-- No large financial source is queried and no application tables are changed.
-- Matches the view's current claim/current policy and REKEY rules.
-- Deliberately omits the final history/reserve HAVING filter: missing financials
-- must not cause claims to be omitted from the proposed backfill scope.
-- This is an inventory of claims linked through current policy records, not
-- proof that every historical claim/version is present in LakeHouse.

DROP TABLE IF EXISTS #InactiveSACAccounts;
DROP TABLE IF EXISTS #InactiveSACClaims;

SELECT CustomerNum, CustomerName, AcctStatus, LossRunDistFreq
INTO #InactiveSACAccounts
FROM dbo.tblAcctSpecial
WHERE AcctStatus = 'Inactive'
  AND LossRunDistFreq <> 'Not Needed'
  AND LossRunDistFreq <> '';

SELECT DISTINCT
    a.CustomerNum,
    c.CLM_NBR AS ClaimNumber,
    c.INCID_RPT_IND AS RecordOnlyIndicator
INTO #InactiveSACClaims
FROM #InactiveSACAccounts a
JOIN CLM_LakeHouse.dbo.DW_POL_DIMNSN p
  ON p.PMS_CUSTMR_NBR = a.CustomerNum
 AND p.POL_CURR_VERSN_IND = 'Y'
JOIN CLM_LakeHouse.dbo.DW_CLMS_CLM_DIMNSN c
  ON c.POL_SYM = p.POL_SYM
 AND UPPER(c.POL_NBR) = p.POL_NBR
 AND c.POL_MOD = p.POL_MOD
 AND c.CURR_VERSN_IND = 'Y'
WHERE UPPER(ISNULL(c.ACCT_LOSS_LOC_ID, 'NA')) NOT LIKE '%REKEY%';

-- Grid 1: overall scope. Unique claims can belong to more than one account
-- mapping, so do not add account counts to estimate distinct source claims.
SELECT
    @@SERVERNAME AS ServerName,
    (SELECT COUNT(*) FROM #InactiveSACAccounts) AS EligibleInactiveAccounts,
    (SELECT COUNT(DISTINCT CustomerNum) FROM #InactiveSACClaims) AS AccountsWithClaims,
    (SELECT COUNT(DISTINCT ClaimNumber) FROM #InactiveSACClaims) AS UniqueClaimNumbers;

-- Grid 2: accounts and claim counts, including accounts with zero matches.
SELECT
    a.CustomerNum AS CustomerNumber,
    a.CustomerName,
    a.AcctStatus,
    a.LossRunDistFreq,
    COUNT(DISTINCT c.ClaimNumber) AS ClaimCount,
    COUNT(DISTINCT CASE WHEN c.RecordOnlyIndicator = 'Y' THEN c.ClaimNumber END)
        AS RecordOnlyClaimCount
FROM #InactiveSACAccounts a
LEFT JOIN #InactiveSACClaims c ON c.CustomerNum = a.CustomerNum
GROUP BY a.CustomerNum, a.CustomerName, a.AcctStatus, a.LossRunDistFreq
ORDER BY ClaimCount DESC, a.CustomerNum;

-- Grid 3: detailed account/claim mapping to share with the DB/data team.
SELECT
    a.CustomerNum AS CustomerNumber,
    a.CustomerName,
    c.ClaimNumber,
    c.RecordOnlyIndicator
FROM #InactiveSACClaims c
JOIN #InactiveSACAccounts a ON a.CustomerNum = c.CustomerNum
ORDER BY a.CustomerNum, c.ClaimNumber;

-- Grid 4: deduplicated claim-number scope for a set-based financial-source join.
-- Do not execute one SELECT against the financial table for each claim.
SELECT DISTINCT ClaimNumber
FROM #InactiveSACClaims
ORDER BY ClaimNumber;

DROP TABLE #InactiveSACClaims;
DROP TABLE #InactiveSACAccounts;
