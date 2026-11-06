"""Structural/edge-case checks; live SQL equivalence and timings need SSMS."""

from datetime import datetime, timedelta
from pathlib import Path

import pytest


SQL = (Path(__file__).resolve().parents[1] / "services/sql/affinity_loss_run_views.sql").read_text()
VIEW = SQL.split("CREATE OR ALTER VIEW dbo.Affinity_Loss_Run_RecordOnly\nAS\n")[1].split("\nGO")[0]


@pytest.mark.parametrize("positions,field", [
    ("1_AND_2", "CAUSE_OF_LOSS"),
    ("3_AND_4", "LOSS_DESC"),
    ("5_AND_6", "CONTRIBUTING_FCTR"),
])
def test_record_only_uses_same_wc_aia_mapping_as_claim_details(positions, field):
    expression = (
        f"CASE WHEN CD.CLM_LOB_CD='wc' THEN CD.AIA_POSTNS_{positions}_DESC "
        f"ELSE CD.{field} END AS {field}"
    )
    assert expression in VIEW
    assert expression in SQL.split("CREATE OR ALTER VIEW dbo.Affinity_Loss_Run_RecordOnly")[0]


def test_filters_and_anti_join_preserve_record_only_rules():
    candidates, enrichment = VIEW.split(")), DATA AS (", 1)
    assert "POL.POL_CURR_VERSN_IND = 'Y'" in candidates
    assert "CD.CURR_VERSN_IND = 'Y'" in candidates
    assert "CD.INCID_RPT_IND = 'Y'" in candidates
    assert "NOT LIKE '%REKEY%'" in candidates
    assert "POL.POL_EFF_DT >= DATEADD(DAY, 1," in candidates
    assert "AND CAST(POL.POL_EFF_DT AS date) >" not in candidates
    assert candidates.index("LEFT OUTER JOIN CLM_LakeHouse.dbo.DW_CLMS_FTR_DIMNSN") < candidates.index("WHERE NOT EXISTS")
    assert "FINCL.CLM_NBR = CD.CLM_NBR AND FINCL.CLM_FTR_NBR = FD.CLM_FTR_NBR" in candidates
    assert "DW_CLMS_CLMNT_DIMNSN" not in candidates
    assert "DM_RETNTN_PRDCNG_AGNT_HR_D" not in candidates
    assert "LEFT OUTER JOIN CLM_LakeHouse.dbo.DW_CLMS_CLMNT_DIMNSN" in enrichment
    assert "CLMNT.CURR_VERSN_IND = 'Y'" in enrichment
    assert "GROUP BY" in enrichment


@pytest.mark.parametrize("effective", [
    None, datetime(2019, 12, 31, 23, 59, 59), datetime(2020, 1, 1),
    datetime(2020, 1, 1, 23, 59, 59, 999999), datetime(2020, 1, 2),
    datetime(2026, 10, 29),
])
def test_policy_date_boundary_keeps_entire_cutoff_day_excluded(effective):
    cutoff = datetime(2020, 1, 1)
    old = effective is not None and effective.date() > cutoff.date()
    revised = effective is not None and effective >= cutoff + timedelta(days=1)
    assert old == revised
