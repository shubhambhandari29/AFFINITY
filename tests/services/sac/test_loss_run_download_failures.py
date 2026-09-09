import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from services.loss_run import loss_run_download_service as downloads


@pytest.mark.parametrize(
    "job,outputs,status",
    [
        (None, [], 404),
        ({"Status": "queued"}, [], 409),
        ({"Status": "processing"}, [], 409),
        ({"Status": "failed"}, [], 409),
        ({"Status": "completed"}, [], 404),
    ],
)
def test_download_rejects_missing_or_unready_reports(monkeypatch, job, outputs, status):
    monkeypatch.setattr(downloads, "get_job", AsyncMock(return_value=job))
    monkeypatch.setattr(downloads, "get_completed_outputs", AsyncMock(return_value=outputs))
    storage = MagicMock()
    monkeypatch.setattr(downloads, "DatabricksLossRunStorage", storage)
    with pytest.raises(HTTPException) as error:
        asyncio.run(downloads.prepare_loss_run_download(uuid4()))
    assert error.value.status_code == status
    storage.assert_not_called()


@pytest.mark.parametrize("count", [1, 2])
def test_failed_download_removes_partial_file(monkeypatch, tmp_path, count):
    import tempfile

    original = tempfile.NamedTemporaryFile
    monkeypatch.setattr(
        downloads.tempfile, "NamedTemporaryFile", lambda **kwargs: original(dir=tmp_path, **kwargs)
    )
    storage = MagicMock()
    storage.download_report_to.side_effect = RuntimeError("storage unavailable")
    monkeypatch.setattr(downloads, "DatabricksLossRunStorage", lambda: storage)
    monkeypatch.setattr(
        downloads, "get_job", AsyncMock(return_value={"Status": "partially_completed"})
    )
    monkeypatch.setattr(
        downloads,
        "get_completed_outputs",
        AsyncMock(
            return_value=[
                {"OutputPath": f"/reports/{index}.xlsx", "CustomerNumber": str(index)}
                for index in range(count)
            ]
        ),
    )
    with pytest.raises(HTTPException) as error:
        asyncio.run(downloads.prepare_loss_run_download(uuid4()))
    assert error.value.status_code == 502
    assert isinstance(error.value.__cause__, RuntimeError)
    assert list(tmp_path.iterdir()) == []


def test_duplicate_filenames_are_unique_even_for_same_customer():
    used = set()
    names = [downloads._unique_filename("report.xlsx", "001", used) for _ in range(4)]
    assert names == ["report.xlsx", "report_001.xlsx", "report_001_2.xlsx", "report_001_3.xlsx"]
    assert used == set(names)
