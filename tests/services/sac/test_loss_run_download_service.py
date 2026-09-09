import asyncio
import os
from uuid import uuid4
from zipfile import ZipFile

import pytest
from fastapi import HTTPException

from services.loss_run import loss_run_download_service


def test_download_rejects_job_that_is_still_processing(monkeypatch):
    async def fake_job(_job_id):
        return {"Status": "processing"}

    monkeypatch.setattr(loss_run_download_service, "get_job", fake_job)

    with pytest.raises(HTTPException) as error:
        asyncio.run(loss_run_download_service.prepare_loss_run_download(uuid4()))

    assert error.value.status_code == 409


def test_download_returns_single_excel(monkeypatch):
    job_id = uuid4()

    async def fake_job(_job_id):
        return {"Status": "completed"}

    async def fake_outputs(_job_id):
        return [
            {
                "CustomerNumber": "00123",
                "OutputPath": "/Volumes/reports/Customer.xlsx",
            }
        ]

    class Storage:
        @staticmethod
        def download_report_to(path, destination):
            assert path == "/Volumes/reports/Customer.xlsx"
            destination.write(b"workbook")

    monkeypatch.setattr(loss_run_download_service, "get_job", fake_job)
    monkeypatch.setattr(loss_run_download_service, "get_completed_outputs", fake_outputs)
    monkeypatch.setattr(loss_run_download_service, "DatabricksLossRunStorage", Storage)

    result = asyncio.run(loss_run_download_service.prepare_loss_run_download(job_id))
    try:
        assert result.filename == "Customer.xlsx"
        assert result.media_type.endswith("spreadsheetml.sheet")
        with open(result.path, "rb") as downloaded:
            assert downloaded.read() == b"workbook"
    finally:
        os.remove(result.path)


def test_download_creates_zip_for_multiple_excels(monkeypatch):
    job_id = uuid4()

    async def fake_job(_job_id):
        return {"Status": "partially_completed"}

    async def fake_outputs(_job_id):
        return [
            {
                "CustomerNumber": "00123",
                "OutputPath": "/Volumes/reports/First.xlsx",
            },
            {
                "CustomerNumber": "00456",
                "OutputPath": "/Volumes/reports/Second.xlsx",
            },
        ]

    class Storage:
        @staticmethod
        def download_report_to(path, destination):
            destination.write(path.encode())

    monkeypatch.setattr(loss_run_download_service, "get_job", fake_job)
    monkeypatch.setattr(loss_run_download_service, "get_completed_outputs", fake_outputs)
    monkeypatch.setattr(loss_run_download_service, "DatabricksLossRunStorage", Storage)

    result = asyncio.run(loss_run_download_service.prepare_loss_run_download(job_id))
    try:
        assert result.filename == f"loss_run_{job_id}.zip"
        assert result.media_type == "application/zip"
        with ZipFile(result.path) as archive:
            assert archive.namelist() == ["First.xlsx", "Second.xlsx"]
    finally:
        os.remove(result.path)
