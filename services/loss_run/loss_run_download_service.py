import os
import tempfile
from dataclasses import dataclass
from pathlib import PurePosixPath
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool

from services.loss_run.databricks_storage_service import DatabricksLossRunStorage
from services.loss_run.loss_run_job_repository import get_completed_outputs, get_job


@dataclass(frozen=True)
class LossRunDownload:
    path: str
    filename: str
    media_type: str


async def prepare_loss_run_download(job_id: UUID) -> LossRunDownload:
    job = await get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "Loss-run job not found"},
        )

    if job["Status"] not in {"completed", "partially_completed"}:
        raise HTTPException(
            status_code=409,
            detail={"error": "Loss-run job is not ready for download"},
        )

    outputs = await get_completed_outputs(job_id)
    if not outputs:
        raise HTTPException(
            status_code=404,
            detail={"error": "No generated loss-run files were found"},
        )

    try:
        return await run_in_threadpool(_prepare_download_file, job_id, outputs)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={"error": "Unable to retrieve loss-run files from Databricks"},
        ) from exc


def _prepare_download_file(job_id: UUID, outputs: list[dict]) -> LossRunDownload:
    storage = DatabricksLossRunStorage()
    suffix = ".xlsx" if len(outputs) == 1 else ".zip"
    temporary = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temporary_path = temporary.name

    try:
        if len(outputs) == 1:
            output_path = str(outputs[0]["OutputPath"])
            with temporary:
                storage.download_report_to(output_path, temporary)
            return LossRunDownload(
                path=temporary_path,
                filename=PurePosixPath(output_path).name,
                media_type=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
            )

        temporary.close()
        used_names: set[str] = set()
        with ZipFile(temporary_path, "w", compression=ZIP_DEFLATED) as archive:
            for output in outputs:
                output_path = str(output["OutputPath"])
                filename = _unique_filename(
                    PurePosixPath(output_path).name,
                    str(output["CustomerNumber"]),
                    used_names,
                )
                with archive.open(filename, "w") as destination:
                    storage.download_report_to(output_path, destination)

        return LossRunDownload(
            path=temporary_path,
            filename=f"loss_run_{job_id}.zip",
            media_type="application/zip",
        )
    except Exception:
        temporary.close()
        if os.path.exists(temporary_path):
            os.remove(temporary_path)
        raise


def _unique_filename(filename: str, customer_number: str, used: set[str]) -> str:
    if filename not in used:
        used.add(filename)
        return filename

    path = PurePosixPath(filename)
    candidate = f"{path.stem}_{customer_number}{path.suffix}"
    counter = 2
    while candidate in used:
        candidate = f"{path.stem}_{customer_number}_{counter}{path.suffix}"
        counter += 1
    used.add(candidate)
    return candidate
