from fastapi import APIRouter, Depends

from core.models.loss_run.loss_run import LossRunSelection
from services.auth_service import get_current_user_from_token
from services.loss_run.databricks_template_test_service import (
    test_azure_databricks_access,
    test_local_databricks_access,
)
from services.loss_run.loss_run_service import generate_loss_runs

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.post("/generate-all")
async def generate_all_loss_runs():
    return await generate_loss_runs()


@router.post("/generate")
async def generate_selected_loss_runs(payload: LossRunSelection):
    return await generate_loss_runs(payload.customerNumbers)


@router.post("/test-databricks-access/local")
async def test_local_databricks():
    return await test_local_databricks_access()


@router.post("/test-databricks-access/azure")
async def test_azure_databricks():
    return await test_azure_databricks_access()
