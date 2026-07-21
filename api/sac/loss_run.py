from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from services.auth_service import get_current_user_from_token
from services.sac.loss_run_service import generate_loss_run

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/{customer_num}/download", response_class=FileResponse)
async def download_loss_run(customer_num: str):
    output_path = await generate_loss_run(customer_num)
    return FileResponse(
        path=output_path,
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
