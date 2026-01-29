from typing import Any

from services.sac.sac_account_service import (
    get_sac_account as get_sac_account_service,
    upsert_sac_account as upsert_sac_account_service,
)


async def get_cct_account(query_params: dict[str, Any]):
    return await get_sac_account_service(query_params)


async def upsert_cct_account(data: dict[str, Any]):
    return await upsert_sac_account_service(data)
