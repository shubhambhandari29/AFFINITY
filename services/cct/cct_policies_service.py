from typing import Any

from services.sac.sac_policies_service import (
    get_premium as get_premium_service,
    get_sac_policies as get_sac_policies_service,
    update_field_for_all_policies as update_field_for_all_policies_service,
    upsert_sac_policies as upsert_sac_policies_service,
)


async def get_cct_policies(query_params: dict[str, Any]):
    return await get_sac_policies_service(query_params)


async def upsert_cct_policies(data: dict[str, Any]):
    return await upsert_sac_policies_service(data)


async def update_field_for_all_policies(data: dict[str, Any]):
    return await update_field_for_all_policies_service(data)


async def get_premium(query_params: dict[str, Any]):
    return await get_premium_service(query_params)
