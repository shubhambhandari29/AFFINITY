from typing import Any

from services.affinity.affinity_program_service import (
    get_affinity_program as get_affinity_program_service,
    upsert_affinity_program as upsert_affinity_program_service,
)


async def get_cct_affinity_program(query_params: dict[str, Any]):
    return await get_affinity_program_service(query_params)


async def upsert_cct_affinity_program(data: dict[str, Any]):
    return await upsert_affinity_program_service(data)
