from services.affinity.search_affinity_programs_service import (
    search_affinity_program_records as search_affinity_program_records_service,
)


async def search_cct_affinity_program_records(search_by: str):
    return await search_affinity_program_records_service(search_by)
