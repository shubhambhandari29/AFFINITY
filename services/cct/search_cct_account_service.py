from services.sac.search_sac_account_service import (
    search_sac_account_records as search_sac_account_records_service,
)


async def search_cct_account_records(search_by: str):
    return await search_sac_account_records_service(search_by)
