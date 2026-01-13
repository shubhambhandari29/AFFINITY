from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.auth import router as auth_router
from api.dropdowns import router as dropdowns_router
from api.sac.claim_review_distribution import router as claim_review_distribution_router
from api.sac.claim_review_frequency import router as claim_review_frequency_router
from api.sac.deduct_bill_distribution import router as deduct_bill_distribution_router
from api.sac.deduct_bill_frequency import router as deduct_bill_frequency_router
from api.sac.hcm_users import router as hcm_users_router
from api.sac.loss_run_distribution import router as loss_run_distribution_router
from api.sac.loss_run_frequency import router as loss_run_frequency_router
from api.sac.sac_account import router as sac_account_router
from api.sac.sac_affiliates import router as sac_affiliates_router
from api.sac.sac_policies import router as sac_policies_router
from api.sac.search_sac_account import router as search_sac_account_router
from core.config import settings
from core.logging_config import configure_logging

"""from api.affinity.affinity_program import router as affinity_program_router
from api.affinity.affinity_agents import router as affinity_agents_router
from api.affinity.affinity_policy_types import router as affinity_policy_types_router
from api.affinity.search_affinity_program import router as search_affinity_program_router
from api.affinity.loss_run_distribution import router as loss_run_distribution_affinity_router
from api.affinity.claim_review_distribution import router as claim_review_distribution_affinity_router
from api.affinity.policy_type_distribution import router as policy_type_distribution_affinity_router
from api.affinity.loss_run_frequency import router as loss_run_frequency_affinity_router
from api.affinity.claim_review_frequency import router as claim_review_frequency_affinity_router"""

configure_logging()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["home page"])
async def home():
    return {"message":"Welcome to SAC"}


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "ok"}


app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(dropdowns_router, prefix="/dropdowns", tags=["dropdowns"])

# sac
app.include_router(sac_account_router, prefix="/sac_account", tags=["sac_account"])
app.include_router(sac_policies_router, prefix="/sac_policies", tags=["sac_policies"])
app.include_router(hcm_users_router, prefix="/hcm_users", tags=["hcm_users"])
app.include_router(sac_affiliates_router, prefix="/sac_affiliates", tags=["sac_affiliates"])
app.include_router(
    search_sac_account_router, prefix="/search_sac_account", tags=["search_sac_account"]
)

app.include_router(
    loss_run_distribution_router, prefix="/loss_run_distribution", tags=["loss_run_distribution"]
)
app.include_router(
    claim_review_distribution_router,
    prefix="/claim_review_distribution",
    tags=["claim_review_distribution"],
)
app.include_router(
    deduct_bill_distribution_router,
    prefix="/deduct_bill_distribution",
    tags=["deduct_bill_distribution"],
)

app.include_router(
    loss_run_frequency_router, prefix="/loss_run_frequency", tags=["loss_run_frequency"]
)
app.include_router(
    claim_review_frequency_router, prefix="/claim_review_frequency", tags=["claim_review_frequency"]
)
app.include_router(
    deduct_bill_frequency_router, prefix="/deduct_bill_frequency", tags=["deduct_bill_frequency"]
)

# affinity
"""app.include_router(affinity_program_router, prefix="/affinity_program", tags=["affinity_program"])
app.include_router(affinity_agents_router, prefix="/affinity_agents", tags=["affinity_agents"])
app.include_router(affinity_policy_types_router, prefix="/affinity_policy_types", tags=["affinity_policy_types"])
app.include_router(search_affinity_program_router, prefix="/search_affinity_program", tags=["search_affinity_program"])

app.include_router(loss_run_distribution_affinity_router, prefix="/loss_run_distribution_affinity", tags=["loss_run_distribution_affinity"])
app.include_router(claim_review_distribution_affinity_router, prefix="/claim_review_distribution_affinity", tags=["claim_review_distribution_affinity"])
app.include_router(policy_type_distribution_affinity_router, prefix="/policy_type_distribution_affinity", tags=["policy_type_distribution_affinity"])

app.include_router(loss_run_frequency_affinity_router, prefix="/loss_run_frequency_affinity", tags=["loss_run_frequency_affinity"])
app.include_router(claim_review_frequency_affinity_router, prefix="/claim_review_frequency_affinity", tags=["claim_review_frequency_affinity"])
"""
