import os
import random

from locust import HttpUser, between, task
from locust.exception import StopUser


class SacAffinityMixedLoadUser(HttpUser):
    """
    Mixed SAC + Affinity starter profile (18 endpoints):
    - Auth: login/me/refresh
    - SAC: account, policies, premium, search, frequency reads + one upsert
    - Affinity: program/agents/policy types/search, frequency reads + one upsert
    - Utility POST: outlook compose link

    Notes:
    - Most traffic is read-only.
    - Write endpoints are low-weight and use stable test keys.
    """

    wait_time = between(1, 3)

    email = os.getenv("SAC_TEST_EMAIL", "mbond@hanover.com")
    password = os.getenv("SAC_TEST_PASSWORD", "12345678")
    sac_customer_num = os.getenv("SAC_TEST_CUSTOMER_NUM", "LT_CUST_001")
    affinity_program_name = os.getenv("AFF_TEST_PROGRAM_NAME", "LT_PROGRAM_001")
    enable_mutating_posts = os.getenv("ENABLE_MUTATING_POSTS", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    sac_search_modes = [
        "AccountName",
        "CustomerNum",
        "PolicyNum",
        "ProducerCode",
        "PolicyNameInsured",
        "AffiliateName",
    ]
    affinity_search_modes = ["ProgramName", "ProducerCode"]

    def _login(self) -> None:
        payload = {"email": self.email, "password": self.password}
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        with self.client.post(
            "/auth/login",
            json=payload,
            headers=headers,
            name="POST /auth/login",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(
                    f"Login failed status={response.status_code} body={response.text}"
                )
                raise StopUser("Stopping user because login failed.")
            response.success()

    def _handle_response(self, response, method: str, path: str) -> None:
        if response.status_code == 401:
            response.failure(f"{method} {path} returned 401. Session may be expired.")
            self._login()
            return
        if response.status_code >= 400:
            response.failure(f"{method} {path} status={response.status_code} body={response.text}")
            return
        response.success()

    def _get(self, path: str, *, name: str, params: dict | None = None) -> None:
        with self.client.get(path, params=params, name=name, catch_response=True) as response:
            self._handle_response(response, "GET", path)

    def _post(self, path: str, *, name: str, payload: dict | list | None = None) -> None:
        with self.client.post(path, json=payload, name=name, catch_response=True) as response:
            self._handle_response(response, "POST", path)

    def on_start(self) -> None:
        self._login()

    # -----------------
    # Auth + Utilities
    # -----------------
    @task(3)
    def auth_me(self) -> None:
        self._get("/auth/me", name="GET /auth/me")

    @task(1)
    def refresh_token(self) -> None:
        self._post("/auth/refresh_token", name="POST /auth/refresh_token")

    @task(1)
    def outlook_compose_link(self) -> None:
        payload = {
            "recipients": ["loadtest@example.com"],
            "subject": "Load Test",
            "body": "SAC API load test validation",
        }
        self._post("/outlook/compose_link", name="POST /outlook/compose_link", payload=payload)

    # --------
    # SAC GET
    # --------
    @task(4)
    def sac_account_get(self) -> None:
        self._get(
            "/sac_account",
            name="GET /sac_account",
            params={"CustomerNum": self.sac_customer_num},
        )

    @task(4)
    def sac_policies_get(self) -> None:
        self._get(
            "/sac_policies",
            name="GET /sac_policies",
            params={"CustomerNum": self.sac_customer_num},
        )

    @task(2)
    def sac_premium_get(self) -> None:
        self._get(
            "/sac_policies/get_premium",
            name="GET /sac_policies/get_premium",
            params={"CustomerNum": self.sac_customer_num},
        )

    @task(2)
    def sac_search_get(self) -> None:
        self._get(
            "/search_sac_account",
            name="GET /search_sac_account",
            params={"search_by": random.choice(self.sac_search_modes)},
        )

    @task(2)
    def sac_loss_run_frequency_get(self) -> None:
        self._get(
            "/loss_run_frequency",
            name="GET /loss_run_frequency",
            params={"CustomerNum": self.sac_customer_num},
        )

    @task(2)
    def sac_claim_review_frequency_get(self) -> None:
        self._get(
            "/claim_review_frequency",
            name="GET /claim_review_frequency",
            params={"CustomerNum": self.sac_customer_num},
        )

    @task(2)
    def sac_deduct_bill_frequency_get(self) -> None:
        self._get(
            "/deduct_bill_frequency",
            name="GET /deduct_bill_frequency",
            params={"CustomerNum": self.sac_customer_num},
        )

    # ---------
    # SAC POST
    # ---------
    @task(1)
    def sac_loss_run_frequency_upsert(self) -> None:
        if not self.enable_mutating_posts:
            return
        payload = [
            {
                "CustomerNum": self.sac_customer_num,
                "MthNum": random.randint(1, 12),
                "RptMth": random.randint(1, 12),
                "NoClaims": "No",
            }
        ]
        self._post(
            "/loss_run_frequency/upsert",
            name="POST /loss_run_frequency/upsert",
            payload=payload,
        )

    # -------------
    # Affinity GET
    # -------------
    @task(3)
    def affinity_program_get(self) -> None:
        self._get(
            "/affinity_program",
            name="GET /affinity_program",
            params={"ProgramName": self.affinity_program_name},
        )

    @task(2)
    def affinity_agents_get(self) -> None:
        self._get(
            "/affinity_agents",
            name="GET /affinity_agents",
            params={"ProgramName": self.affinity_program_name},
        )

    @task(2)
    def affinity_policy_types_get(self) -> None:
        self._get(
            "/affinity_policy_types",
            name="GET /affinity_policy_types",
            params={"ProgramName": self.affinity_program_name},
        )

    @task(2)
    def affinity_search_get(self) -> None:
        self._get(
            "/search_affinity_program",
            name="GET /search_affinity_program",
            params={"search_by": random.choice(self.affinity_search_modes)},
        )

    @task(2)
    def affinity_loss_run_frequency_get(self) -> None:
        self._get(
            "/loss_run_frequency_affinity",
            name="GET /loss_run_frequency_affinity",
            params={"ProgramName": self.affinity_program_name},
        )

    @task(2)
    def affinity_claim_review_frequency_get(self) -> None:
        self._get(
            "/claim_review_frequency_affinity",
            name="GET /claim_review_frequency_affinity",
            params={"ProgramName": self.affinity_program_name},
        )

    # --------------
    # Affinity POST
    # --------------
    @task(1)
    def affinity_loss_run_frequency_upsert(self) -> None:
        if not self.enable_mutating_posts:
            return
        payload = [
            {
                "ProgramName": self.affinity_program_name,
                "MthNum": random.randint(1, 12),
                "RptMth": random.randint(1, 12),
                "NoClaims": "No",
            }
        ]
        self._post(
            "/loss_run_frequency_affinity/upsert",
            name="POST /loss_run_frequency_affinity/upsert",
            payload=payload,
        )
