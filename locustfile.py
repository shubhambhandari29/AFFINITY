import os
import random

from locust import HttpUser, between, task
from locust.exception import StopUser


class SacSmokeLoadUser(HttpUser):
    """
    Starter load profile for a small API subset:
    - Auth login once per simulated user
    - auth/me (session validation)
    - sac_policies/get_premium (read endpoint)
    - search_sac_account (read endpoint with simple query param)
    """

    wait_time = between(1, 3)

    email = os.getenv("SAC_TEST_EMAIL", "mbond@hanover.com")
    password = os.getenv("SAC_TEST_PASSWORD", "12345678")
    search_modes = [
        "AccountName",
        "CustomerNum",
        "PolicyNum",
        "ProducerCode",
        "PolicyNameInsured",
        "AffiliateName",
    ]

    def on_start(self) -> None:
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
                    f"Login failed with status={response.status_code}, body={response.text}"
                )
                raise StopUser("Stopping user because login failed.")
            response.success()

    @task(4)
    def get_current_user(self) -> None:
        self.client.get("/auth/me", name="GET /auth/me")

    @task(3)
    def get_premium(self) -> None:
        self.client.get("/sac_policies/get_premium", name="GET /sac_policies/get_premium")

    @task(2)
    def search_sac_account(self) -> None:
        search_by = random.choice(self.search_modes)
        self.client.get(
            "/search_sac_account",
            params={"search_by": search_by},
            name="GET /search_sac_account",
        )

    @task(1)
    def refresh_token(self) -> None:
        self.client.post("/auth/refresh_token", name="POST /auth/refresh_token")
