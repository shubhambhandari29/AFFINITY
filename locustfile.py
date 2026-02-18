# locustfile.py
from locust import HttpUser, task, between

class LoginUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def login(self):
        payload = {
            "email": "mbond@hanover.com",
            "password": "12345678"
        }

        # Perform login (POST) just like your curl command
        self.client.post(
            "/auth/login",
            json=payload,
            headers={
                "accept": "application/json",
                "Content-Type": "application/json"
            },
            name="POST /auth/login"
        )