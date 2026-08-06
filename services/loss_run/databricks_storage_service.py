from io import BytesIO
from urllib.parse import quote

import requests
from azure.identity import ManagedIdentityCredential

from core.config import settings

DATABRICKS_TOKEN_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
REQUEST_TIMEOUT_SECONDS = 60


class DatabricksLossRunStorage:
    def __init__(self) -> None:
        if not settings.DATABRICKS_HOST:
            raise ValueError("DATABRICKS_HOST is not configured")
        if not settings.LOSS_RUN_DATABRICKS_CATALOG:
            raise ValueError("LOSS_RUN_DATABRICKS_CATALOG is not configured")

        self.host = settings.DATABRICKS_HOST.rstrip("/")
        self.catalog = settings.LOSS_RUN_DATABRICKS_CATALOG
        self.template_path = (
            f"/Volumes/{self.catalog}/gold/statics/SACLossRunTemplate.xlsx"
        )
        self.output_directory = (
            f"/Volumes/{self.catalog}/gold/external_volume/"
            "specialaccounts_lossruns_temporary"
        )
        self.is_local = settings.ENVIRONMENT.strip().lower() == "local"

        if self.is_local:
            from databricks.sdk import WorkspaceClient

            self.client = WorkspaceClient(
                host=self.host,
                profile=settings.DATABRICKS_PROFILE,
            )
            self.credential = None
        else:
            self.client = None
            self.credential = ManagedIdentityCredential()

    def download_template(self) -> bytes:
        if self.client:
            download = self.client.files.download(self.template_path)
            with download.contents as stream:
                return stream.read()

        response = requests.get(
            self._files_api_url(self.template_path),
            headers=self._authorization_header(),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        self._raise_for_databricks_error(response, "template download")
        return response.content

    def upload_report(self, filename: str, workbook_bytes: bytes) -> str:
        output_path = f"{self.output_directory}/{filename}"

        if self.client:
            self.client.files.upload(
                output_path,
                BytesIO(workbook_bytes),
                overwrite=True,
            )
            return output_path

        response = requests.put(
            self._files_api_url(output_path),
            headers={
                **self._authorization_header(),
                "Content-Type": "application/octet-stream",
            },
            params={"overwrite": "true"},
            data=workbook_bytes,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        self._raise_for_databricks_error(response, "report upload")
        return output_path

    def _authorization_header(self) -> dict[str, str]:
        access_token = self.credential.get_token(DATABRICKS_TOKEN_SCOPE)
        return {"Authorization": f"Bearer {access_token.token}"}

    def _files_api_url(self, path: str) -> str:
        return f"{self.host}/api/2.0/fs/files{quote(path, safe='/')}"

    @staticmethod
    def _raise_for_databricks_error(
        response: requests.Response, operation: str
    ) -> None:
        if response.ok:
            return

        try:
            error = response.json()
            detail = error.get("message") or error.get("error_code") or response.text
        except ValueError:
            detail = response.text

        raise RuntimeError(
            f"Databricks {operation} failed with HTTP {response.status_code}: {detail}"
        )
