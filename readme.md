# SAC Backend

Backend services for Special Accounts Center (SAC). The app authenticates internal users, proxies curated SQL Server datasets, and exposes CRUD/search APIs that power the SAC front-end.

## Highlights
- FastAPI application with modular routers per feature domain (`api/`) and thin service layer (`services/`).
- Centralized SQL Server access helpers (`core/db_helpers.py`) that validate filters, run parameterized queries, and support upserts/deletes.
- JWT + secure cookie authentication managed in `services/auth_service.py` with bcrypt hashing for migrated accounts.
- Pydantic models under `core/models/` enforce request validation and give automatic OpenAPI documentation.
- Structured application logging with log rotation (`core/logging_config.py`).

## Architecture Overview
```
app.py
 ├── api/                  # FastAPI routers
 │     ├── auth/           # Login/logout endpoints
 │     └── sac/            # SAC feature routers (accounts, policies, distributions, etc.)
 ├── services/             # Business logic and DB orchestration
 ├── core/                 # Cross-cutting concerns (config, models, auth, DB helpers)
 ├── db.py                 # pyodbc connection helpers
 └── tests/                # pytest test suites
```
Each route validates input with a Pydantic schema, enforces authentication via dependency injection, and calls its corresponding service. Services lean on `core.db_helpers` to build parameterized SQL statements and transform results into JSON-friendly responses.

## Prerequisites
- Python 3.11+
- Access to the SAC SQL Server instance and the appropriate Azure AD/SQL credentials.
- Microsoft ODBC Driver 17 (or compatible) installed on the host running the service.

## Getting Started
1. **Clone & create a virtual environment**
   ```bash
   git clone <repo-url> sac-be
   cd sac-be
   python -m venv .venv
   source .venv/bin/activate
   ```
2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure environment variables**
   Copy `.env` from a secure location or create one based on the snippet below.
   ```env
   APP_NAME=SAC_Backend
   ENVIRONMENT=Dev
   DB_DRIVER={ODBC Driver 17 for SQL Server}
   DB_SERVER=<server>.database.windows.net
   DB_NAME=<database>
   DB_AUTH=ActiveDirectoryIntegrated
   SECRET_KEY=<random-64-character-string>
   ACCESS_TOKEN_VALIDITY=480
   FRONTEND_URL=http://localhost:3000
   ```
   > Do **not** commit real secrets. Use Key Vault, AWS Secrets Manager, or your chosen secret store in deployed environments.

4. **Run the API**
   ```bash
   uvicorn app:app --reload
   ```
   The interactive docs are available at `http://localhost:8000/docs`.

## Running Tests & Tooling
```bash
pytest              # run unit/integration tests
pytest --cov        # optional coverage run
ruff check .        # lint
black .             # formatting
```
Tests currently cover core models and selected services; expand them as new endpoints are added.

## Deployment Notes
- The service is stateless; scale out via multiple FastAPI/Uvicorn workers behind your preferred gateway.
- Configure environment-specific CORS, cookie flags, and SQL connection strings via environment variables.
- Ensure the runtime has Microsoft ODBC drivers installed and outbound access to the SQL Managed Instance.
- Application logs are written to `logs/app.log` (rotated daily). Ship them to your centralized logging stack in production.

## Contributing
1. Create a feature branch.
2. Keep modules small—add new routers/services/models instead of expanding large files.
3. Run `ruff` and `black` plus the relevant `pytest` suites before opening a PR.
4. Update the README and API documentation when you add or change endpoints.

Questions or improvement ideas? Open an issue or start a discussion with the SAC platform team.