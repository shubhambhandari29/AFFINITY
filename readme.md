# SAC Backend

Backend services for Special Accounts Center (SAC). The app authenticates internal users, proxies curated SQL Server datasets, and exposes CRUD and search APIs that power the SAC front-end.

## Highlights
- FastAPI application with modular routers under `api/` and a thin service layer under `services/`.
- Centralized SQL Server helpers in `core/db_helpers.py` for parameterized queries, upserts, deletes, and filter validation.
- JWT plus secure-cookie authentication in `services/auth_service.py` for both database login and F5 login flows.
- Pydantic request models under `core/models/` for validation and OpenAPI generation.
- SAC, Affinity, dropdown, and Outlook compose endpoints served from one app instance.

## Architecture Overview
```text
app.py
 ├── api/
 │   ├── auth.py              # login, F5 login, me, logout, refresh
 │   ├── sac/                 # SAC feature routers
 │   ├── affinity/            # Affinity feature routers
 │   └── outlook_compose.py   # Outlook compose link endpoint
 ├── services/                # Business logic and DB orchestration
 ├── core/                    # Config, JWT handling, DB helpers, shared models/utilities
 ├── db.py                    # pyodbc connection helpers
 └── tests/                   # pytest suites
```
Each route validates input with a Pydantic schema, enforces authentication where required, and delegates business logic to its corresponding service module.

## Prerequisites
- Python 3.11+
- Access to the SAC SQL Server instance and the appropriate Azure AD or SQL credentials
- Microsoft ODBC Driver 17 for SQL Server, or a compatible driver, installed on the host

## Getting Started
1. **Clone and create a virtual environment**
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
   ENVIRONMENT=Dev
   DB_DRIVER={ODBC Driver 17 for SQL Server}
   DB_SERVER=<server>.database.windows.net
   DB_NAME=<database>
   DB_AUTH=ActiveDirectoryInteractive
   SECRET_KEY=<random-64-character-string>
   ACCESS_TOKEN_VALIDITY=480
   REFRESH_TOKEN_VALIDITY=10080
   FRONTEND_URL=http://localhost:3000
   SECURE_COOKIE=false
   SAME_SITE=lax
   OUTLOOK_COMPOSE_BASE_URL=https://outlook.office.com/mail/deeplink/compose
   ```
   For multiple allowed frontend origins, use a comma-separated value:
   `FRONTEND_URL=https://sacplatformwebpreprd.azurewebsites.net,https://sacplatformpreprd.allmerica.com`

   Do not commit real secrets. Use your deployment platform's secret store in non-local environments.

4. **Run the API**
   ```bash
   uvicorn app:app --reload
   ```
   Interactive docs are available at `http://localhost:8000/docs`.

## Authentication
- `POST /auth/login` authenticates against `tblUsers` using `email` and `password`.
- `POST /auth/F5_login` accepts a payload like `{"user": "<UserID>", "groups": [...]}` from F5.
- Supported SAC role groups for F5 login are `ADMIN`, `DIRECTOR`, `UNDERWRITER`, and `CCT_User`.
- Incoming F5 groups are matched case-insensitively and returned in canonical form: `Admin`, `Director`, `Underwriter`, `CCT_User`.
- If multiple F5 role groups are present, roles are returned in this priority order: `Admin`, `Director`, `Underwriter`, `CCT_User`.
- Successful login sets two cookies: `session` for the access token and `refresh_session` for the refresh token.
- `GET /auth/me` reads the current user from the `session` cookie.
- `POST /auth/refresh` reissues only a new access token. It does not rotate the refresh token.
- `POST /auth/logout` clears both auth cookies.

### Branch Resolution
- Database-backed login resolves branch by email using `tblBranchMapping`.
- F5 login resolves branch by `UserID` only when the resolved role list includes `Director`.
- If no branch mapping row is found, the branch defaults to `All`.

## Logging
This repo does not ship a custom logging configuration module anymore. Service modules still use Python `logging`, but handlers, log levels, and output destinations now come from the ASGI server and hosting platform configuration.

## Running Tests and Tooling
```bash
pytest
pytest --cov
ruff check .
black .
```
Tests currently cover core helpers, API route wiring, and selected services. Expand coverage when you add or change endpoint behavior.

## Deployment Notes
- The service is stateless and can be scaled behind your preferred gateway or worker model.
- Configure environment-specific CORS, cookie flags, and SQL connection settings through environment variables.
- Ensure the runtime has Microsoft ODBC drivers installed and outbound access to the SQL Managed Instance.
- In HTTPS deployments, set `SECURE_COOKIE=true` and choose an appropriate `SAME_SITE` value for your frontend flow.
- If you need application logs in Azure or another host, configure stdout and stderr collection at the platform level.

## Contributing
1. Create a feature branch.
2. Keep modules small by adding routers, services, and models instead of growing existing files unnecessarily.
3. Run `ruff`, `black`, and the relevant `pytest` suites before opening a PR.
4. Update the README and API documentation when endpoint behavior changes.

Questions or improvement ideas should go through the SAC platform team.