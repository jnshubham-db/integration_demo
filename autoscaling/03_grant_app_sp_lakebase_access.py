"""
Script 3 (Autoscaling): Grant the Databricks App's Service Principal access to Lakebase.

What this does:
  1. Reads autoscaling_config.json for connection details
  2. Looks up the app SP client ID from the deployed app
  3. Creates a Lakebase role for the SP (identity_type=SERVICE_PRINCIPAL, OAuth v1)
  4. Connects to Lakebase as superuser and grants the SP role:
       - USAGE + SELECT on tpch_sync_as (synced read-only tables)
       - USAGE + ALL on tpch (staging table CRUD)
  5. Sets DEFAULT PRIVILEGES so future tables are automatically accessible

After this script, the app's FastAPI backend can call:
    w = WorkspaceClient()   # uses injected DATABRICKS_CLIENT_ID/SECRET
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
  and connect to Lakebase as the SP — no OBO needed.
"""

import base64
import json
import sys
from pathlib import Path

import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleIdentityType, RoleAuthMethod, RoleAttributes,
)

APP_NAME = "integration-demo-as"

config_path = Path(__file__).parent / "autoscaling_config.json"
if not config_path.exists():
    sys.exit("autoscaling_config.json not found. Run 02_setup_lakebase_autoscaling.py first.")

with open(config_path) as f:
    cfg = json.load(f)

ENDPOINT_PATH = cfg["endpoint_path"]
PG_HOST       = cfg["pg_host"]
DATABASE_ID   = cfg["database_id"]

w = WorkspaceClient(profile="DEFAULT")

# ── Step 1: Get app SP identity ───────────────────────────────────────────────
print(f"[1/3] Looking up service principal for app: {APP_NAME}")
app_info = w.apps.get(name=APP_NAME)
sp_client_id = app_info.service_principal_client_id
sp_name      = app_info.service_principal_name
print(f"  SP name:      {sp_name}")
print(f"  SP client ID: {sp_client_id}  (used as Postgres role name)")

# ── Step 2: Create Lakebase role for the SP ───────────────────────────────────
print(f"\n[2/3] Creating Lakebase role for SP...")
PARENT  = f"projects/{cfg['project_id']}/branches/{cfg['branch_id']}"
ROLE_ID = "app-sp"

try:
    w.postgres.create_role(
        parent=PARENT,
        role=Role(spec=RoleRoleSpec(
            identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
            auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
            postgres_role=sp_client_id,
            attributes=RoleAttributes(createdb=False, createrole=False),
        )),
        role_id=ROLE_ID,
    )
    print(f"  Created role '{ROLE_ID}' → postgres_role='{sp_client_id}'")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Role '{ROLE_ID}' already exists, skipping creation.")
    else:
        raise

# ── Step 3: Grant permissions in Postgres ────────────────────────────────────
print(f"\n[3/3] Granting Lakebase permissions to SP role...")

cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
# Decode JWT sub claim to get the superuser's Postgres identity
payload = cred.token.split(".")[1]
payload += "=" * (4 - len(payload) % 4)
pg_superuser = json.loads(base64.b64decode(payload)).get("sub", "databricks")
print(f"  Connecting as superuser: {pg_superuser}")

conn = psycopg2.connect(
    host=PG_HOST, port=5432, dbname=DATABASE_ID,
    user=pg_superuser, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

SP_ROLE = sp_client_id  # Postgres role name = SP OAuth client UUID

# tpch schema: full CRUD (staging table for writes + forward ETL)
cur.execute(f'GRANT USAGE ON SCHEMA tpch TO "{SP_ROLE}"')
cur.execute(f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tpch TO "{SP_ROLE}"')
cur.execute(f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tpch TO "{SP_ROLE}"')
print(f"  Granted USAGE + ALL on tpch schema")

# tpch_sync_as schema: read-only (synced Delta tables)
cur.execute(f'GRANT USAGE ON SCHEMA tpch_sync_as TO "{SP_ROLE}"')
cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA tpch_sync_as TO "{SP_ROLE}"')
print(f"  Granted USAGE + SELECT on tpch_sync_as schema")

# Default privileges: auto-grant on future tables
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA tpch GRANT ALL ON TABLES TO "{SP_ROLE}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA tpch GRANT ALL ON SEQUENCES TO "{SP_ROLE}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA tpch_sync_as GRANT SELECT ON TABLES TO "{SP_ROLE}"')
print(f"  Set DEFAULT PRIVILEGES for future tables in both schemas")

cur.close()
conn.close()

print(f"""
✓ App SP Lakebase access configured!
  SP client ID : {sp_client_id}
  Postgres role: {SP_ROLE}
  Schemas      : tpch (ALL), tpch_sync_as (SELECT)

The FastAPI backend now connects via SP auth:
  w = WorkspaceClient()   # DATABRICKS_CLIENT_ID/SECRET injected by Apps runtime
  cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
  # pg_user = JWT sub claim = '{sp_client_id}'

Next step: python3 app/deploy_app.py
""")
