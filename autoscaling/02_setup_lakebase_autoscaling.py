"""
Script 2 (Autoscaling): Setup Lakebase Autoscaling
Uses w.postgres SDK for infrastructure + psycopg2 for schema/table creation.

Parts:
  A  – Create autoscaling project, branch, database, endpoint
  B  – Create UC schema for synced table definitions
  C  – Create writable tpch schema + orders_staging + tpch_sync_as schema via psycopg2
  D  – [MANUAL] Print instructions to create synced tables via Databricks UI
  E  – Write autoscaling_config.json

Shared Delta source: integration_demo.tpch.*  (created by ../01_setup_catalog.py)
"""

import json
import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Project, ProjectSpec,
    Branch, BranchSpec,
    Database, DatabaseDatabaseSpec,
    Endpoint, EndpointSpec, EndpointType,
)

w = WorkspaceClient(profile="DEFAULT")

# ── IDs (hyphens for Postgres, underscores for UC) ───────────────────────────
PROJECT_ID    = "integration-demo-as"
BRANCH_ID     = "main"
DATABASE_ID   = "tpch-as"
ENDPOINT_ID   = "primary"
UC_CATALOG    = "integration_demo"    # existing UC catalog (shared with classic)
UC_SCHEMA     = "tpch_sync_as"        # UC schema where synced tables will be created via UI

PARENT        = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}"
ENDPOINT_PATH = f"{PARENT}/endpoints/{ENDPOINT_ID}"

# ============================================================
# Part A: Autoscaling Lakebase project + branch + database + endpoint
# ============================================================
print("=== Part A: Autoscaling project infrastructure ===")

print(f"  Creating project: {PROJECT_ID}")
try:
    project = w.postgres.create_project(
        project=Project(spec=ProjectSpec(display_name="Integration Demo (Autoscaling)")),
        project_id=PROJECT_ID,
    ).wait()
    print(f"    Created: {project.name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print("    Already exists, skipping.")
    else:
        raise

print(f"  Creating branch: {BRANCH_ID}")
try:
    w.postgres.create_branch(
        parent=f"projects/{PROJECT_ID}",
        branch=Branch(spec=BranchSpec(no_expiry=True)),
        branch_id=BRANCH_ID,
    ).wait()
    print("    Created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print("    Already exists, skipping.")
    else:
        raise

# Fetch the superuser role that Lakebase auto-creates on each branch
existing_roles = list(w.postgres.list_roles(parent=PARENT))
superuser_role = existing_roles[0].name if existing_roles else None
print(f"  Superuser role: {superuser_role}")

print(f"  Creating database: {DATABASE_ID}")
try:
    w.postgres.create_database(
        parent=PARENT,
        database=Database(spec=DatabaseDatabaseSpec(
            postgres_database=DATABASE_ID,
            role=superuser_role,
        )),
        database_id=DATABASE_ID,
    ).wait()
    print("    Created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print("    Already exists, skipping.")
    else:
        raise

print(f"  Creating endpoint: {ENDPOINT_ID}")
try:
    endpoint = w.postgres.create_endpoint(
        parent=PARENT,
        endpoint=Endpoint(spec=EndpointSpec(
            endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
            autoscaling_limit_min_cu=0.5,
            autoscaling_limit_max_cu=4.0,
        )),
        endpoint_id=ENDPOINT_ID,
    ).wait()
    pg_host = endpoint.status.hosts.host
    print(f"    Created. Host: {pg_host}")
except Exception as e:
    if "already exists" in str(e).lower():
        print("    Already exists, fetching host...")
        endpoint = w.postgres.get_endpoint(name=ENDPOINT_PATH)
        pg_host = endpoint.status.hosts.host
        print(f"    Host: {pg_host}")
    else:
        raise

# ============================================================
# Part B: Ensure UC schema exists for synced table definitions
# ============================================================
print(f"\n=== Part B: Ensure UC schema {UC_CATALOG}.{UC_SCHEMA} ===")
try:
    w.schemas.create(name=UC_SCHEMA, catalog_name=UC_CATALOG)
    print(f"  Created schema: {UC_CATALOG}.{UC_SCHEMA}")
except Exception as e:
    if "already exists" in str(e).lower():
        print("  Already exists, skipping.")
    else:
        raise

# ============================================================
# Part C: Create Postgres schemas + orders_staging via psycopg2
# ============================================================
print("\n=== Part C: Create Postgres schemas and staging table ===")

cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
me = w.current_user.me()
pg_user = me.user_name

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=DATABASE_ID,
    user=pg_user, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Schema for writable CRUD table
cur.execute("CREATE SCHEMA IF NOT EXISTS tpch")

# Schema for synced tables (will be populated by Lakebase after UI setup)
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {UC_SCHEMA}")

# CDC log table for capturing all CRUD operations (INSERT/UPDATE/DELETE)
cur.execute("DROP TABLE IF EXISTS tpch.orders_staging")
cur.execute("""
    CREATE TABLE tpch.orders_staging (
        cdc_id          BIGSERIAL PRIMARY KEY,
        o_orderkey      BIGINT NOT NULL,
        o_custkey       BIGINT,
        o_orderstatus   CHAR(1),
        o_totalprice    NUMERIC(15,2),
        o_orderdate     DATE,
        o_orderpriority VARCHAR(15),
        o_clerk         VARCHAR(15),
        o_shippriority  INT,
        o_comment       VARCHAR(79),
        _operation      VARCHAR(10) NOT NULL,
        _timestamp      TIMESTAMPTZ DEFAULT NOW()
    )
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_staging_orderkey_cdc ON tpch.orders_staging (o_orderkey, cdc_id DESC)")

# Grant access to all Databricks users connecting via OAuth
cur.execute("GRANT USAGE ON SCHEMA tpch TO PUBLIC")
cur.execute(f"GRANT USAGE ON SCHEMA {UC_SCHEMA} TO PUBLIC")
cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tpch TO PUBLIC")
cur.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tpch TO PUBLIC")

cur.close()
conn.close()
print(f"  Created schemas: tpch, {UC_SCHEMA}")
print("  Created tpch.orders_staging (CDC log table) + granted PUBLIC access.")

# ============================================================
# Part D: Synced Tables — manual step via Databricks UI
# ============================================================
TPCH_PKS = {
    "customer":  ["c_custkey"],
    "orders":    ["o_orderkey"],
    "lineitem":  ["l_orderkey", "l_linenumber"],
    "part":      ["p_partkey"],
    "supplier":  ["s_suppkey"],
    "nation":    ["n_nationkey"],
    "region":    ["r_regionkey"],
    "partsupp":  ["ps_partkey", "ps_suppkey"],
}

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║              MANUAL STEP: Create Synced Tables via UI                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  The Databricks SDK does not yet support creating synced tables for     ║
║  Autoscaling Lakebase projects. Please create them via the UI:          ║
║                                                                          ║
║  1. Go to: Databricks UI → Catalog → Create → Create Synced Table       ║
║  2. Use the following settings for each table:                           ║
║                                                                          ║
║     Target catalog  : integration_demo                                  ║
║     Target schema   : tpch_sync_as                                      ║
║     Lakebase project: integration-demo-as                               ║
║     Lakebase branch : main                                               ║
║     Lakebase DB     : tpch-as                                            ║
║                                                                          ║
║  3. Create the following 8 synced tables:                                ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

print(f"  {'Table':<12}  Source (Delta)                          Primary Key(s)")
print(f"  {'-'*12}  {'-'*39}  {'-'*30}")
for table, pks in TPCH_PKS.items():
    source = f"integration_demo.tpch.{table}"
    pk_str = ", ".join(pks)
    print(f"  {table:<12}  {source:<39}  {pk_str}")

print(f"""
  Pipeline storage catalog : {UC_CATALOG}
  Pipeline storage schema  : staging_as   (will be created automatically)
  Scheduling policy        : CONTINUOUS

  After creating synced tables, the app will automatically read customers
  from {UC_CATALOG}.{UC_SCHEMA}.customer via the Postgres schema {UC_SCHEMA}.
""")

# ============================================================
# Part E: Save config
# ============================================================
config = {
    "pg_host":       pg_host,
    "project_id":    PROJECT_ID,
    "branch_id":     BRANCH_ID,
    "database_id":   DATABASE_ID,
    "endpoint_id":   ENDPOINT_ID,
    "endpoint_path": ENDPOINT_PATH,
    "uc_catalog":    UC_CATALOG,
    "uc_schema":     UC_SCHEMA,
    "backend":       "autoscaling",
}

with open("autoscaling_config.json", "w") as f:
    json.dump(config, f, indent=2)

print(f"✓ Config saved to autoscaling_config.json")
print(f"  pg_host:      {pg_host}")
print(f"  project:      {PROJECT_ID}")
print(f"  UC schema:    {UC_CATALOG}.{UC_SCHEMA}")
print(f"\n✓ Lakebase Autoscaling infrastructure setup complete!")
print(f"  Next steps:")
print(f"    1. Create synced tables via UI (see instructions above)")
print(f"    2. Run: python3 app/deploy_app.py")
print(f"    3. Run: python3 05_forward_etl_autoscaling.py")
