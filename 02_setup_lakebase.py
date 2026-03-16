"""
Script 2: Setup Lakebase (classic w.database API)
- Creates a classic Lakebase instance (integration-demo)
- Registers it as a UC database catalog (integration_demo_lakebase)
- Creates writable tpch.orders_staging table via psycopg2
- Creates 8 synced tables (Delta → Lakebase)
- Writes lakebase_config.json for use by deploy_app.py and 04_forward_etl.py
"""

import json
import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    DatabaseInstance, DatabaseCatalog,
    SyncedDatabaseTable, SyncedTableSpec,
    SyncedTableSchedulingPolicy, NewPipelineSpec,
    DatabaseInstanceState,
)

w = WorkspaceClient(profile="DEFAULT")

INSTANCE_NAME    = "integration-demo"
LAKEBASE_CATALOG = "integration_demo_lakebase"
LAKEBASE_DB      = "tpch_sync"          # Postgres database name

# ============================================================
# Part A: Create classic Lakebase instance
# ============================================================
print("=== Part A: Creating Lakebase instance ===")

instance = None
try:
    instance = w.database.create_database_instance_and_wait(
        database_instance=DatabaseInstance(
            name=INSTANCE_NAME,
            capacity="CU_2",
        )
    )
    print(f"  Created instance: {instance.name}")
    print(f"  Read/write DNS:   {instance.read_write_dns}")
except Exception as e:
    if "already exists" in str(e).lower() or "not unique" in str(e).lower():
        print(f"  Already exists, fetching...")
        instance = w.database.get_database_instance(name=INSTANCE_NAME)
        print(f"  Instance: {instance.name}")
        print(f"  Read/write DNS: {instance.read_write_dns}")
    else:
        raise

# Wait until AVAILABLE
import time
for _ in range(30):
    if instance.state == DatabaseInstanceState.AVAILABLE:
        break
    print(f"  State: {instance.state}, waiting...")
    time.sleep(10)
    instance = w.database.get_database_instance(name=INSTANCE_NAME)

pg_host = instance.read_write_dns
print(f"  Host: {pg_host}")

# ============================================================
# Part B: Register as UC Database Catalog
# ============================================================
print("\n=== Part B: Registering as UC catalog ===")

try:
    w.database.create_database_catalog(
        catalog=DatabaseCatalog(
            name=LAKEBASE_CATALOG,
            database_instance_name=INSTANCE_NAME,
            database_name=LAKEBASE_DB,
            create_database_if_not_exists=True,
        )
    )
    print(f"  Created UC catalog: {LAKEBASE_CATALOG}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  UC catalog {LAKEBASE_CATALOG} already exists, skipping.")
    else:
        raise

# ============================================================
# Part C: Create writable tpch.orders_staging via psycopg2
# ============================================================
print("\n=== Part C: Creating tpch.orders_staging in Lakebase ===")

cred = w.database.generate_database_credential(instance_names=[INSTANCE_NAME])
me = w.current_user.me()
pg_user = me.user_name

conn = psycopg2.connect(
    host=pg_host,
    port=5432,
    dbname=LAKEBASE_DB,
    user=pg_user,
    password=cred.token,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE SCHEMA IF NOT EXISTS tpch")
cur.execute("""
    CREATE TABLE IF NOT EXISTS tpch.orders_staging (
        o_orderkey      BIGINT PRIMARY KEY,
        o_custkey       BIGINT,
        o_orderstatus   CHAR(1),
        o_totalprice    NUMERIC(15,2),
        o_orderdate     DATE,
        o_orderpriority VARCHAR(15),
        o_clerk         VARCHAR(15),
        o_shippriority  INT,
        o_comment       VARCHAR(79),
        last_modified   TIMESTAMPTZ DEFAULT NOW()
    )
""")
print("  Created tpch.orders_staging (writable)")
cur.close()
conn.close()

# ============================================================
# Part D: Synced Tables (Delta → Lakebase)
# ============================================================
print("\n=== Part D: Creating synced tables ===")

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

for table, pks in TPCH_PKS.items():
    print(f"  Creating synced table for: {table}")
    try:
        w.database.create_synced_database_table(
            synced_table=SyncedDatabaseTable(
                name=f"{LAKEBASE_CATALOG}.tpch_sync.{table}",
                spec=SyncedTableSpec(
                    source_table_full_name=f"integration_demo.tpch.{table}",
                    primary_key_columns=pks,
                    scheduling_policy=SyncedTableSchedulingPolicy.CONTINUOUS,
                    new_pipeline_spec=NewPipelineSpec(
                        storage_catalog=LAKEBASE_CATALOG,
                        storage_schema="staging",
                    ),
                ),
            )
        )
        print(f"    Created: {LAKEBASE_CATALOG}.tpch_sync.{table}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"    Already exists, skipping.")
        else:
            print(f"    Warning: {e}")

# ============================================================
# Save config
# ============================================================
config = {
    "pg_host":          pg_host,
    "instance_name":    INSTANCE_NAME,
    "database_id":      LAKEBASE_DB,
    "lakebase_catalog": LAKEBASE_CATALOG,
}

with open("lakebase_config.json", "w") as f:
    json.dump(config, f, indent=2)

print(f"\n✓ Config saved to lakebase_config.json")
print(f"  pg_host:  {pg_host}")
print(f"  instance: {INSTANCE_NAME}")
print(f"  catalog:  {LAKEBASE_CATALOG}")
print("\n✓ Lakebase setup complete!")
print("  Note: Initial synced table backfill may take 5–30 min per table.")
