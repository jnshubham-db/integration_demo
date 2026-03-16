"""
Script 1: Setup catalog and copy TPC-H data
Creates integration_demo catalog + tpch schema, copies 8 TPC-H tables from samples.tpch.
"""

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient(profile="DEFAULT")
warehouse_id = next(w.warehouses.list()).id
print(f"Using warehouse: {warehouse_id}")


def run_sql(sql, poll_on_running=True):
    resp = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    # Poll if RUNNING (large tables like lineitem may exceed 50s)
    while poll_on_running and resp.status.state == StatementState.RUNNING:
        print(f"  Still running... polling in 5s")
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state == StatementState.FAILED:
        raise RuntimeError(resp.status.error.message)
    return resp


# --- Create catalog (idempotent) ---
print("Creating catalog: integration_demo")
try:
    w.catalogs.create(name="integration_demo")
    print("  Created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print("  Already exists, skipping.")
    else:
        raise

# --- Create schema (idempotent) ---
print("Creating schema: integration_demo.tpch")
try:
    w.schemas.create(name="tpch", catalog_name="integration_demo")
    print("  Created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print("  Already exists, skipping.")
    else:
        raise

# --- Deep clone 8 TPC-H tables ---
TABLES = ["lineitem", "orders", "customer", "part", "supplier", "partsupp", "nation", "region"]

for t in TABLES:
    print(f"Cloning samples.tpch.{t} → integration_demo.tpch.{t} ...")
    run_sql(f"CREATE TABLE IF NOT EXISTS integration_demo.tpch.{t} DEEP CLONE samples.tpch.{t}")
    print(f"  Done.")

print("\n✓ Setup complete: integration_demo.tpch has 8 TPC-H tables.")
