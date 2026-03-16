"""
Script 4: Forward ETL job
Creates a scheduled Databricks Job that reads tpch.orders_staging from Lakebase
and MERGEs back into integration_demo.tpch.orders Delta table.

The job runs every 15 minutes.
"""

import base64
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    CronSchedule,
    PauseStatus,
)
from databricks.sdk.service.compute import ClusterSpec

# --- Config -------------------------------------------------------------------
w = WorkspaceClient(profile="DEFAULT")
NOTEBOOK_PATH = "/Workspace/Shared/integration_demo/forward_etl"

# Load lakebase config
config_path = Path(__file__).parent / "lakebase_config.json"
if not config_path.exists():
    raise FileNotFoundError("lakebase_config.json not found. Run 02_setup_lakebase.py first.")

with open(config_path) as f:
    cfg = json.load(f)

pg_host       = cfg["pg_host"]
database_id   = cfg["database_id"]
instance_name = cfg["instance_name"]

# --- Upload notebook to workspace ---------------------------------------------
print(f"Uploading forward ETL notebook to: {NOTEBOOK_PATH}")

notebook_source = f'''# Databricks notebook source
# MAGIC %pip install psycopg2-binary databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import os
import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient

PG_HOST       = "{pg_host}"
PG_DB         = "{database_id}"
INSTANCE_NAME = "{instance_name}"

w = WorkspaceClient()
cred = w.database.generate_database_credential(instance_names=[INSTANCE_NAME])
pg_user = w.current_user.me().user_name

conn = psycopg2.connect(
    host=PG_HOST, port=5432, dbname=PG_DB,
    user=pg_user, password=cred.token, sslmode="require"
)
conn.autocommit = True

# COMMAND ----------
# Fetch all rows from orders_staging
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("SELECT * FROM tpch.orders_staging")
    rows = cur.fetchall()

conn.close()
print(f"Fetched {{len(rows)}} rows from orders_staging")

# COMMAND ----------
if len(rows) == 0:
    print("No rows to merge, exiting.")
    dbutils.notebook.exit("no_rows")

# COMMAND ----------
# Convert to Spark DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DecimalType, DateType, IntegerType, TimestampType
)
from datetime import date, datetime
from decimal import Decimal

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("o_orderkey",     LongType(),        False),
    StructField("o_custkey",      LongType(),        True),
    StructField("o_orderstatus",  StringType(),      True),
    StructField("o_totalprice",   DecimalType(15,2), True),
    StructField("o_orderdate",    DateType(),        True),
    StructField("o_orderpriority",StringType(),      True),
    StructField("o_clerk",        StringType(),      True),
    StructField("o_shippriority", IntegerType(),     True),
    StructField("o_comment",      StringType(),      True),
])

# Convert rows to plain python types
def to_row(r):
    return (
        int(r["o_orderkey"]),
        int(r["o_custkey"]) if r["o_custkey"] is not None else None,
        str(r["o_orderstatus"]) if r["o_orderstatus"] else None,
        r["o_totalprice"],
        r["o_orderdate"],
        r["o_orderpriority"],
        r["o_clerk"],
        int(r["o_shippriority"]) if r["o_shippriority"] is not None else None,
        r["o_comment"],
    )

rdd_data = [to_row(r) for r in rows]
df = spark.createDataFrame(rdd_data, schema=schema)
df.createOrReplaceTempView("orders_staging_view")
print(f"Created temp view with {{df.count()}} rows")

# COMMAND ----------
# MERGE into Delta table
merge_sql = """
MERGE INTO integration_demo.tpch.orders AS target
USING orders_staging_view AS source
ON target.o_orderkey = source.o_orderkey
WHEN MATCHED THEN UPDATE SET
    target.o_custkey      = source.o_custkey,
    target.o_orderstatus  = source.o_orderstatus,
    target.o_totalprice   = source.o_totalprice,
    target.o_orderdate    = source.o_orderdate,
    target.o_orderpriority = source.o_orderpriority,
    target.o_clerk        = source.o_clerk,
    target.o_shippriority = source.o_shippriority,
    target.o_comment      = source.o_comment
WHEN NOT MATCHED THEN INSERT (
    o_orderkey, o_custkey, o_orderstatus, o_totalprice,
    o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
) VALUES (
    source.o_orderkey, source.o_custkey, source.o_orderstatus, source.o_totalprice,
    source.o_orderdate, source.o_orderpriority, source.o_clerk, source.o_shippriority, source.o_comment
)
"""

result = spark.sql(merge_sql)
result.show()
print("MERGE complete!")
'''

# Encode notebook as base64 (Python source format)
encoded = base64.b64encode(notebook_source.encode("utf-8")).decode("utf-8")

# Ensure parent directory exists
try:
    w.workspace.mkdirs(path="/Workspace/Shared/integration_demo")
except Exception:
    pass

from databricks.sdk.service.workspace import ImportFormat, Language

w.workspace.import_(
    path=NOTEBOOK_PATH,
    content=encoded,
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)
print(f"  Notebook uploaded to {NOTEBOOK_PATH}")

# --- Determine cluster node type (use smallest available) ---------------------
# Use first available node type that matches a reasonable size
node_type_id = "i3.xlarge"
try:
    node_types = w.clusters.list_node_types()
    # Prefer Standard_DS3_v2 on Azure or similar; fallback to i3.xlarge
    for nt in node_types.node_types or []:
        if nt.node_type_id in ("Standard_DS3_v2", "i3.xlarge", "m5.xlarge"):
            node_type_id = nt.node_type_id
            break
except Exception:
    pass

print(f"Using node type: {node_type_id}")

# --- Get latest LTS Spark version ---------------------------------------------
spark_version = "15.4.x-scala2.12"
try:
    versions = w.clusters.spark_versions()
    lts = [v for v in (versions.versions or []) if "LTS" in (v.name or "") and "ML" not in (v.name or "")]
    if lts:
        spark_version = lts[0].key
except Exception:
    pass

print(f"Using Spark version: {spark_version}")

# --- Create job ---------------------------------------------------------------
print("\nCreating forward ETL job...")

JOB_NAME = "integration_demo_forward_etl"

# Delete existing job with same name (idempotent)
for job in w.jobs.list(name=JOB_NAME):
    print(f"  Deleting existing job: {job.job_id}")
    w.jobs.delete(job_id=job.job_id)

job = w.jobs.create(
    name=JOB_NAME,
    tasks=[
        Task(
            task_key="merge_to_delta",
            notebook_task=NotebookTask(notebook_path=NOTEBOOK_PATH),
            new_cluster=ClusterSpec(
                spark_version=spark_version,
                node_type_id=node_type_id,
                num_workers=1,
            ),
        )
    ],
    schedule=CronSchedule(
        quartz_cron_expression="0 0/15 * * * ?",
        timezone_id="UTC",
        pause_status=PauseStatus.UNPAUSED,
    ),
)

print(f"\n✓ Job created!")
print(f"  Job ID:   {job.job_id}")
print(f"  Job name: {JOB_NAME}")
print(f"  Schedule: every 15 minutes (UTC)")
print(f"\nTo trigger manually:")
print(f"  python -c \"")
print(f"    from databricks.sdk import WorkspaceClient")
print(f"    w = WorkspaceClient(profile='DEFAULT')")
print(f"    run = w.jobs.run_now(job_id={job.job_id}).wait()")
print(f"    print('Run state:', run.state.result_state)")
print(f"  \"")
