"""
Script 5 (Autoscaling): Forward ETL job
Reads tpch.orders_staging from Lakebase Autoscaling and MERGEs into Delta.
Uses w.postgres.generate_database_credential(endpoint=...) for auth.

Scheduled every 15 minutes.
"""

import base64
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, CronSchedule, PauseStatus
from databricks.sdk.service.compute import ClusterSpec
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient(profile="DEFAULT")
NOTEBOOK_PATH = "/Workspace/Shared/integration_demo/forward_etl_autoscaling"

config_path = Path(__file__).parent / "autoscaling_config.json"
if not config_path.exists():
    raise FileNotFoundError("autoscaling_config.json not found. Run 02_setup_lakebase_autoscaling.py first.")

with open(config_path) as f:
    cfg = json.load(f)

pg_host       = cfg["pg_host"]
database_id   = cfg["database_id"]
endpoint_path = cfg["endpoint_path"]

# ── Notebook source ───────────────────────────────────────────────────────────
notebook_source = f'''# Databricks notebook source
# MAGIC %pip install psycopg2-binary databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import psycopg2, psycopg2.extras
from databricks.sdk import WorkspaceClient

PG_HOST       = "{pg_host}"
PG_DB         = "{database_id}"
ENDPOINT_PATH = "{endpoint_path}"

w = WorkspaceClient()
cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
pg_user = w.current_user.me().user_name

conn = psycopg2.connect(
    host=PG_HOST, port=5432, dbname=PG_DB,
    user=pg_user, password=cred.token, sslmode="require"
)
conn.autocommit = True

# COMMAND ----------
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    cur.execute("SELECT * FROM tpch.orders_staging")
    rows = cur.fetchall()
conn.close()
print(f"Fetched {{len(rows)}} rows from Lakebase Autoscaling orders_staging")

# COMMAND ----------
if len(rows) == 0:
    print("No rows to merge, exiting.")
    dbutils.notebook.exit("no_rows")

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DecimalType, DateType, IntegerType,
)

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("o_orderkey",      LongType(),        False),
    StructField("o_custkey",       LongType(),        True),
    StructField("o_orderstatus",   StringType(),      True),
    StructField("o_totalprice",    DecimalType(15,2), True),
    StructField("o_orderdate",     DateType(),        True),
    StructField("o_orderpriority", StringType(),      True),
    StructField("o_clerk",         StringType(),      True),
    StructField("o_shippriority",  IntegerType(),     True),
    StructField("o_comment",       StringType(),      True),
])

rdd_data = [
    (int(r["o_orderkey"]), int(r["o_custkey"]) if r["o_custkey"] else None,
     str(r["o_orderstatus"]) if r["o_orderstatus"] else None,
     r["o_totalprice"], r["o_orderdate"], r["o_orderpriority"],
     r["o_clerk"], int(r["o_shippriority"]) if r["o_shippriority"] is not None else None,
     r["o_comment"])
    for r in rows
]

df = spark.createDataFrame(rdd_data, schema=schema)
df.createOrReplaceTempView("orders_staging_view")
print(f"Spark view created with {{df.count()}} rows")

# COMMAND ----------
spark.sql("""
MERGE INTO integration_demo.tpch.orders AS target
USING orders_staging_view AS source
ON target.o_orderkey = source.o_orderkey
WHEN MATCHED THEN UPDATE SET
    target.o_custkey       = source.o_custkey,
    target.o_orderstatus   = source.o_orderstatus,
    target.o_totalprice    = source.o_totalprice,
    target.o_orderdate     = source.o_orderdate,
    target.o_orderpriority = source.o_orderpriority,
    target.o_clerk         = source.o_clerk,
    target.o_shippriority  = source.o_shippriority,
    target.o_comment       = source.o_comment
WHEN NOT MATCHED THEN INSERT (
    o_orderkey, o_custkey, o_orderstatus, o_totalprice,
    o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
) VALUES (
    source.o_orderkey, source.o_custkey, source.o_orderstatus, source.o_totalprice,
    source.o_orderdate, source.o_orderpriority, source.o_clerk, source.o_shippriority, source.o_comment
)
""").show()
print("MERGE complete — Lakebase Autoscaling → Delta!")
'''

# ── Upload notebook ───────────────────────────────────────────────────────────
print(f"Uploading notebook: {NOTEBOOK_PATH}")
try:
    w.workspace.mkdirs(path="/Workspace/Shared/integration_demo")
except Exception:
    pass

w.workspace.import_(
    path=NOTEBOOK_PATH,
    content=base64.b64encode(notebook_source.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)
print("  Uploaded.")

# ── Pick node type + Spark version ────────────────────────────────────────────
node_type_id = "i3.xlarge"
try:
    for nt in (w.clusters.list_node_types().node_types or []):
        if nt.node_type_id in ("Standard_DS3_v2", "i3.xlarge", "m5.xlarge"):
            node_type_id = nt.node_type_id
            break
except Exception:
    pass

spark_version = "15.4.x-scala2.12"
try:
    lts = [v for v in (w.clusters.spark_versions().versions or [])
           if "LTS" in (v.name or "") and "ML" not in (v.name or "")]
    if lts:
        spark_version = lts[0].key
except Exception:
    pass

print(f"Using: {node_type_id}  /  Spark {spark_version}")

# ── Create job ────────────────────────────────────────────────────────────────
JOB_NAME = "integration_demo_forward_etl_autoscaling"
for job in w.jobs.list(name=JOB_NAME):
    print(f"  Deleting existing job: {job.job_id}")
    w.jobs.delete(job_id=job.job_id)

job = w.jobs.create(
    name=JOB_NAME,
    tasks=[Task(
        task_key="merge_to_delta_as",
        notebook_task=NotebookTask(notebook_path=NOTEBOOK_PATH),
        new_cluster=ClusterSpec(
            spark_version=spark_version,
            node_type_id=node_type_id,
            num_workers=1,
        ),
    )],
    schedule=CronSchedule(
        quartz_cron_expression="0 0/15 * * * ?",
        timezone_id="UTC",
        pause_status=PauseStatus.UNPAUSED,
    ),
)

print(f"\n✓ Job created: {JOB_NAME}  (ID: {job.job_id})")
print(f"  Schedule: every 15 minutes (UTC)")
print(f"\nManual trigger:")
print(f"  python3 -c \"from databricks.sdk import WorkspaceClient; w = WorkspaceClient(profile='DEFAULT'); r = w.jobs.run_now(job_id={job.job_id}).wait(); print(r.state.result_state)\"")
