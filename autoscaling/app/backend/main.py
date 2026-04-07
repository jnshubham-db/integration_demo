"""
FastAPI backend — Autoscaling Lakebase variant with SP authentication.

Authentication:
  SP (Service Principal): Databricks Apps injects DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET.
  WorkspaceClient() picks these up automatically → generate_database_credential() for Lakebase.
  The app SP (role 'app-sp') has been granted USAGE + SELECT on tpch_sync_as and ALL on tpch.
  Falls back gracefully in local dev (uses DEFAULT profile).

Architecture:
  READ  orders list/detail → tpch_sync_as.orders   (synced read-only, Delta snapshot)
  WRITE create/update/delete → tpch.orders_staging  (CDC log — append-only, forward ETL source)
  READ  customers           → tpch_sync_as.customer (synced read-only)

CDC Pattern:
  orders_staging is a CDC log table. Each operation (INSERT/UPDATE/DELETE) appends a new row
  with _operation and _timestamp columns. The Forward ETL job deduplicates by o_orderkey
  (latest cdc_id wins), applies the three-way MERGE (insert/update/delete) to Delta,
  then cleans up processed records.
"""

import base64
import json
import os
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="LakeSync Autoscaling")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

SYNCED_SCHEMA  = "tpch_sync_as"
STAGING_SCHEMA = "tpch"


def _pg_user_from_token(lakebase_token: str) -> str:
    """Extract the user identity (sub claim) from the Lakebase JWT token."""
    try:
        payload = lakebase_token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        return json.loads(base64.b64decode(payload)).get("sub", "databricks")
    except Exception:
        return "databricks"


def get_conn(request: Request = None):
    """
    Get a Lakebase connection using SP auth.
    In Databricks Apps, DATABRICKS_CLIENT_ID/SECRET are injected automatically.
    WorkspaceClient() picks these up; generate_database_credential() returns a JWT
    whose 'sub' claim is the SP's client UUID — used as the Postgres username.
    """
    w = WorkspaceClient()
    cred = w.postgres.generate_database_credential(
        endpoint=os.environ["LAKEBASE_ENDPOINT"]
    )
    pg_user = _pg_user_from_token(cred.token)

    return psycopg2.connect(
        host=os.environ["LAKEBASE_HOST"],
        port=5432,
        dbname=os.environ["LAKEBASE_DB"],
        user=pg_user,
        password=cred.token,
        sslmode="require",
    )


# ── Pydantic models ───────────────────────────────────────────────────────────

class OrderCreate(BaseModel):
    o_orderkey: int
    o_custkey: int
    o_orderstatus: str
    o_totalprice: float
    o_orderdate: date
    o_orderpriority: str
    o_clerk: str
    o_shippriority: int
    o_comment: str


class OrderUpdate(BaseModel):
    o_custkey: Optional[int] = None
    o_orderstatus: Optional[str] = None
    o_totalprice: Optional[float] = None
    o_orderdate: Optional[date] = None
    o_orderpriority: Optional[str] = None
    o_clerk: Optional[str] = None
    o_shippriority: Optional[int] = None
    o_comment: Optional[str] = None


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "backend": "autoscaling",
        "auth": "sp",
        "env_vars_set": {
            "LAKEBASE_HOST": bool(os.environ.get("LAKEBASE_HOST")),
            "LAKEBASE_DB": bool(os.environ.get("LAKEBASE_DB")),
            "LAKEBASE_ENDPOINT": bool(os.environ.get("LAKEBASE_ENDPOINT")),
        },
    }


# ── Orders READ — from synced table (Delta snapshot) ─────────────────────────

@app.get("/api/orders")
def list_orders(request: Request, search: str = "", limit: int = 50, offset: int = 0):
    """List orders from synced table (read-only Delta snapshot)."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if search:
                cur.execute(
                    f"SELECT * FROM {SYNCED_SCHEMA}.orders WHERE o_comment ILIKE %s ORDER BY o_orderkey LIMIT %s OFFSET %s",
                    (f"%{search}%", limit, offset),
                )
            else:
                cur.execute(
                    f"SELECT * FROM {SYNCED_SCHEMA}.orders ORDER BY o_orderkey LIMIT %s OFFSET %s",
                    (limit, offset),
                )
            return [dict(r) for r in cur.fetchall()]
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName):
        return []
    finally:
        conn.close()


@app.get("/api/orders/count")
def count_orders(request: Request, search: str = ""):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if search:
                cur.execute(
                    f"SELECT COUNT(*) FROM {SYNCED_SCHEMA}.orders WHERE o_comment ILIKE %s",
                    (f"%{search}%",),
                )
            else:
                cur.execute(f"SELECT COUNT(*) FROM {SYNCED_SCHEMA}.orders")
            return {"count": cur.fetchone()[0]}
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName):
        return {"count": 0}
    finally:
        conn.close()


@app.get("/api/orders/{order_key}")
def get_order(request: Request, order_key: int):
    """Get a single order from the synced table."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM {SYNCED_SCHEMA}.orders WHERE o_orderkey = %s",
                (order_key,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Order not found")
            return {**dict(row), "_source": "synced"}
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName):
        raise HTTPException(status_code=404, detail="Synced table not available")
    finally:
        conn.close()


# ── Orders WRITE — CDC log to staging table ──────────────────────────────────

@app.post("/api/orders", status_code=201)
def create_order(request: Request, order: OrderCreate):
    """Append an INSERT CDC record to the staging log."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""INSERT INTO {STAGING_SCHEMA}.orders_staging
                   (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                    o_orderpriority, o_clerk, o_shippriority, o_comment, _operation)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'INSERT') RETURNING *""",
                (order.o_orderkey, order.o_custkey, order.o_orderstatus,
                 order.o_totalprice, order.o_orderdate, order.o_orderpriority,
                 order.o_clerk, order.o_shippriority, order.o_comment),
            )
            conn.commit()
            return dict(cur.fetchone())
    finally:
        conn.close()


@app.put("/api/orders/{order_key}")
def update_order(request: Request, order_key: int, order: OrderUpdate):
    """Append an UPDATE CDC record to the staging log."""
    updates = {k: v for k, v in order.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Read current state from synced table
            try:
                cur.execute(
                    f"SELECT * FROM {SYNCED_SCHEMA}.orders WHERE o_orderkey = %s",
                    (order_key,),
                )
                source = cur.fetchone()
            except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName):
                source = None

            if not source:
                raise HTTPException(status_code=404, detail="Order not found")

            row = dict(source)
            row.update(updates)

            # Append CDC UPDATE record
            cur.execute(
                f"""INSERT INTO {STAGING_SCHEMA}.orders_staging
                    (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                     o_orderpriority, o_clerk, o_shippriority, o_comment, _operation)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'UPDATE') RETURNING *""",
                (row["o_orderkey"], row["o_custkey"], row["o_orderstatus"],
                 row["o_totalprice"], row["o_orderdate"], row["o_orderpriority"],
                 row["o_clerk"], row["o_shippriority"], row["o_comment"]),
            )
            conn.commit()
            return dict(cur.fetchone())
    finally:
        conn.close()


@app.delete("/api/orders/{order_key}", status_code=204)
def delete_order(request: Request, order_key: int):
    """Append a DELETE CDC record to the staging log."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {STAGING_SCHEMA}.orders_staging
                    (o_orderkey, _operation)
                    VALUES (%s, 'DELETE')""",
                (order_key,),
            )
            conn.commit()
    finally:
        conn.close()


# ── Customers READ — from synced table ────────────────────────────────────────

@app.get("/api/customers")
def list_customers(request: Request, limit: int = 100, offset: int = 0):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT c_custkey, c_name, c_nationkey, c_acctbal, c_mktsegment FROM {SYNCED_SCHEMA}.customer ORDER BY c_custkey LIMIT %s OFFSET %s",
                (limit, offset),
            )
            return [dict(r) for r in cur.fetchall()]
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName):
        return []
    finally:
        conn.close()


# ── Genie ─────────────────────────────────────────────────────────────────────

class GenieRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None

@app.post("/api/genie/message")
def genie_message(req: GenieRequest):
    space_id = os.environ.get("GENIE_SPACE_ID", "")
    if not space_id:
        raise HTTPException(status_code=503, detail="GENIE_SPACE_ID not configured")
    w = WorkspaceClient()
    try:
        if req.conversation_id:
            msg = w.genie.create_message_and_wait(space_id, req.conversation_id, req.question)
        else:
            msg = w.genie.start_conversation_and_wait(space_id, req.question)
        text = ""
        if msg.attachments:
            for att in msg.attachments:
                if att.text and att.text.content:
                    text += att.text.content + "\n"
                elif att.query and att.query.description:
                    text += att.query.description + "\n"
        if not text and msg.content:
            text = msg.content
        return {
            "answer": text.strip() or "No response",
            "conversation_id": msg.conversation_id,
            "message_id": msg.id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Serve React static build ──────────────────────────────────────────────────
import os as _os
_static = _os.path.join(_os.path.dirname(__file__), "..", "frontend", "build")
if _os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")
