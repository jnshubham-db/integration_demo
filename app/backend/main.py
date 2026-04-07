"""
FastAPI backend for TPC-H Orders CRUD demo.
- Orders CRUD  → tpch.orders_staging        (writable Lakebase table)
- Customers    → tpch_sync.customer          (synced read-only from Delta)
- Seed         → pulls rows from Delta via SQL warehouse → inserts into staging
"""

import os
import time
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="TPC-H Orders Demo")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    """Fresh Lakebase connection per request (OAuth tokens are short-lived)."""
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(
        instance_names=[os.environ["LAKEBASE_INSTANCE"]]
    )
    pg_user = w.current_user.me().user_name
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


# ── Orders CRUD (tpch.orders_staging) ────────────────────────────────────────

@app.get("/api/orders")
def list_orders(search: str = "", limit: int = 50, offset: int = 0):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if search:
                cur.execute(
                    """SELECT * FROM tpch.orders_staging
                       WHERE o_comment ILIKE %s
                       ORDER BY o_orderkey LIMIT %s OFFSET %s""",
                    (f"%{search}%", limit, offset),
                )
            else:
                cur.execute(
                    """SELECT * FROM tpch.orders_staging
                       ORDER BY o_orderkey LIMIT %s OFFSET %s""",
                    (limit, offset),
                )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@app.get("/api/orders/count")
def count_orders(search: str = ""):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if search:
                cur.execute(
                    "SELECT COUNT(*) FROM tpch.orders_staging WHERE o_comment ILIKE %s",
                    (f"%{search}%",),
                )
            else:
                cur.execute("SELECT COUNT(*) FROM tpch.orders_staging")
            return {"count": cur.fetchone()[0]}
    finally:
        conn.close()


@app.get("/api/orders/{order_key}")
def get_order(order_key: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM tpch.orders_staging WHERE o_orderkey = %s",
                (order_key,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Order not found")
            return dict(row)
    finally:
        conn.close()


@app.post("/api/orders", status_code=201)
def create_order(order: OrderCreate):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """INSERT INTO tpch.orders_staging
                   (o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,
                    o_orderpriority,o_clerk,o_shippriority,o_comment)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (order.o_orderkey, order.o_custkey, order.o_orderstatus,
                 order.o_totalprice, order.o_orderdate, order.o_orderpriority,
                 order.o_clerk, order.o_shippriority, order.o_comment),
            )
            conn.commit()
            return dict(cur.fetchone())
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(status_code=409, detail="Order key already exists")
    finally:
        conn.close()


@app.put("/api/orders/{order_key}")
def update_order(order_key: int, order: OrderUpdate):
    fields = {k: v for k, v in order.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    values = list(fields.values()) + [order_key]
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""UPDATE tpch.orders_staging
                    SET {set_clause}, last_modified = NOW()
                    WHERE o_orderkey = %s RETURNING *""",
                values,
            )
            conn.commit()
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Order not found")
            return dict(row)
    finally:
        conn.close()


@app.delete("/api/orders/{order_key}", status_code=204)
def delete_order(order_key: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tpch.orders_staging WHERE o_orderkey = %s",
                (order_key,),
            )
            conn.commit()
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Order not found")
    finally:
        conn.close()


# ── Seed from Delta (copies rows from integration_demo.tpch.orders) ──────────

@app.post("/api/seed")
def seed_from_delta(limit: int = 100):
    """Pull `limit` rows from Delta and upsert into orders_staging."""
    w = WorkspaceClient()
    warehouse_id = next(w.warehouses.list()).id
    resp = w.statement_execution.execute_statement(
        statement=f"""SELECT o_orderkey, o_custkey, o_orderstatus,
                             CAST(o_totalprice AS STRING), o_orderdate,
                             o_orderpriority, o_clerk, o_shippriority, o_comment
                      FROM integration_demo.tpch.orders
                      LIMIT {limit}""",
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    while resp.status.state == StatementState.RUNNING:
        time.sleep(3)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state == StatementState.FAILED:
        raise HTTPException(status_code=500, detail=resp.status.error.message)

    rows = resp.result.data_array or []
    conn = get_conn()
    inserted = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """INSERT INTO tpch.orders_staging
                       (o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,
                        o_orderpriority,o_clerk,o_shippriority,o_comment)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (o_orderkey) DO NOTHING""",
                    (int(row[0]), int(row[1]), row[2], float(row[3]),
                     row[4], row[5], row[6], int(row[7]), row[8]),
                )
                inserted += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return {"seeded": inserted, "attempted": len(rows)}


# ── Customers (read-only synced table) ───────────────────────────────────────

@app.get("/api/customers")
def list_customers(limit: int = 100, offset: int = 0):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT c_custkey, c_name, c_nationkey, c_acctbal, c_mktsegment
                   FROM tpch_sync.customer
                   ORDER BY c_custkey LIMIT %s OFFSET %s""",
                (limit, offset),
            )
            return [dict(r) for r in cur.fetchall()]
    except psycopg2.errors.UndefinedTable:
        return []  # synced table still warming up
    finally:
        conn.close()


@app.get("/api/health")
def health():
    return {"status": "ok"}


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
