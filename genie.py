"""
genie.py — Create an AI/BI Genie space for TPC-H data in integration_demo.tpch

What this does:
  1. Lists available SQL warehouses and prompts you to choose one
  2. Builds a serialized_space JSON covering all 8 TPC-H tables
  3. Creates (or updates existing) Genie space titled "LakeSync TPC-H Analytics"
  4. Saves the space_id to genie_config.json so the app can use it

Usage:
  python3 genie.py
"""

import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

SPACE_TITLE = "LakeSync TPC-H Analytics"
SPACE_DESC  = (
    "Natural-language analytics over the TPC-H benchmark dataset stored in "
    "integration_demo.tpch. Ask questions about orders, customers, revenue, "
    "suppliers, and shipping performance."
)

TPCH_TABLES = [
    "integration_demo.tpch.customer",
    "integration_demo.tpch.lineitem",
    "integration_demo.tpch.nation",
    "integration_demo.tpch.orders",
    "integration_demo.tpch.part",
    "integration_demo.tpch.partsupp",
    "integration_demo.tpch.region",
    "integration_demo.tpch.supplier",
]  # must be sorted alphabetically

w = WorkspaceClient(profile="DEFAULT")

# ── Step 1: list warehouses and prompt user ───────────────────────────────────
DEFAULT_WAREHOUSE_ID = "148ccb90800933a1"  # Shared Endpoint

print("[1/3] Available SQL warehouses:")
warehouses = list(w.warehouses.list())
for i, wh in enumerate(warehouses):
    state = wh.state.value if wh.state else "unknown"
    marker = " (default)" if wh.id == DEFAULT_WAREHOUSE_ID else ""
    print(f"  [{i}] {wh.name:<40} id={wh.id}  state={state}{marker}")

print()
choice = input(f"Enter warehouse number or ID [press Enter for default {DEFAULT_WAREHOUSE_ID}]: ").strip()

if not choice:
    warehouse_id = DEFAULT_WAREHOUSE_ID
    warehouse_name = next((wh.name for wh in warehouses if wh.id == DEFAULT_WAREHOUSE_ID), DEFAULT_WAREHOUSE_ID)
elif choice.isdigit() and int(choice) < len(warehouses):
    warehouse_id = warehouses[int(choice)].id
    warehouse_name = warehouses[int(choice)].name
else:
    warehouse_id = choice
    warehouse_name = choice

print(f"  Using warehouse: {warehouse_name} ({warehouse_id})")

# ── Step 2: check if space already exists ────────────────────────────────────
print(f"\n[2/3] Checking for existing Genie space: '{SPACE_TITLE}'...")
existing_space_id = None
spaces_resp = w.genie.list_spaces()
for space in (spaces_resp.spaces or []):
    if space.title == SPACE_TITLE:
        existing_space_id = space.space_id
        print(f"  Found existing space: {existing_space_id}")
        break

# ── Step 3: create or update space ───────────────────────────────────────────
serialized = json.dumps({
    "version": 2,
    "data_sources": {
        "tables": [{"identifier": t} for t in TPCH_TABLES]
    }
})

print(f"\n[3/3] {'Updating' if existing_space_id else 'Creating'} Genie space...")
if existing_space_id:
    w.genie.update_space(
        space_id=existing_space_id,
        title=SPACE_TITLE,
        description=SPACE_DESC,
        warehouse_id=warehouse_id,
        serialized_space=serialized,
    )
    space_id = existing_space_id
    print(f"  Updated space: {space_id}")
else:
    space = w.genie.create_space(
        warehouse_id=warehouse_id,
        serialized_space=serialized,
        title=SPACE_TITLE,
        description=SPACE_DESC,
    )
    space_id = space.space_id
    print(f"  Created space: {space_id}")

# ── Save config ───────────────────────────────────────────────────────────────
config = {"genie_space_id": space_id, "genie_title": SPACE_TITLE}
config_path = Path(__file__).parent / "genie_config.json"
with open(config_path, "w") as f:
    json.dump(config, f, indent=2)

print(f"""
✓ Genie space ready!
  Space ID : {space_id}
  Title    : {SPACE_TITLE}
  Tables   : {len(TPCH_TABLES)} TPC-H tables
  Config   : {config_path}

Open in browser:
  https://adb-984752964297111.11.azuredatabricks.net/genie/spaces/{space_id}

Next: update GENIE_SPACE_ID in deploy_app.py then run python3 app/deploy_app.py
""")
