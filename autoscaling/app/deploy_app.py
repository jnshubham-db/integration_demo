"""
deploy_app.py (Autoscaling): Build React + deploy to Databricks Apps.
App name: integration-demo-as  (separate from classic integration-demo)
Uses AppResourcePostgres (w.postgres backend) instead of AppResourceDatabase.
"""

import base64
import json
import subprocess
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App, AppDeployment, AppDeploymentMode,
    ComputeState, EnvVar,
)
from databricks.sdk.service.workspace import ImportFormat

APP_NAME = "integration-demo-as"
HERE = Path(__file__).parent

config_path = HERE.parent / "autoscaling_config.json"
if not config_path.exists():
    sys.exit("autoscaling_config.json not found. Run 02_setup_lakebase_autoscaling.py first.")

with open(config_path) as f:
    cfg = json.load(f)

pg_host       = cfg["pg_host"]
database_id   = cfg["database_id"]
endpoint_path = cfg["endpoint_path"]
project_id    = cfg["project_id"]
branch_id     = cfg["branch_id"]

w = WorkspaceClient(profile="DEFAULT")
me = w.current_user.me()
WORKSPACE_APP_PATH = f"/Workspace/Users/{me.user_name}/apps/{APP_NAME}"

print(f"Deploying as: {me.user_name}")
print(f"Workspace path: {WORKSPACE_APP_PATH}")
print(f"App name: {APP_NAME}")

# ── Step 1: Build React ───────────────────────────────────────────────────────
print("\n[1/4] Building React frontend...")
result = subprocess.run(["npm", "run", "build"], cwd=HERE / "frontend",
                        capture_output=True, text=True)
if result.returncode != 0:
    print("npm build FAILED:"); print(result.stderr); sys.exit(1)
print("  Build succeeded.")

# ── Step 2: Upload ────────────────────────────────────────────────────────────
print("\n[2/4] Uploading files to Workspace...")

def upload_file(local_path: Path, workspace_path: str):
    encoded = base64.b64encode(local_path.read_bytes()).decode()
    w.workspace.import_(path=workspace_path, content=encoded,
                        format=ImportFormat.AUTO, overwrite=True)

def upload_directory(local_dir: Path, workspace_dir: str):
    for item in sorted(local_dir.rglob("*")):
        if item.is_file():
            parts = item.parts
            if "node_modules" in parts or ".git" in parts:
                continue
            rel = item.relative_to(local_dir)
            ws_path = f"{workspace_dir}/{rel}".replace("\\", "/")
            print(f"  Uploading: {rel}")
            parent = "/".join(ws_path.split("/")[:-1])
            try: w.workspace.mkdirs(path=parent)
            except Exception: pass
            upload_file(item, ws_path)

try: w.workspace.mkdirs(path=WORKSPACE_APP_PATH)
except Exception: pass

upload_directory(HERE / "backend", f"{WORKSPACE_APP_PATH}/backend")

build_dir = HERE / "frontend" / "build"
if build_dir.exists():
    upload_directory(build_dir, f"{WORKSPACE_APP_PATH}/frontend/build")

for fname in ("app.yaml", "requirements.txt"):
    fpath = HERE / fname
    if fpath.exists():
        print(f"  Uploading: {fname}")
        upload_file(fpath, f"{WORKSPACE_APP_PATH}/{fname}")

print("  Upload complete.")

# ── Step 3: Create app ────────────────────────────────────────────────────────
print("\n[3/4] Creating Databricks App...")

try:
    w.apps.create(app=App(
        name=APP_NAME,
        description="TPC-H Orders Demo — Autoscaling Lakebase",
    ))
    print(f"  App '{APP_NAME}' created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  App '{APP_NAME}' already exists, will redeploy.")
    else:
        raise

# Wait for ACTIVE compute
print("  Waiting for compute to be ACTIVE...")
for _ in range(60):
    app_state = w.apps.get(name=APP_NAME)
    state = app_state.compute_status.state if app_state.compute_status else None
    print(f"  State: {state}")
    if state == ComputeState.ACTIVE:
        break
    if state in (ComputeState.ERROR, ComputeState.STOPPED):
        try: w.apps.start(name=APP_NAME).wait()
        except Exception: pass
        break
    time.sleep(10)

# ── Step 4: Deploy ────────────────────────────────────────────────────────────
print("\n[4/4] Deploying app (SNAPSHOT mode)...")

deployment = w.apps.deploy(
    app_name=APP_NAME,
    app_deployment=AppDeployment(
        mode=AppDeploymentMode.SNAPSHOT,
        source_code_path=WORKSPACE_APP_PATH,
        env_vars=[
            EnvVar(name="LAKEBASE_HOST",     value=pg_host),
            EnvVar(name="LAKEBASE_DB",       value=database_id),
            EnvVar(name="LAKEBASE_ENDPOINT", value=endpoint_path),
        ],
    ),
).result()

print(f"\n✓ Deployment complete!")
print(f"  Deployment ID: {deployment.deployment_id}")
app_info = w.apps.get(name=APP_NAME)
print(f"  App URL: {app_info.url}")
