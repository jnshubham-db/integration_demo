"""
deploy_app.py: Build the React frontend and deploy the full app to Databricks Apps.

Steps:
  1. npm run build (React → frontend/build/)
  2. Upload all files to /Workspace/Users/{user}/apps/integration_demo/
  3. Create (or update) the Databricks App
  4. Deploy with SNAPSHOT mode
  5. Print the app URL
"""

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppDeploymentMode,
    AppResource,
    AppResourceDatabase,
    AppResourceDatabaseDatabasePermission,
    ComputeState,
    EnvVar,
)
from databricks.sdk.service.workspace import ImportFormat

# --- Config -------------------------------------------------------------------
APP_NAME = "integration-demo"
HERE = Path(__file__).parent

# Load lakebase config written by 02_setup_lakebase.py
config_path = HERE.parent / "lakebase_config.json"
if not config_path.exists():
    sys.exit("lakebase_config.json not found. Run 02_setup_lakebase.py first.")

with open(config_path) as f:
    cfg = json.load(f)

pg_host       = cfg["pg_host"]
database_id   = cfg["database_id"]
instance_name = cfg["instance_name"]

w = WorkspaceClient(profile="DEFAULT")
me = w.current_user.me()
user_name = me.user_name
WORKSPACE_APP_PATH = f"/Workspace/Users/{user_name}/apps/{APP_NAME}"

print(f"Deploying as: {user_name}")
print(f"Workspace path: {WORKSPACE_APP_PATH}")

# --- Step 1: Build React frontend ---------------------------------------------
print("\n[1/4] Building React frontend...")
frontend_dir = HERE / "frontend"
result = subprocess.run(
    ["npm", "run", "build"],
    cwd=frontend_dir,
    capture_output=True,
    text=True,
)
if result.returncode != 0:
    print("npm build FAILED:")
    print(result.stderr)
    sys.exit(1)
print("  Build succeeded.")

# --- Step 2: Upload files to Workspace ----------------------------------------
print("\n[2/4] Uploading files to Workspace...")

def upload_file(local_path: Path, workspace_path: str):
    """Upload a single file to Databricks Workspace."""
    content = local_path.read_bytes()
    encoded = base64.b64encode(content).decode("utf-8")
    # Determine format
    if local_path.suffix in (".py", ".txt", ".yaml", ".yml", ".json", ".js", ".jsx", ".html", ".css", ".md"):
        fmt = ImportFormat.AUTO
    else:
        fmt = ImportFormat.AUTO
    w.workspace.import_(
        path=workspace_path,
        content=encoded,
        format=fmt,
        overwrite=True,
    )

def upload_directory(local_dir: Path, workspace_dir: str):
    """Recursively upload a directory."""
    for item in sorted(local_dir.rglob("*")):
        if item.is_file():
            # Skip node_modules and .git
            parts = item.parts
            if "node_modules" in parts or ".git" in parts:
                continue
            rel = item.relative_to(local_dir)
            ws_path = f"{workspace_dir}/{rel}".replace("\\", "/")
            print(f"  Uploading: {rel}")
            # Ensure parent directory exists (workspace.mkdirs)
            parent = "/".join(ws_path.split("/")[:-1])
            try:
                w.workspace.mkdirs(path=parent)
            except Exception:
                pass
            upload_file(item, ws_path)

# Ensure root workspace dir exists
try:
    w.workspace.mkdirs(path=WORKSPACE_APP_PATH)
except Exception:
    pass

# Upload backend
upload_directory(HERE / "backend", f"{WORKSPACE_APP_PATH}/backend")

# Upload frontend/build (static assets)
build_dir = HERE / "frontend" / "build"
if build_dir.exists():
    upload_directory(build_dir, f"{WORKSPACE_APP_PATH}/frontend/build")

# Upload root-level files
for fname in ("app.yaml", "requirements.txt"):
    fpath = HERE / fname
    if fpath.exists():
        print(f"  Uploading: {fname}")
        upload_file(fpath, f"{WORKSPACE_APP_PATH}/{fname}")

print("  Upload complete.")

# --- Step 3: Create (or update) the app ---------------------------------------
print("\n[3/4] Creating Databricks App...")

database_resource = AppResource(
    name="lakebase-tpch",
    description="Lakebase for TPC-H demo",
    database=AppResourceDatabase(
        instance_name=instance_name,
        database_name=database_id,
        permission=AppResourceDatabaseDatabasePermission.CAN_CONNECT_AND_CREATE,
    ),
)

try:
    w.apps.create(
        app=App(
            name=APP_NAME,
            description="TPC-H Orders Demo: Delta ↔ Lakebase integration",
            resources=[database_resource],
        )
    )
    print(f"  App '{APP_NAME}' created.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  App '{APP_NAME}' already exists, will redeploy.")
    else:
        raise

# Wait for app to be RUNNING before deploying
print("  Waiting for app compute to be RUNNING...")
for _ in range(60):
    app_state = w.apps.get(name=APP_NAME)
    state = app_state.compute_status.state if app_state.compute_status else None
    print(f"  State: {state}")
    if state == ComputeState.ACTIVE:
        break
    if state in (ComputeState.ERROR, ComputeState.STOPPED):
        # Try to start the app
        print(f"  Starting app...")
        try:
            w.apps.start(name=APP_NAME).wait()
        except Exception:
            pass
        break
    time.sleep(10)

# --- Step 4: Deploy -----------------------------------------------------------
print("\n[4/4] Deploying app (SNAPSHOT mode)...")

deployment = w.apps.deploy(
    app_name=APP_NAME,
    app_deployment=AppDeployment(
        mode=AppDeploymentMode.SNAPSHOT,
        source_code_path=WORKSPACE_APP_PATH,
        env_vars=[
            EnvVar(name="LAKEBASE_HOST",     value=pg_host),
            EnvVar(name="LAKEBASE_DB",       value=database_id),
            EnvVar(name="LAKEBASE_INSTANCE", value=instance_name),
        ],
    ),
).result()

print(f"\n✓ Deployment complete!")
print(f"  Deployment ID: {deployment.deployment_id}")

# Get app URL
app_info = w.apps.get(name=APP_NAME)
print(f"  App URL: {app_info.url}")
