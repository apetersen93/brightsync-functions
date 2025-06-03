import logging
import subprocess
import azure.functions as func
import os
import sys

# Ensure access to SharePoint utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "global_config")))
from sharepoint_utils import download_file_from_sharepoint

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Engine function triggered.")

    store = req.params.get("store")

    try:
        if store:
            filename = f"{store}_sync_ready.json"
            full_path = f"/tmp/{filename}"

            try:
                logging.info(f"📥 Attempting SharePoint download: {filename}")
                file_bytes = download_file_from_sharepoint("sync_ready", filename)
                with open(full_path, "wb") as f:
                    f.write(file_bytes)
                logging.info(f"✅ Downloaded to: {full_path}")
            except Exception as e:
                logging.error(f"❌ SharePoint download failed: {e}")
                return func.HttpResponse(f"❌ Failed to download sync file: {e}", status_code=500)

            result = subprocess.run(
                ["python", "engine_function/engine_core.py", full_path],
                check=True,
                capture_output=True,
                text=True
            )

        else:
            logging.info("🔁 Running full engine run")
            result = subprocess.run(
                ["python", "engine_function/run_all_engines.py"],
                check=True,
                capture_output=True,
                text=True
            )

        logging.info("📤 STDOUT:\n" + result.stdout)
        logging.info("📥 STDERR:\n" + result.stderr)
        return func.HttpResponse(f"✅ Engine run complete:\n{result.stdout}", status_code=200)

    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        return func.HttpResponse(f"❌ Engine run failed:\n{e.stderr}", status_code=500)
