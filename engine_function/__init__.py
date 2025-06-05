import logging
import os
import time
import azure.functions as func

from engine_core import engine_main
from global_config.sharepoint_utils import (
    download_file_from_sharepoint,
    list_sharepoint_folder
)

def get_sync_ready_files():
    try:
        files = list_sharepoint_folder("Webstore Assets/BrightSync/sync_ready")
        return [f for f in files if f.endswith("_sync_ready.json")]
    except Exception as e:
        logging.error(f"❌ Failed to list sync files: {e}")
        return []

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Engine function triggered.")
    store = req.params.get("store")

    try:
        if not store:
            return func.HttpResponse("❗ Missing `store` query parameter", status_code=400)

        results = []

        if store.lower() == "all":
            for filename in get_sync_ready_files():
                store_key = filename.split("_")[0]
                full_path = os.path.join("/tmp", filename)
                try:
                    logging.info(f"📥 Downloading sync file for {store_key}")
                    file_bytes = download_file_from_sharepoint("Webstore Assets/BrightSync/sync_ready", filename)
                    with open(full_path, "wb") as f:
                        f.write(file_bytes)
                    logging.info(f"✅ Running engine for: {store_key}")
                    msg = engine_main(full_path)
                    results.append(msg)
                except Exception as e:
                    logging.error(f"❌ Failed to process {filename}: {e}")
                    results.append(f"❌ {store_key} failed: {e}")
                time.sleep(2)
            return func.HttpResponse("\n".join(results), status_code=200)

        # Single store engine run
        filename = f"{store}_sync_ready.json"
        full_path = f"/tmp/{filename}"
        try:
            file_bytes = download_file_from_sharepoint("Webstore Assets/BrightSync/sync_ready", filename)
            with open(full_path, "wb") as f:
                f.write(file_bytes)
            logging.info(f"✅ Downloaded sync file for {store}")
            msg = engine_main(full_path)
            return func.HttpResponse(msg, status_code=200)
        except Exception as e:
            logging.error(f"❌ Failed to run engine for {store}: {e}")
            return func.HttpResponse(f"❌ Engine failed for {store}: {e}", status_code=500)

    except Exception as e:
        logging.error(f"❌ Fatal engine error: {e}")
        return func.HttpResponse(f"❌ Unexpected engine error: {e}", status_code=500)
