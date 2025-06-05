import logging
import os
import time
import azure.functions as func

from sync_scripts.sync_store import load_config, delete_old_sync_file, sync_store
from global_config.sharepoint_utils import download_file_from_sharepoint

def get_store_keys():
    try:
        files = download_file_from_sharepoint("Webstore Assets/BrightSync/store_configs", None, list_only=True)
        return [f.replace("_config.json", "") for f in files if f.endswith("_config.json")]
    except Exception as e:
        logging.error(f"❌ Failed to get store list: {e}")
        return []

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Sync function triggered.")
    store = req.params.get("store")

    try:
        if not store:
            return func.HttpResponse("❗ Missing `store` query parameter", status_code=400)

        if store.lower() == "all":
            results = []
            for key in get_store_keys():
                try:
                    cfg = load_config(key)
                    delete_old_sync_file(cfg)
                    sync_store(cfg)
                    results.append(f"✅ {key} synced")
                except Exception as e:
                    logging.error(f"❌ {key} failed: {e}")
                    results.append(f"❌ {key} failed: {e}")
                time.sleep(2)  # avoid back-to-back API rate limits
            return func.HttpResponse("\n".join(results), status_code=200)

        # Single store run
        cfg = load_config(store)
        delete_old_sync_file(cfg)
        sync_store(cfg)
        return func.HttpResponse(f"✅ Sync complete for {store}", status_code=200)

    except Exception as e:
        logging.error(f"❌ Sync failed: {e}")
        return func.HttpResponse(f"❌ Sync failed: {e}", status_code=500)
