import logging
import time
from sync_scripts.sync_store import load_config, delete_old_sync_file, sync_store
from global_config.sharepoint_utils import list_sharepoint_folder

def get_store_keys():
    try:
        files = list_sharepoint_folder("Webstore Assets/BrightSync/store_configs")
        return [f.replace("_config.json", "") for f in files if f.endswith("_config.json")]
    except Exception as e:
        logging.error(f"❌ Failed to list store configs: {e}")
        return []

def main(mytimer):
    logging.info("⏰ Timer-triggered sync started (ALL stores)")
    for key in get_store_keys():
        try:
            cfg = load_config(key)
            delete_old_sync_file(cfg)
            sync_store(cfg)
            logging.info(f"✅ Synced store: {key}")
        except Exception as e:
            logging.error(f"❌ Sync failed for {key}: {e}")
        time.sleep(2)
