import subprocess
import logging

def main(req):
    try:
        store = req.params.get("store")
        if store:
            logging.info(f"🔁 Running sync for single store: {store}")
            subprocess.run(["python", "sync_scripts/sync_store.py", store], check=True)
            return f"✅ Sync complete for store: {store}"
        else:
            logging.info("🔁 Running full sync for all stores")
            subprocess.run(["python", "sync_scripts/sync_all.py"], check=True)
            return "✅ Full sync complete"
    except subprocess.CalledProcessError as e:
        return f"❌ Sync failed: {str(e)}"
