import subprocess
import logging

def main(req):
    store = req.params.get("store")

    try:
        if store:
            logging.info(f"🔁 Running sync for store: {store}")
            result = subprocess.run(["python", "sync_scripts/sync_store.py", store], check=True, capture_output=True, text=True)
        else:
            logging.info("🔁 Running full sync")
            result = subprocess.run(["python", "sync_scripts/sync_all.py"], check=True, capture_output=True, text=True)

        logging.info(result.stdout)
        return f"✅ Sync output:\n{result.stdout}"

    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        return f"❌ Sync failed:\n{e.stderr}"
        
