import logging
import subprocess
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Sync function triggered.")

    store = req.params.get("store")

    try:
        if store:
            logging.info(f"🔁 Running sync for store: {store}")
            result = subprocess.run(
                ["python", "sync_scripts/sync_store.py", store],
                check=True,
                capture_output=True,
                text=True
            )
        else:
            logging.info("🔁 Running full sync")
            result = subprocess.run(
                ["python", "sync_scripts/sync_all.py"],
                check=True,
                capture_output=True,
                text=True
            )

        logging.info(result.stdout)
        return func.HttpResponse(f"✅ Sync completed:\n{result.stdout}", status_code=200)

    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        return func.HttpResponse(f"❌ Sync failed:\n{e.stderr}", status_code=500)
