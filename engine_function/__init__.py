import logging
import subprocess
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Engine function triggered.")

    store = req.params.get("store")

    try:
        if store:
            logging.info(f"🔁 Running engine for store: {store}")
            result = subprocess.run(
                ["python", "engine_function/engine_core.py", store],
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

        logging.info(result.stdout)
        return func.HttpResponse(f"✅ Engine run complete:\n{result.stdout}", status_code=200)

    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        return func.HttpResponse(f"❌ Engine run failed:\n{e.stderr}", status_code=500)
