import logging
import azure.functions as func
import subprocess

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Rerun Missing Function triggered.")
    try:
        result = subprocess.run(
            ["python", "engine_function/rerun_missing_all.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(result.stdout)
        return func.HttpResponse(f"✅ Missing rerun complete:\n{result.stdout}", status_code=200)
    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        return func.HttpResponse(f"❌ Missing rerun failed:\n{e.stderr}", status_code=500)
