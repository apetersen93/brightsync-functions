import logging
import subprocess
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Missing engine function triggered.")

    try:
        result = subprocess.run(
            ["python", "missing_engine_function/missing_engine_core.py"],
            check=True,
            capture_output=True,
            text=True
        )

        logging.info("📤 STDOUT:\n" + result.stdout)
        logging.info("📥 STDERR:\n" + result.stderr)
        return func.HttpResponse(f"✅ Missing engine run complete:\n{result.stdout}", status_code=200)

    except subprocess.CalledProcessError as e:
        logging.error("❌ Subprocess failed:\n" + e.stderr)
        return func.HttpResponse(f"❌ Missing engine run failed:\n{e.stderr}", status_code=500)
