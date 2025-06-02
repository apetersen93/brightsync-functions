import logging
import azure.functions as func
from engine_runner import run_engine_sync

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Engine function triggered.")

    # Optional: accept 'store' as a parameter to run one sync
    store = req.params.get("store")
    result = run_engine_sync(store)

    return func.HttpResponse(result, status_code=200)
