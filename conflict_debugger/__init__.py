import logging
import azure.functions as func
import os
import sys
import traceback

sys.path.append(os.path.dirname(__file__))

from conflict_debugger import run_debugger

def main(req: func.HttpRequest) -> func.HttpResponse:
    store_key = req.params.get('store_key')
    if not store_key:
        return func.HttpResponse("Missing store_key parameter", status_code=400)

    try:
        run_debugger(store_key)
        return func.HttpResponse(f"✅ Conflict debugger ran for store: {store_key}", status_code=200)
    except Exception as e:
        import traceback
        logging.error("❌ Conflict debugger failed:\n%s", traceback.format_exc())
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

