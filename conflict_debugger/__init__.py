import logging
import azure.functions as func
import os
import sys
import traceback

sys.path.append(os.path.dirname(__file__))

def main(req: func.HttpRequest) -> func.HttpResponse:
    store_key = req.params.get('store_key')
    if not store_key:
        return func.HttpResponse("Missing store_key parameter", status_code=400)

    try:
        from conflict_logic import run_debugger  # ✅ FIXED import
        run_debugger(store_key)
        return func.HttpResponse(f"✅ Conflict debugger ran for store: {store_key}", status_code=200)
    except Exception as e:
        tb = traceback.format_exc()
        logging.error("❌ Top-level error:\n" + tb)
        print("❌ Top-level error:\n" + tb)
        sys.stdout.flush()
        return func.HttpResponse(f"500 Error: {str(e)}", status_code=500)
