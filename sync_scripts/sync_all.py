import os
import time
import requests
from global_config.sharepoint_utils import download_file_from_sharepoint

AZURE_SYNC_URL = "https://brightsync-functions.azurewebsites.net/api/sync_function"
API_CODE = os.environ.get("AZURE_SYNC_FUNCTION_KEY")
if not API_CODE:
    raise RuntimeError("AZURE_SYNC_FUNCTION_KEY is not set in environment variables.")

def get_store_keys():
    try:
        files = download_file_from_sharepoint("Webstore Assets/BrightSync/store_configs", None, list_only=True)
        return [f.replace("_config.json", "") for f in files if f.endswith("_config.json")]
    except Exception as e:
        print(f"❌ Failed to fetch store keys: {e}")
        return []

def main():
    store_keys = get_store_keys()
    for store in store_keys:
        print(f"🔄 Syncing: {store}")
        try:
            url = f"{AZURE_SYNC_URL}?store={store}&code={API_CODE}"
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            print(f"✅ Synced: {store}")
        except Exception as e:
            print(f"❌ Failed to sync {store}: {e}")
        print("—" * 40)
        time.sleep(3)  # optional throttle

if __name__ == "__main__":
    main()
