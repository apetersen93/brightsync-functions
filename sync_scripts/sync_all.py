import subprocess
import os
from global_config.sharepoint_utils import download_file_from_sharepoint

def get_store_keys():
    # Get list of config files directly from SharePoint folder
    try:
        files = download_file_from_sharepoint("Webstore Assets/BrightSync/store_configs", None, list_only=True)
        return [f.replace("_config.json", "") for f in files if f.endswith("_config.json")]
    except Exception as e:
        print(f"❌ Failed to fetch store keys: {e}")
        return []

def main():
    stores = get_store_keys()
    for store in stores:
        print(f"🔄 Running daily sync for: {store}")
        try:
            subprocess.run(["python", "sync_scripts/sync_store.py", store], check=True)
        except subprocess.CalledProcessError:
            print(f"❌ Sync failed for: {store}")
        print("—" * 40)

if __name__ == "__main__":
    main()
