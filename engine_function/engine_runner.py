import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "global_config")))

print("📦 Importing engine_core...")
from engine_core import engine_main

print("🔗 Importing SharePoint utils...")
from sharepoint_utils import download_file_from_sharepoint, upload_file_to_sharepoint

def run_engine_sync(store=None):
    print("🚀 ENGINE FUNCTION TRIGGERED")

    if store:
        filename = f"{store}_sync_ready.json"
        print(f"📥 Downloading {filename} from SharePoint...")
        download_file_from_sharepoint("sync_ready", filename, f"/tmp/{filename}")
        print("📄 Running engine_main...")
        result = engine_main(f"/tmp/{filename}")
    else:
        download_file_from_sharepoint("sync_ready", target_dir="/tmp/sync_ready")
        for file in os.listdir("/tmp/sync_ready"):
            if file.endswith("_sync_ready.json"):
                result = engine_main(os.path.join("/tmp/sync_ready", file))

    return "✅ Engine completed"
