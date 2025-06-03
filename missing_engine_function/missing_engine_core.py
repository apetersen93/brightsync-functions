import os
import sys
import json
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "global_config")))

from sharepoint_utils import (
    download_file_from_sharepoint,
    list_sharepoint_folder
)
from engine_core import engine_main

def rerun_all_missing():
    folder = "missing_products"
    tmp_dir = "/tmp/missing_products"
    os.makedirs(tmp_dir, exist_ok=True)

    print(f"📂 Downloading missing JSON files from SharePoint: {folder}")
    files = list_sharepoint_folder(folder)
    json_files = [f for f in files if f.endswith(".json") and f.startswith("missing_products_")]

    if not json_files:
        print("✅ No missing product JSONs found.")
        return "✅ No files to rerun."

    for filename in json_files:
        local_path = os.path.join(tmp_dir, filename)
        try:
            print(f"📥 Downloading {filename}...")
            content = download_file_from_sharepoint(folder, filename)
            with open(local_path, "wb") as f:
                f.write(content)
            print(f"🚀 Running engine for {filename}...")
            engine_main(local_path)
        except Exception as e:
            print(f"❌ Failed to process {filename}: {e}")

    return "✅ Rerun of all missing product files complete."

if __name__ == "__main__":
    rerun_all_missing()
