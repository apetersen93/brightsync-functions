# engine_core.py
import os
import sys
import json
import csv
import requests
import subprocess

subprocess.run([sys.executable, "-m", "pip", "install", "--target", "/tmp/pip_modules", "python-dateutil"], check=True)
sys.path.insert(0, "/tmp/pip_modules")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "global_config")))

from dateutil.parser import parse as parse_date
from sharepoint_utils import upload_file_to_sharepoint, download_file_from_sharepoint, get_graph_token

def engine_main(sync_file_path):
    store_name = os.path.basename(sync_file_path).split("_")[0]
    print(f"📦 Loading product data from: {sync_file_path}")

    with open(sync_file_path, "r") as f:
        products = json.load(f)

    api_key = "2e138fed62f049f9b8d6b06b5e5be960"
    api_secret = "f50c4b0d4dbf428c9f189537e2fb3737"
    auth = (api_key, api_secret)
    headers = {"Content-Type": "application/json"}
    base_url = "https://ssapi.shipstation.com"

    missing = []

    def update_product(entry):
        sku = entry["sku"]
        print(f"🛠️ Updating SKU: {sku}")

        list_url = f"{base_url}/products?sku={sku}"
        r = requests.get(list_url, auth=auth)
        results = r.json().get("products", [])

        if not results:
            print(f"❌ SKU {sku} not found — logging for manual creation.")
            missing.append(entry)
            return

        product = results[0]
        productId = product["productId"]

        existing_tag_ids = set(tag["tagId"] for tag in product.get("tags") or [])
        desired_tag_ids = set(tag["tagId"] for tag in entry.get("tags") or [])
        tag_sources = entry.get("_tag_sources", {}) or {}
        tracked_tag_ids = set(int(tid) for tid in tag_sources.keys())
        final_tag_ids = desired_tag_ids.union(existing_tag_ids - tracked_tag_ids)

        product["tags"] = [{"tagId": tid} for tid in sorted(final_tag_ids)]
        product["imageUrl"] = entry.get("imageUrl", product.get("imageUrl"))
        product["thumbnailUrl"] = entry.get("imageUrl", product.get("thumbnailUrl"))
        product["name"] = entry.get("name", product.get("name"))

        put_url = f"{base_url}/products/{productId}"
        r = requests.put(put_url, auth=auth, headers=headers, json=product)

        if r.status_code == 200:
            print(f"✅ Updated {sku}")
        else:
            print(f"❌ Failed to update {sku}: {r.status_code} | {r.text}")

    for item in products:
        update_product(item)
    print(f"🔁 Processed {len(products)} SKUs from sync file.")

    if missing:
        print(f"⚠️ {len(missing)} products missing — writing to CSV + JSON...")
        tmp_dir = "/tmp"
        json_path = os.path.join(tmp_dir, f"missing_products_{store_name}.json")
        csv_path = os.path.join(tmp_dir, f"missing_products_{store_name}.csv")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["SKU", "Name", "Image URL", "Order Tags"])
            for item in missing:
                writer.writerow([
                    item.get("sku", ""),
                    item.get("name", ""),
                    item.get("imageUrl", ""),
                    "inventory"
                ])
        with open(json_path, "w") as f:
            json.dump(missing, f, indent=2)

        # ⬆️ Upload both to SharePoint
        for path in [json_path, csv_path]:
            upload_file_to_sharepoint(path, "missing_products")
        print(f"☁️ Uploaded missing reports for {store_name} to SharePoint.")

    else:
        print("✅ No missing SKUs — all products updated successfully.")

    # 🧼 Cleanup sync file
    if os.path.exists(sync_file_path):
        os.remove(sync_file_path)
        print(f"🧼 Cleaned up sync file: {sync_file_path}")

    return f"✅ Finished engine run for {store_name}"
