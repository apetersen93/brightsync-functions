import os
import sys
import json
import csv
import requests
import subprocess

# ⬇️ Azure-compatible setup
subprocess.run([sys.executable, "-m", "pip", "install", "--target", "/tmp/pip_modules", "python-dateutil"], check=True)
sys.path.insert(0, "/tmp/pip_modules")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "global_config")))

from dateutil.parser import parse as parse_date
from sharepoint_utils import (
    upload_file_to_sharepoint,
    download_file_from_sharepoint,
    get_graph_token,
    delete_file_from_sharepoint,
    list_sharepoint_folder
)

def engine_main(sync_file_path):
    store_name = os.path.basename(sync_file_path).split("_")[0]
    print(f"📦 Loading product data from: {sync_file_path}")
    print(f"📂 File exists: {os.path.exists(sync_file_path)}")

    try:
        with open(sync_file_path, "r") as f:
            raw = f.read()
            print(f"📝 Raw sync file (first 1000 chars):\n{raw[:1000]}")
            products = json.loads(raw)
    except Exception as e:
        print(f"❌ Failed to load sync file: {e}")
        return f"❌ Failed to load sync file for {store_name}"

    print(f"🧾 Loaded {len(products)} products from {sync_file_path}")
    if len(products) > 0:
        print(f"🔍 First SKU in file: {products[0].get('sku', 'N/A')}")

    api_key = "2e138fed62f049f9b8d6b06b5e5be960"
    api_secret = "f50c4b0d4dbf428c9f189537e2fb3737"
    auth = (api_key, api_secret)
    headers = {"Content-Type": "application/json"}
    base_url = "https://ssapi.shipstation.com"

    missing = []
    updated = []

    def update_product(entry):
        sku = entry["sku"]
        print(f"🔧 Entering update_product for: {sku}")

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

        for field in [
            "productType",
            "defaultCarrierCode",
            "defaultWarehouseId",
            "defaultPackageId",
            "customsDeclaration",
        ]:
            product.pop(field, None)

        put_url = f"{base_url}/products/{productId}"
        print(f"📤 PUT to {put_url}")
        print("📦 Payload:")
        print(json.dumps(product, indent=2))

        r = requests.put(put_url, auth=auth, headers=headers, json=product)
        print(f"🛬 Response code: {r.status_code}")
        print(f"📝 Response: {r.text}")

        if r.status_code == 200:
            print(f"✅ Updated {sku}")
            updated.append(sku)
        else:
            print(f"❌ Failed to update {sku}: {r.status_code} | {r.text}")

    try:
        for item in products:
            update_product(item)
        print(f"🔁 Processed {len(products)} SKUs from sync file.")
    finally:
        if os.path.exists(sync_file_path):
            os.remove(sync_file_path)
            print(f"🧼 Cleaned up sync file: {sync_file_path}")

        sharepoint_folder = "Webstore Assets/BrightSync/sync_ready"
        
        print(f"🔎 Listing files in '{sharepoint_folder}':")
        list_sharepoint_folder(sharepoint_folder)
        
        try:
            delete_file_from_sharepoint(sharepoint_folder, f"{store_name}_sync_ready.json")
            print(f"🗑️ Removed sync file from SharePoint: {store_name}_sync_ready.json")
        except Exception as e:
            print(f"⚠️ Failed to delete sync file from SharePoint: {e}")


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

        for path in [json_path, csv_path]:
            upload_file_to_sharepoint(path, "missing_products")
        print(f"☁️ Uploaded missing reports for {store_name} to SharePoint.")
    else:
        print("✅ No missing SKUs — all products updated successfully.")
        print("📭 No missing product report uploaded.")

    print(f"✅ Final summary: {len(updated)} updated, {len(missing)} missing.")
    return f"✅ Finished engine run for {store_name}"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❗ Expected sync file path as first argument")
    else:
        engine_main(sys.argv[1])
