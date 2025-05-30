# -*- coding: utf-8 -*-
import sys
import json
import requests
import csv
import os

# 📂 Load sync file
file_to_load = sys.argv[1] if len(sys.argv) > 1 else "wcu_sync_ready.json"
store_name = os.path.basename(file_to_load).split("_")[0]

print(f"📦 Loading product data from: {file_to_load}")
with open(file_to_load, "r") as f:
    products = json.load(f)

# 🔐 API Credentials
api_key = "2e138fed62f049f9b8d6b06b5e5be960"
api_secret = "f50c4b0d4dbf428c9f189537e2fb3737"

# ⚙️ Setup
auth = (api_key, api_secret)
headers = {"Content-Type": "application/json"}
base_url = "https://ssapi.shipstation.com"

missing = []

# 🛠️ Update product in ShipStation or flag as missing
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

    # 🧠 Smart tag merge with optional removal logic
    existing_tag_ids = set(tag["tagId"] for tag in product.get("tags") or [])
    desired_tag_ids = set(tag["tagId"] for tag in entry.get("tags") or [])

    # Determine tag sources if available
    tag_sources = entry.get("_tag_sources", {}) or {}
    tracked_tag_ids = set(int(tid) for tid in tag_sources.keys())

    # Preserve tags we don't track (manual or external)
    final_tag_ids = desired_tag_ids.union(existing_tag_ids - tracked_tag_ids)

    product["tags"] = [{"tagId": tid} for tid in sorted(final_tag_ids)]

    # Only update needed fields
    product["imageUrl"] = entry.get("imageUrl", product.get("imageUrl"))
    product["thumbnailUrl"] = entry.get("imageUrl", product.get("thumbnailUrl"))
    product["name"] = entry.get("name", product.get("name"))

    put_url = f"{base_url}/products/{productId}"
    print(json.dumps(product, indent=2))
    r = requests.put(put_url, auth=auth, headers=headers, json=product)

    if r.status_code == 200:
        print(f"✅ Updated {sku}")
    else:
        print(f"❌ Failed to update {sku}: {r.status_code} | {r.text}")

# ▶️ Run sync
for item in products:
    update_product(item)

# 📥 Export missing SKUs to files
if missing:
    print(f"⚠️ {len(missing)} products missing — writing to CSV + JSON...")

    os.makedirs("missing_products", exist_ok=True)

    json_path = os.path.join("missing_products", f"missing_products_{store_name}.json")
    csv_path = os.path.join("missing_products", f"missing_products_{store_name}.csv")

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

    print(f"📄 Files created: {csv_path} and {json_path}")

    if len(missing) == len(products):
        os.remove(file_to_load)
        print(f"🧹 All products were missing — deleted sync file: {file_to_load}")
    else:
        os.remove(file_to_load)
        print(f"🧼 Cleaned up sync file: {file_to_load}")

else:
    print("✅ No missing SKUs — all products updated successfully.")
    if os.path.exists(file_to_load):
        os.remove(file_to_load)
        print(f"🧼 Cleaned up sync file: {file_to_load}")
