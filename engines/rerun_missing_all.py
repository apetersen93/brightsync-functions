# -*- coding: utf-8 -*-
import os
import json
import csv
import requests

# 🛠️ Setup
missing_dir = "missing_products"
log_path = os.path.join(missing_dir, "error_log.csv")

# 🔐 ShipStation API credentials
api_key = "2e138fed62f049f9b8d6b06b5e5be960"
api_secret = "f50c4b0d4dbf428c9f189537e2fb3737"
auth = (api_key, api_secret)
headers = {"Content-Type": "application/json"}
base_url = "https://ssapi.shipstation.com"

# 📋 Prepare error log
errors_logged = False
if not os.path.exists(log_path):
    with open(log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Store", "SKU", "Name", "Reason"])

# 📁 Process all missing product JSON files
json_files = [f for f in os.listdir(missing_dir) if f.startswith("missing_products_") and f.endswith(".json")]

for file in json_files:
    store = file.replace("missing_products_", "").replace(".json", "")
    json_path = os.path.join(missing_dir, file)
    csv_path = os.path.join(missing_dir, f"missing_products_{store}.csv")

    with open(json_path, "r", encoding="utf-8") as f:
        missing = json.load(f)

    still_missing = []

    for item in missing:
        sku = item.get("sku")
        print(f"🔁 Retrying SKU: {sku}")

        try:
            list_url = f"{base_url}/products?sku={sku}"
            r = requests.get(list_url, auth=auth)
            results = r.json().get("products", [])

            if not results:
                raise Exception("SKU still not found")

            product = results[0]
            productId = product["productId"]

            # Merge tags: keep existing, add new ones
            existing_tag_ids = set(tag["tagId"] for tag in product.get("tags") or [])
            new_tags = item.get("tags", [])
            for tag in new_tags:
                existing_tag_ids.add(tag["tagId"])

            product["tags"] = [{"tagId": tag_id} for tag_id in sorted(existing_tag_ids)]

            product["imageUrl"] = item.get("imageUrl", product.get("imageUrl"))
            product["thumbnailUrl"] = item.get("imageUrl", product.get("thumbnailUrl"))
            product["name"] = item.get("name", product.get("name"))

            put_url = f"{base_url}/products/{productId}"
            item.pop("_tag_sources", None)  # Clean out internal tracking if present
            r = requests.put(put_url, auth=auth, headers=headers, json=product)

            if r.status_code == 200:
                print(f"✅ Successfully updated {sku}")
                continue  # Don't add to still_missing
            else:
                raise Exception(f"Update failed: {r.status_code} {r.text}")


        except Exception as e:
            print(f"❌ Failed for {sku}: {e}")
            still_missing.append(item)
            errors_logged = True
            with open(log_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([store, sku, item.get("name", ""), str(e)])

    if still_missing:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(still_missing, f, indent=2)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["SKU", "Name", "Image URL", "Order Tags"])
            for item in still_missing:
                writer.writerow([
                    item.get("sku", ""),
                    item.get("name", ""),
                    item.get("imageUrl", ""),
                    "inventory"
                ])
        print(f"⚠️  {len(still_missing)} SKUs still missing for {store} — files updated.")
    else:
        os.remove(json_path)
        if os.path.exists(csv_path):
            os.remove(csv_path)
        print(f"🧹 All missing SKUs for {store} successfully synced — files deleted.")

# 🔁 Rebuild combined missing_products_all.csv
combined_csv = os.path.join(missing_dir, "missing_products_all.csv")
combined_rows = []

for file in os.listdir(missing_dir):
    if file.startswith("missing_products_") and file.endswith(".csv") and file != "missing_products_all.csv":
        path = os.path.join(missing_dir, file)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)  # Skip header
            for row in reader:
                combined_rows.append(row)

if combined_rows:
    with open(combined_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["SKU", "Name", "Image URL", "Order Tags"])
        writer.writerows(combined_rows)
    print(f"📄 Rebuilt {combined_csv} with {len(combined_rows)} entries.")
else:
    if os.path.exists(combined_csv):
        os.remove(combined_csv)
        print("🧽 Cleaned up empty missing_products_all.csv")


# 🧼 Optional: clean up error log if unused
if os.path.exists(log_path) and not errors_logged:
    os.remove(log_path)
    print("🗑️ No errors — removed empty error_log.csv")
