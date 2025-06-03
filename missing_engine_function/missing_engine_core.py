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

from sharepoint_utils import (
    download_file_from_sharepoint,
    upload_file_to_sharepoint,
    delete_file_from_sharepoint,
    list_sharepoint_folder
)

# 🔐 ShipStation credentials
api_key = "2e138fed62f049f9b8d6b06b5e5be960"
api_secret = "f50c4b0d4dbf428c9f189537e2fb3737"
auth = (api_key, api_secret)
headers = {"Content-Type": "application/json"}
base_url = "https://ssapi.shipstation.com"

def rerun_all_missing():
    tmp_dir = "/tmp/missing_products"
    os.makedirs(tmp_dir, exist_ok=True)
    log_path = os.path.join(tmp_dir, "error_log.csv")
    errors_logged = False

    # 📥 Pull list of files from SharePoint
    print("📥 Listing 'missing_products' files...")
    filenames = list_sharepoint_folder("missing_products")
    json_files = [f for f in filenames if f.startswith("missing_products_") and f.endswith(".json")]

    if not json_files:
        print("✅ No missing files to process.")
        return

    if not os.path.exists(log_path):
        with open(log_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Store", "SKU", "Name", "Reason"])

    for file in json_files:
        store = file.replace("missing_products_", "").replace(".json", "")
        local_json = os.path.join(tmp_dir, file)
        local_csv = os.path.join(tmp_dir, f"missing_products_{store}.csv")

        try:
            print(f"⬇️ Downloading {file}...")
            content = download_file_from_sharepoint("missing_products", file)
            with open(local_json, "wb") as f:
                f.write(content)
            missing = json.loads(content)
        except Exception as e:
            print(f"❌ Failed to download or parse {file}: {e}")
            continue

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

                existing_tag_ids = set(tag["tagId"] for tag in product.get("tags") or [])
                new_tags = item.get("tags", [])
                for tag in new_tags:
                    existing_tag_ids.add(tag["tagId"])
                product["tags"] = [{"tagId": tid} for tid in sorted(existing_tag_ids)]

                product["imageUrl"] = item.get("imageUrl", product.get("imageUrl"))
                product["thumbnailUrl"] = item.get("imageUrl", product.get("thumbnailUrl"))
                product["name"] = item.get("name", product.get("name"))
                item.pop("_tag_sources", None)

                put_url = f"{base_url}/products/{productId}"
                r = requests.put(put_url, auth=auth, headers=headers, json=product)

                if r.status_code == 200:
                    print(f"✅ Successfully updated {sku}")
                else:
                    raise Exception(f"{r.status_code} {r.text}")
            except Exception as e:
                print(f"❌ Failed for {sku}: {e}")
                still_missing.append(item)
                errors_logged = True
                with open(log_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([store, sku, item.get("name", ""), str(e)])

        # Handle output
        if still_missing:
            with open(local_json, "w") as f:
                json.dump(still_missing, f, indent=2)
            with open(local_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["SKU", "Name", "Image URL", "Order Tags"])
                for i in still_missing:
                    writer.writerow([i.get("sku", ""), i.get("name", ""), i.get("imageUrl", ""), "inventory"])

            upload_file_to_sharepoint(local_json, "missing_products")
            upload_file_to_sharepoint(local_csv, "missing_products")
            print(f"⚠️ {len(still_missing)} still missing for {store} — updated files.")
        else:
            delete_file_from_sharepoint("missing_products", file)
            delete_file_from_sharepoint("missing_products", f"missing_products_{store}.csv")
            print(f"🧹 All SKUs for {store} updated — deleted JSON and CSV.")

    # ✅ Rebuild combined CSV
    combined_rows = []
    for file in list_sharepoint_folder("missing_products"):
        if file.startswith("missing_products_") and file.endswith(".csv") and file != "missing_products_all.csv":
            try:
                content = download_file_from_sharepoint("missing_products", file)
                lines = content.decode("utf-8").splitlines()
                reader = csv.reader(lines)
                next(reader, None)
                combined_rows.extend(reader)
            except Exception as e:
                print(f"❌ Skipped {file}: {e}")

    combined_csv = os.path.join(tmp_dir, "missing_products_all.csv")
    if combined_rows:
        with open(combined_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["SKU", "Name", "Image URL", "Order Tags"])
            writer.writerows(combined_rows)
        upload_file_to_sharepoint(combined_csv, "missing_products")
        print(f"📄 Rebuilt missing_products_all.csv with {len(combined_rows)} entries.")
    else:
        delete_file_from_sharepoint("missing_products", "missing_products_all.csv")
        print("🧽 Cleaned up missing_products_all.csv")

    if os.path.exists(log_path) and not errors_logged:
        os.remove(log_path)
        print("🗑️ No errors — removed empty error_log.csv")

if __name__ == "__main__":
    rerun_all_missing()
