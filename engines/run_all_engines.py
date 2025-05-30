import os
import subprocess
import json
import csv
from datetime import datetime

def run_engines_from_sync_ready():
    ready_dir = "sync_ready"
    missing_dir = "missing_products"
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"engine_log_{timestamp}.txt")

    with open(log_file_path, "w", encoding="utf-8") as log:
        if not os.path.exists(ready_dir):
            msg = "❌ sync_ready directory not found. Aborting."
            print(msg)
            log.write(msg + "\n")
            return

        files = [f for f in os.listdir(ready_dir) if f.endswith("_sync_ready.json")]
        if not files:
            msg = "✅ No sync_ready files found. Nothing to push."
            print(msg)
            log.write(msg + "\n")
            return

        for f in files:
            full_path = os.path.join(ready_dir, f)
            store = f.split("_")[0]
            msg = f"🚀 Running engine for: {store}"
            print(msg)
            log.write(msg + "\n")

            try:
                subprocess.run(["python", os.path.join("engines", "engine.py"), full_path], check=True)
            except subprocess.CalledProcessError as e:
                err = f"❌ Engine failed for {store}: {str(e)}"
                print(err)
                log.write(err + "\n")

        # 🔄 Combine all missing JSONs into a single CSV
        combined_csv_path = os.path.join(missing_dir, "missing_products_all.csv")
        combined_rows = []

        json_files = [f for f in os.listdir(missing_dir) if f.startswith("missing_products_") and f.endswith(".json")]
        for file in json_files:
            store = file.replace("missing_products_", "").replace(".json", "")
            path = os.path.join(missing_dir, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for item in data:
                        combined_rows.append([
                            store,
                            item.get("sku", ""),
                            item.get("name", ""),
                            item.get("imageUrl", ""),
                            "inventory"
                        ])
            except Exception as e:
                print(f"⚠️ Failed to read {file}: {e}")
                log.write(f"⚠️ Failed to read {file}: {e}\n")

        if combined_rows:
            with open(combined_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Store", "SKU", "Name", "Image URL", "Order Tags"])
                writer.writerows(combined_rows)
            print(f"🚨 Combined missing_products_all.csv created with {len(combined_rows)} rows")
            log.write(f"🚨 Combined missing_products_all.csv created with {len(combined_rows)} rows\n")
        else:
            print("✅ No missing products found. No combined CSV created.")
            log.write("✅ No missing products found. No combined CSV created.\n")

if __name__ == "__main__":
    run_engines_from_sync_ready()
