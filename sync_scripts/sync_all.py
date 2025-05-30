import subprocess
import os

CONFIG_DIR = "store_configs"

def get_store_keys():
    return [f.replace("_config.json", "") for f in os.listdir(CONFIG_DIR) if f.endswith("_config.json")]

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
