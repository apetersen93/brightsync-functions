# -*- coding: utf-8 -*-
import requests
import csv
import os
import json
import sys
import re
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
import subprocess

# 🛠️ Ensure dateutil works on Azure
subprocess.run([sys.executable, "-m", "pip", "install", "--target", "/tmp/pip_modules", "python-dateutil"], check=True)
sys.path.insert(0, "/tmp/pip_modules")

# 🌍 Load SharePoint helpers
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "global_config")))
from sharepoint_utils import upload_file_to_sharepoint, download_file_from_sharepoint, get_graph_token


# 📦 Load per-store config from SharePoint
def load_config(store_key):
    try:
        file_bytes = download_file_from_sharepoint(
            "Webstore Assets/BrightSync/store_configs", f"{store_key}_config.json"
        )
        return json.loads(file_bytes)
    except Exception as e:
        raise FileNotFoundError(f"❌ Failed to load config from SharePoint: {e}")


def load_vendor_tag_map():
    path = os.path.join(os.path.dirname(__file__), "..", "global_config", "vendor_tag_map.json")
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ vendor_tag_map.json not found at {path}")
    with open(path, "r") as f:
        return json.load(f)


def should_include_product(cfg, sku, vendors):
    mode = cfg.get("filter_mode", "sku")
    prefix_map = cfg.get("prefix_to_tag", {})
    sku_upper = sku.upper()
    sku_match = any(prefix.upper() in sku_upper for prefix in prefix_map)
    vendor_match = any(v.get("name") in load_vendor_tag_map() for v in vendors) if vendors else False
    return (
        (mode == "sku" and sku_match) or
        (mode == "vendor" and vendor_match) or
        (mode == "sku_or_vendor" and (sku_match or vendor_match)) or
        (mode == "all")
    )


def scan_conflicts(cfg):
    print(f"🔎 [{cfg['store_name']}] Checking for conflicts...")

    tmp_dir = "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    store = cfg["store_name"]
    base_url = cfg["brightstores_url"]
    token = cfg["brightstores_token"]
    vendor_map = load_vendor_tag_map()
    prefix_map = cfg.get("prefix_to_tag", {})
    date_threshold = datetime.now() - timedelta(days=cfg["inclusion_days"])

    # Load conflict flags
    try:
        flag_bytes = download_file_from_sharepoint("Webstore Assets/BrightSync/cache", "conflict_flags.json")
        all_flags = json.loads(flag_bytes)
    except:
        all_flags = {}

    # Load BrightStores cache
    try:
        cache_bytes = download_file_from_sharepoint("Webstore Assets/BrightSync/cache", f"{store.lower()}_bs_cache.json")
        bs_cache = json.loads(cache_bytes)
    except:
        bs_cache = {}

    # Fetch product index
    all_prods, page = [], 1
    while True:
        url = f"{base_url}/api/v2.6.1/products?token={token}&per_page=500&page={page}"
        r = requests.get(url)
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            break
        all_prods.extend(batch)
        page += 1

    sku_map = defaultdict(list)
    conflict_rows = []
    conflict_skus = set()
    conflict_pids = set()
    BAD_CHAR_PATTERN = re.compile(r"[^\w\-./ ]")

    for p in all_prods:
        sku = (p.get("sku") or "").strip()
        pid = str(p["id"])
        vendors = p.get("vendors", [])
        active = p.get("active", True)
        last_edit = p.get("updated_at") or p.get("lastModified")
        updated_dt = parse_date(last_edit) if last_edit else None

        if not should_include_product(cfg, sku, vendors):
            continue
        if not active and (not updated_dt or updated_dt.replace(tzinfo=None) < date_threshold):
            continue

        sku_map[sku].append(p)

    # Check for duplicate and bad-character SKUs
    for sku, entries in sku_map.items():
        ids = set(e.get("id") for e in entries if e.get("id"))
        if len(ids) > 1:
            for e in entries:
                conflict_rows.append(["duplicate", sku, e["id"], e.get("name", ""), ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(e["id"]))
        if BAD_CHAR_PATTERN.search(sku):
            for e in entries:
                conflict_rows.append(["bad_sku_chars", sku, e["id"], e.get("name", ""), ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(e["id"]))

    # Detailed checks for missing sub_skus and inventory
    for p in all_prods:
        sku = (p.get("sku") or "").strip()
        pid = str(p["id"])
        vendors = p.get("vendors", [])
        active = p.get("active", True)
        last_edit = p.get("updated_at") or p.get("lastModified")
        updated_dt = parse_date(last_edit) if last_edit else None

        if not should_include_product(cfg, sku, vendors):
            continue
        if not active and (not updated_dt or updated_dt.replace(tzinfo=None) < date_threshold):
            continue
        if pid in bs_cache and bs_cache[pid].get("updated_at") == last_edit:
            continue

        # Fetch full product detail
        try:
            r = requests.get(f"{base_url}/api/v2.6.1/products/{pid}?token={token}")
            r.raise_for_status()
            d = r.json()
        except:
            continue

        def log_missing_subsku(subs, label):
            for s in subs:
                if not s.get("sub_sku"):
                    conflict_rows.append(["missing_sub_sku", sku, pid, d.get("name", ""), f"{label} -> {s.get('name')}"])
                    conflict_skus.add(sku)
                    conflict_pids.add(pid)

        for opt in d.get("options", []):
            log_missing_subsku(opt.get("sub_options", []), "options")
        log_missing_subsku(d.get("sub_options", []), "flat")

        # Missing inventory
        prefix_match = next((p for p in prefix_map if p.upper() in sku.upper()), None)
        vendor_match = any(v.get("name") in vendor_map for v in vendors)
        if not d.get("inventories") and (prefix_match or vendor_match):
            reason = []
            if prefix_match:
                reason.append(f"prefix:{prefix_match}")
            if vendor_match:
                matched = [v.get("name") for v in vendors if v.get("name") in vendor_map]
                reason.extend(f"vendor:{v}" for v in matched)
            conflict_rows.append(["missing_inventory", sku, pid, d.get("name", ""), ", ".join(reason)])
            conflict_skus.add(sku)
            conflict_pids.add(pid)

        if pid in bs_cache:
            bs_cache[pid]["updated_at"] = last_edit

    # Write report to /tmp and upload
    out_path = os.path.join(tmp_dir, f"{store}_conflict_report.csv")
    if conflict_rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Conflict Type", "SKU", "Product ID", "Name", "Sub Option"])
            writer.writerows(conflict_rows)
        upload_file_to_sharepoint(out_path, "Webstore Assets/BrightSync/conflict_reports", os.path.basename(out_path))
        print(f"📤 Uploaded conflict report: {out_path}")
    else:
        print(f"✅ No conflicts found for {store}")
        try:
            access_token = get_graph_token()
            site_id = os.environ["GRAPH_SITE_ID"]
            drive_id = os.environ["GRAPH_DRIVE_ID"]
            del_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/Webstore Assets/BrightSync/conflict_reports/{store}_conflict_report.csv"
            headers = {"Authorization": f"Bearer {access_token}"}
            requests.delete(del_url, headers=headers)
        except:
            pass

    # Update cache and conflict_flags
    flag_path = os.path.join(tmp_dir, "conflict_flags.json")
    cache_path = os.path.join(tmp_dir, f"{store.lower()}_bs_cache.json")

    if conflict_skus or conflict_pids:
        all_flags[store.upper()] = {
            "skus": sorted(list(conflict_skus)),
            "pids": sorted(list(conflict_pids)),
            "last_checked": datetime.now().isoformat()
        }
    else:
        all_flags.pop(store.upper(), None)

    with open(flag_path, "w") as f:
        json.dump(all_flags, f, indent=2)
    upload_file_to_sharepoint(flag_path, "Webstore Assets/BrightSync/cache", "conflict_flags.json")

    with open(cache_path, "w") as f:
        json.dump(bs_cache, f, indent=2)
    upload_file_to_sharepoint(cache_path, "Webstore Assets/BrightSync/cache", os.path.basename(cache_path))


# ▶️ Allow single or all store run
def run_debugger(store_key):
    if store_key.lower() == "all":
        for file in os.listdir("store_configs"):
            if file.endswith("_config.json"):
                try:
                    key = file.replace("_config.json", "")
                    scan_conflicts(load_config(key))
                except Exception as e:
                    print(f"❌ Failed to run debugger for {file}: {e}")
    else:
        scan_conflicts(load_config(store_key))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❗ Usage: python conflict_logic.py [store_key|all]")
        sys.exit(1)
    run_debugger(sys.argv[1])
