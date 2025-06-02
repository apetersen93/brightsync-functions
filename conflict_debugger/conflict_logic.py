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
from global_config.sharepoint_utils import upload_file_to_sharepoint, download_file_from_sharepoint, get_graph_token

# 📦 Load per-store config
def load_config(store_key):
    path = os.path.join("store_configs", f"{store_key}_config.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Config file not found: {path}")
    with open(path, "r") as f:
        return json.load(f)

# 📦 Load vendor map
def load_vendor_tag_map():
    path = os.path.join(os.path.dirname(__file__), "..", "global_config", "vendor_tag_map.json")
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ vendor_tag_map.json not found at {path}")
    with open(path, "r") as f:
        return json.load(f)

def load_conflict_flags_from_sharepoint():
    try:
        file_bytes = download_file_from_sharepoint("Webstore Assets/BrightSync/cache", "conflict_flags.json")
        return json.loads(file_bytes)
    except Exception as e:
        print(f"⚠️ Failed to load conflict_flags.json from SharePoint: {e}")
        return {}

def delete_old_conflict_reports(store_name):
    try:
        access_token = get_graph_token()
        site_id = os.environ["GRAPH_SITE_ID"]
        drive_id = os.environ["GRAPH_DRIVE_ID"]

        list_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
            f"/root:/Webstore Assets/BrightSync/conflict_reports:/children"
        )
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        r = requests.get(list_url, headers=headers)
        r.raise_for_status()
        files = r.json().get("value", [])

        for f in files:
            name = f.get("name", "")
            if name.startswith(f"{store_name.lower()}_conflict_report_"):
                del_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{f['id']}"
                del_r = requests.delete(del_url, headers=headers)
                del_r.raise_for_status()
                print(f"🗑️ Deleted old conflict report: {name}")

    except Exception as e:
        print(f"⚠️ Could not delete old conflict reports: {e}")

# 🧠 Inclusion logic
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

# 🔍 Main conflict scan
def scan_conflicts(cfg):
    all_flags = load_conflict_flags_from_sharepoint()
    store_name = cfg["store_name"]
    base_url = cfg["brightstores_url"]
    token = cfg["brightstores_token"]
    date_threshold = datetime.now() - timedelta(days=cfg["inclusion_days"])
    tmp_dir = "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)

    conflict_flags_path = os.path.join(tmp_dir, "conflict_flags.json")
    date_stamp = datetime.now().strftime("%Y%m%d")
    out_path = os.path.join(tmp_dir, f"{store_name}_conflict_report_{date_stamp}.csv")

    all_prods, page = [], 1
    while True:
        url = f"{base_url}/api/v2.6.1/products?token={token}&per_page=500&page={page}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json().get("products", [])
        if not data:
            break
        all_prods.extend(data)
        page += 1

    conflict_rows = []
    conflict_skus = set()
    conflict_pids = set()
    sku_map = defaultdict(list)
    for p in all_prods:
        sku = (p.get("sku") or "").strip()
        vendors = p.get("vendors", [])
        pid = str(p["id"])
        is_active = p.get("active", True)
        updated_at = p.get("updated_at")
        updated_dt = parse_date(updated_at) if updated_at else None

        if not should_include_product(cfg, sku, vendors):
            continue
        if not is_active and (not updated_dt or updated_dt.replace(tzinfo=None) < date_threshold):
            continue
        sku_map[sku].append(p)

    for sku, entries in sku_map.items():
        if len(entries) > 1:
            for e in entries:
                conflict_rows.append(["duplicate", sku, e["id"], e["name"], ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(e["id"]))

    BAD_CHAR_PATTERN = re.compile(r"[^\w\-./ ]")
    for sku, entries in sku_map.items():
        if BAD_CHAR_PATTERN.search(sku):
            for e in entries:
                conflict_rows.append(["bad_sku_chars", sku, e["id"], e["name"], ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(e["id"]))

    vendor_map = load_vendor_tag_map()
    prefix_map = cfg.get("prefix_to_tag", {})
    for prod in all_prods:
        sku = (prod.get("sku") or "").strip()
        vendors = prod.get("vendors", [])
        pid = str(prod["id"])
        is_active = prod.get("active", True)
        updated_at = prod.get("updated_at")
        updated_dt = parse_date(updated_at) if updated_at else None

        if not should_include_product(cfg, sku, vendors):
            continue
        if not is_active and (not updated_dt or updated_dt.replace(tzinfo=None) < date_threshold):
            continue

        detail_url = f"{base_url}/api/v2.6.1/products/{pid}?token={token}"
        try:
            r = requests.get(detail_url)
            r.raise_for_status()
            d = r.json()
        except Exception as e:
            print(f"🚫 Failed to fetch product {pid}: {e}")
            continue

        def log_missing_subsku(subs, label):
            for s in subs:
                if not s.get("sub_sku"):
                    name = s.get("name", "Unnamed Option")
                    conflict_rows.append(["missing_sub_sku", sku, pid, d.get("name"), f"{label} -> {name}"])
                    conflict_skus.add(sku)
                    conflict_pids.add(pid)

        for opt in d.get("options", []):
            log_missing_subsku(opt.get("sub_options", []), "options")
        log_missing_subsku(d.get("sub_options", []), "flat")

        prefix_match = next((p for p in prefix_map if p.upper() in sku.upper()), None)
        vendor_match = any(v.get("name") in vendor_map for v in vendors)
        if not d.get("inventories") and (prefix_match or vendor_match):
            reasons = []
            if prefix_match:
                reasons.append(f"prefix:{prefix_match}")
            if vendor_match:
                reasons.extend(f"vendor:{v.get('name')}" for v in vendors if v.get("name") in vendor_map)
            conflict_rows.append(["missing_inventory", sku, pid, d.get("name", ""), ", ".join(reasons)])
            conflict_skus.add(sku)
            conflict_pids.add(pid)

    if conflict_rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Conflict Type", "SKU", "Product ID", "Name", "Sub Option"])
            writer.writerows(conflict_rows)
        print(f"📄 Conflict report saved: {out_path}")

        with open(out_path, "rb") as f:
            file_bytes = f.read()
        upload_file_to_sharepoint(
            filename=os.path.basename(out_path),
            file_bytes=file_bytes,
            target_path=f"Webstore Assets/BrightSync/conflict_reports/{store_name}_conflict_report_{date_stamp}.csv"
        )
        print("📤 Uploaded conflict report to SharePoint")
    else:
        print(f"✅ [{store_name}] No conflicts found.")
        delete_old_conflict_reports(store_name)

    if conflict_skus or conflict_pids:
        all_flags[store_name.upper()] = {
            "skus": sorted(conflict_skus),
            "pids": sorted(conflict_pids),
            "last_checked": datetime.now().isoformat()
        }
    else:
        all_flags.pop(store_name.upper(), None)

    with open(conflict_flags_path, "w") as f:
        json.dump(all_flags, f, indent=2)

    with open(conflict_flags_path, "rb") as f:
        flags_bytes = f.read()
    upload_file_to_sharepoint(
        filename="conflict_flags.json",
        file_bytes=flags_bytes,
        target_path="Webstore Assets/BrightSync/cache/conflict_flags.json"
    )
    print("📤 Uploaded conflict_flags.json to SharePoint")

# ▶️ Entry point
def run_debugger(store_key):
    cfg = load_config(store_key)
    scan_conflicts(cfg)

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("📜 Usage: python conflict_debugger.py [store_key|all]")
        sys.exit(1)
    if args[0].lower() == "all":
        for file in os.listdir("store_configs"):
            if file.endswith("_config.json"):
                try:
                    run_debugger(file.replace("_config.json", ""))
                except Exception as e:
                    print(f"❌ Failed to run debugger for {file}: {e}")
    else:
        run_debugger(args[0].lower())
