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

def load_config(store_key):
    path = os.path.join("store_configs", f"{store_key}_config.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Config file not found: {path}")
    with open(path, "r") as f:
        return json.load(f)

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

def load_bs_cache(store_name):
    try:
        cache_file = f"{store_name.lower()}_bs_cache.json"
        file_bytes = download_file_from_sharepoint("Webstore Assets/BrightSync/cache", cache_file)
        return json.loads(file_bytes)
    except Exception as e:
        print(f"⚠️ Failed to load bs_cache for {store_name}: {e}")
        return {}

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
    all_flags = load_conflict_flags_from_sharepoint()
    bs_cache = load_bs_cache(cfg["store_name"])
    store_name = cfg["store_name"]
    base_url = cfg["brightstores_url"]
    token = cfg["brightstores_token"]
    date_threshold = datetime.now() - timedelta(days=cfg["inclusion_days"])
    tmp_dir = "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)

    conflict_flags_path = os.path.join(tmp_dir, "conflict_flags.json")
    out_path = os.path.join(tmp_dir, f"{store_name}_conflict_report.csv")

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
        pid = str(p.get("id"))
        vendors = p.get("vendors", [])
        active = p.get("active", True)
        last_edit = p.get("updated_at") or p.get("lastModified")
        updated_dt = parse_date(last_edit) if last_edit else None

        if not should_include_product(cfg, sku, vendors):
            continue
        if not active and (not updated_dt or updated_dt.replace(tzinfo=None) < date_threshold):
            continue

        sku_map[sku].append(p)

    for sku, entries in sku_map.items():
        ids = set(e.get("id") for e in entries if e.get("id"))
        if len(ids) > 1:
            for e in entries:
                conflict_rows.append(["duplicate", sku, e["id"], e.get("name", ""), ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(e["id"]))

    for prod in all_prods:
        sku = (prod.get("sku") or "").strip()
        vendors = prod.get("vendors", [])
        pid = str(prod.get("id"))
        active = prod.get("active", True)
        last_edit = prod.get("updated_at") or prod.get("lastModified")
        updated_dt = parse_date(last_edit) if last_edit else None

        if not should_include_product(cfg, sku, vendors):
            continue
        if not active and (not updated_dt or updated_dt.replace(tzinfo=None) < date_threshold):
            continue
        if pid in bs_cache and bs_cache[pid].get("updated_at") == last_edit:
            continue

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

        bs_cache[pid] = {
            "id": pid,
            "sku": sku,
            "updated_at": last_edit
        }

    if conflict_rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Conflict Type", "SKU", "Product ID", "Name", "Sub Option"])
            writer.writerows(conflict_rows)
        print(f"📄 Conflict report saved: {out_path}")

        upload_file_to_sharepoint(
            out_path,
            "Webstore Assets/BrightSync/conflict_reports",
            f"{store_name}_conflict_report.csv"
        )
        print("📤 Uploaded conflict report to SharePoint")
    else:
        print(f"✅ [{store_name}] No conflicts found.")
        try:
            access_token = get_graph_token()
            site_id = os.environ["GRAPH_SITE_ID"]
            drive_id = os.environ["GRAPH_DRIVE_ID"]
            del_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/Webstore Assets/BrightSync/conflict_reports/{store_name}_conflict_report.csv"
            headers = {"Authorization": f"Bearer {access_token}"}
            requests.delete(del_url, headers=headers)
            print(f"🧽 Removed old conflict report for {store_name}")
        except:
            pass

    all_flags[store_name.upper()] = {
        "skus": sorted(list(conflict_skus)),
        "pids": sorted(list(conflict_pids)),
        "last_checked": datetime.now().isoformat()
    } if conflict_rows else all_flags.pop(store_name.upper(), None)

    with open(conflict_flags_path, "w") as f:
        json.dump(all_flags, f, indent=2)
    upload_file_to_sharepoint(conflict_flags_path, "Webstore Assets/BrightSync/cache", "conflict_flags.json")
    with open(os.path.join(tmp_dir, f"{store_name.lower()}_bs_cache.json"), "w") as f:
        json.dump(bs_cache, f, indent=2)
    upload_file_to_sharepoint(
        os.path.join(tmp_dir, f"{store_name.lower()}_bs_cache.json"),
        "Webstore Assets/BrightSync/cache",
        f"{store_name.lower()}_bs_cache.json"
    )

def run_debugger(store_key):
    if store_key.lower() == "all":
        for file in os.listdir("store_configs"):
            if file.endswith("_config.json"):
                try:
                    scan_conflicts(load_config(file.replace("_config.json", "")))
                except Exception as e:
                    print(f"❌ Failed to run debugger for {file}: {e}")
    else:
        scan_conflicts(load_config(store_key))

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("📜 Usage: python conflict_debugger.py [store_key|all]")
        sys.exit(1)
    run_debugger(args[0])
