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

def delete_old_conflict_reports(store_name):
    try:
        access_token = get_graph_token()
        site_id = os.environ["GRAPH_SITE_ID"]
        drive_id = os.environ["GRAPH_DRIVE_ID"]

        list_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
            f"/root:/Webstore Assets/BrightSync/conflict_reports:/children"
        )
        headers = { "Authorization": f"Bearer {access_token}" }
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

def upload_file_to_sharepoint(local_path, folder_path, filename=None):
    filename = filename or os.path.basename(local_path)
    access_token = get_graph_token()
    site_id = os.environ["GRAPH_SITE_ID"]
    drive_id = os.environ["GRAPH_DRIVE_ID"]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream"
    }

    upload_url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
        f"/root:/{folder_path}/{filename}:/content"
    )

    # Delete existing file first if it exists
    try:
        check_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
            f"/root:/{folder_path}/{filename}"
        )
        meta_resp = requests.get(check_url, headers=headers)
        if meta_resp.status_code == 200:
            file_id = meta_resp.json().get("id")
            del_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"
            del_resp = requests.delete(del_url, headers=headers)
            del_resp.raise_for_status()
            print(f"🧹 Deleted existing SharePoint file: {filename}")
    except Exception as e:
        print(f"⚠️ Could not delete existing file (safe to ignore if not found): {e}")

    # Upload new file
    with open(local_path, "rb") as f:
        r = requests.put(upload_url, headers=headers, data=f)
        r.raise_for_status()
        print(f"📤 Overwrote SharePoint file: {filename}")

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
        pid = p.get("id")
        active = p.get("active", True)
        vendors = p.get("vendors", [])
        last_edit_raw = p.get("lastModified") or p.get("updated_at")
        try:
            last_edit = parse_date(last_edit_raw) if last_edit_raw else None
        except:
            last_edit = None

        if not (active or (last_edit and last_edit >= date_threshold)):
            continue
        if not should_include_product(cfg, sku, vendors):
            continue

        if sku and pid:
            sku_map[sku].append({"id": pid, "source": "live"})

    for sku, entries in sku_map.items():
        id_to_entry = {}
        for e in entries:
            pid = e.get("id")
            if pid and pid not in id_to_entry:
                id_to_entry[pid] = e

        if len(id_to_entry) > 1:
            for pid, e in id_to_entry.items():
                conflict_rows.append(["Duplicate SKU", sku, pid, "", ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(pid))

    if conflict_rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Conflict Type", "SKU", "Product ID", "Name", "Sub Option"])
            writer.writerows(conflict_rows)
        print(f"📄 Conflict report saved: {out_path}")

        upload_file_to_sharepoint(
            out_path,
            "Webstore Assets/BrightSync/conflict_reports",
            os.path.basename(out_path)
        )
    else:
        print(f"✅ [{store_name}] No conflicts found.")
        delete_old_conflict_reports(store_name)

    store_key = store_name.upper()
    if store_key not in all_flags:
        all_flags[store_key] = {"skus": [], "pids": []}

    existing_skus = set(all_flags[store_key].get("skus", []))
    existing_pids = set(all_flags[store_key].get("pids", []))

    updated_skus = sorted(existing_skus | conflict_skus)
    updated_pids = sorted(existing_pids | conflict_pids)

    all_flags[store_key]["skus"] = updated_skus
    all_flags[store_key]["pids"] = updated_pids

    with open(conflict_flags_path, "w") as f:
        json.dump(all_flags, f, indent=2)

    upload_file_to_sharepoint(
        conflict_flags_path,
        "Webstore Assets/BrightSync/cache",
        "conflict_flags.json"
    )

def run_debugger(store_key):
    if store_key.lower() == "all":
        for file in os.listdir("store_configs"):
            if file.endswith("_config.json"):
                try:
                    store = file.replace("_config.json", "")
                    print(f"🔁 Starting debugger for: {store}")
                    cfg = load_config(store)
                    scan_conflicts(cfg)
                except Exception as e:
                    print(f"❌ Failed to run debugger for {store}: {e}")
    else:
        try:
            cfg = load_config(store_key)
            scan_conflicts(cfg)
        except Exception as e:
            print(f"❌ Failed to run debugger for {store_key}: {e}")

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("📜 Usage: python conflict_debugger.py [store_key|all]")
        sys.exit(1)

    run_debugger(args[0].lower())
