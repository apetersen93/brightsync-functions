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

# 📦 Load per-store config
def load_config(store_key):
    path = os.path.join("store_configs", f"{store_key}_config.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Config file not found: {path}")
    with open(path, "r") as f:
        return json.load(f)

# 📦 Load shipstation brightstore vendor map for tagging  
def load_vendor_tag_map():
    path = os.path.join(os.path.dirname(__file__), "..", "global_config", "vendor_tag_map.json")
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ vendor_tag_map.json not found at {path}")
    with open(path, "r") as f:
        return json.load(f)

# 🧠 Decide if we should include this product
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

# 🔍 Scan store for duplicate SKUs and missing subSKUs
def scan_conflicts(cfg):
    print(f"🧠 [{cfg['store_name'].upper()}] Scanning for SKU conflicts and missing sub_skus...")

    base_url = cfg["brightstores_url"]
    token = cfg["brightstores_token"]
    store_name = cfg["store_name"]
    date_threshold = datetime.now() - timedelta(days=cfg["inclusion_days"])

    cache_dir = os.path.join(".", "cache")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{store_name}_bs_cache.json")
    conflict_flags_path = os.path.join(cache_dir, "conflict_flags.json")

    try:
        with open(cache_path, "r") as f:
            bs_cache = json.load(f)
    except FileNotFoundError:
        bs_cache = {}

    # 🌐 Pull product data
    all_prods = []
    page = 1
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

    # 🧹 Check for duplicate SKUs
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

    BAD_CHAR_PATTERN = re.compile(r"[^\w\-./ ]")  # allows letters, numbers, underscores, hyphens, perios, slash

    # 🚫 Check for SKUs with special characters
    for sku, entries in sku_map.items():
        if BAD_CHAR_PATTERN.search(sku):
            for e in entries:
                conflict_rows.append(["bad_sku_chars", sku, e["id"], e["name"], ""])
                conflict_skus.add(sku)
                conflict_pids.add(str(e["id"]))
            
    # 🕳️ Check for missing subSKUs and missing inventory
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
        if str(prod["id"]) in bs_cache and bs_cache[str(prod["id"])]["updated_at"] == updated_at:
            continue

        detail_url = f"{base_url}/api/v2.6.1/products/{pid}?token={token}"
        try:
            r = requests.get(detail_url)
            r.raise_for_status()
            d = r.json()
        except Exception as e:
            print(f"🚫 Failed to fetch product {pid}: {e}")
            continue

        bs_cache[str(prod["id"])] = {
            "id": prod["id"],
            "sku": prod.get("sku", "").strip(),
            "updated_at": prod.get("updated_at")
        }

        def log_missing_subsku(subs, label):
            for s in subs:
                sub_sku = s.get("sub_sku")
                if not sub_sku:
                    name = s.get("name", "Unnamed Option")
                    conflict_rows.append(["missing_sub_sku", sku, pid, d.get("name"), f"{label} -> {name}"])
                    conflict_skus.add(sku)
                    conflict_pids.add(pid)

        if "options" in d:
            for opt in d.get("options", []):
                log_missing_subsku(opt.get("sub_options", []), "options")

        if "sub_options" in d:
            log_missing_subsku(d.get("sub_options"), "flat")

        # 🚨 Check for missing inventory if it matches filter
        sku_upper = sku.upper()
        prefix_match = next((p for p in prefix_map if p.upper() in sku_upper), None)
        vendor_match = any(v.get("name") in vendor_map for v in vendors)
        inventory_missing = not d.get("inventories")

        if inventory_missing and (prefix_match or vendor_match):
            reason = []
            if prefix_match:
                reason.append(f"prefix:{prefix_match}")
            if vendor_match:
                matched = [v.get("name") for v in vendors if v.get("name") in vendor_map]
                reason.extend(f"vendor:{v}" for v in matched)

            conflict_rows.append(["missing_inventory", sku, pid, d.get("name", ""), ", ".join(reason)])
            conflict_skus.add(sku)
            conflict_pids.add(pid)

    # 📟 Write report
    folder = os.path.join(".", "conflict_reports")
    os.makedirs(folder, exist_ok=True)
    date_stamp = datetime.now().strftime("%Y%m%d")
    out_path = os.path.join(folder, f"{store_name}_conflict_report_{date_stamp}.csv")

    if conflict_rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Conflict Type", "SKU", "Product ID", "Name", "Sub Option"])
            writer.writerows(conflict_rows)
        print(f"🚾 Conflict report saved: {out_path}")
    else:
        print(f"✅ [{store_name}] No conflicts found.")
        if os.path.exists(out_path):
            os.remove(out_path)
            print(f"🧽 Cleared previous conflict report: {out_path}")
    # 📤 Upload to SharePoint if a report was written
    if conflict_rows:
        try:
            from sharepoint_utils import upload_file_to_sharepoint
            with open(out_path, "rb") as f:
                file_bytes = f.read()
    
            target_path = f"Webstore Assets/BrightSync/conflict_reports/{store_name}_conflict_report_{date_stamp}.csv"
            upload_file_to_sharepoint(filename=os.path.basename(out_path), file_bytes=file_bytes, target_path=target_path)
            print(f"📤 Uploaded conflict report to SharePoint: {target_path}")
        except Exception as e:
            print(f"❌ Failed to upload to SharePoint: {e}")

    

    # 📌 Update conflict flags
    all_flags = {}
    if os.path.exists(conflict_flags_path):
        try:
            with open(conflict_flags_path, "r") as f:
                content = f.read().strip()
                if content:
                    all_flags = json.loads(content)
        except json.JSONDecodeError:
            print(f"⚠️ Skipping corrupted conflict_flags.json — resetting to empty.")

    if conflict_skus or conflict_pids:
        all_flags[store_name.upper()] = {
            "skus": sorted(list(conflict_skus)),
            "pids": sorted(list(conflict_pids)),
            "last_checked": datetime.now().isoformat()
        }
    else:
        all_flags.pop(store_name.upper(), None)

    with open(conflict_flags_path, "w") as f:
        json.dump(all_flags, f, indent=2)
    print(f"📌 Conflict flags updated: {conflict_flags_path}")

# ▶️ Run for one store or all
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
                key = file.replace("_config.json", "")
                try:
                    run_debugger(key)
                except Exception as e:
                    print(f"❌ Failed to run debugger for {key}: {e}")
    else:
        run_debugger(args[0].lower())
