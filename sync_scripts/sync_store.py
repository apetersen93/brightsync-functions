# -*- coding: utf-8 -*-
import os
import sys
import json
import requests
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from conflict_debugger.sharepoint_utils import upload_file_to_sharepoint

def load_config(store_key):
    path = os.path.join("store_configs", f"{store_key}_config.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Config file not found: {path}")
    with open(path, "r") as f:
        return json.load(f)

def fix_image_url(url, base):
    if not url:
        return None
    return url if url.startswith("http") else base.rstrip("/") + "/" + url.lstrip("/")

def fetch_updated_products(cfg, since):
    all_products, page = [], 1
    url_base = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/products?token={cfg['brightstores_token']}&updated_at_from={since}"
    while True:
        r = requests.get(f"{url_base}&per_page=500&page={page}")
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            break
        all_products.extend(batch)
        page += 1
    return all_products

def fetch_all_products(cfg):
    all_products, page = [], 1
    url_base = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/products?token={cfg['brightstores_token']}"
    while True:
        r = requests.get(f"{url_base}&per_page=500&page={page}")
        r.raise_for_status()
        batch = r.json().get("products", [])
        if not batch:
            break
        all_products.extend(batch)
        page += 1
    return all_products

def fetch_inventory(cfg):
    url = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/inventories?token={cfg['brightstores_token']}"
    all_inv, page = [], 1
    while True:
        r = requests.get(f"{url}&per_page=500&page={page}")
        r.raise_for_status()
        batch = r.json().get("inventories", [])
        if not batch:
            break
        all_inv.extend(batch)
        page += 1
    return all_inv

def fetch_product(cfg, pid):
    url = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/products/{pid}?token={cfg['brightstores_token']}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def fetch_product_options(cfg, pid):
    try:
        url = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/products/{pid}/options?token={cfg['brightstores_token']}"
        r = requests.get(url)
        r.raise_for_status()
        return r.json().get("options", [])
    except:
        return []

def fetch_sub_options(cfg, pid, oid):
    try:
        url = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/products/{pid}/options/{oid}/sub_options?token={cfg['brightstores_token']}"
        r = requests.get(url)
        r.raise_for_status()
        return r.json().get("sub_options", [])
    except:
        return []

def fetch_primary_image(cfg, pid):
    try:
        url = f"{cfg['brightstores_url'].rstrip('/')}/api/v2.6.1/products/{pid}/images?token={cfg['brightstores_token']}"
        r = requests.get(url)
        r.raise_for_status()
        imgs = r.json().get("images", [])
        for i in imgs:
            if i.get("primary"):
                return i.get("src")
        if imgs:
            return imgs[0].get("src")
    except:
        return None

def try_match_sub_option_image(cfg, product, final_sku, parent_sku, pid):
    sku_sep = cfg.get("sku_separator", "-")
    final_parts = final_sku.split(sku_sep)
    parent_parts = parent_sku.split(sku_sep)
    variant_parts = final_parts[len(parent_parts):]
    if not variant_parts:
        return None
    options = fetch_product_options(cfg, pid)
    sorted_opts = sorted(options, key=lambda o: o.get("position", 999))
    for opt in sorted_opts:
        subs = fetch_sub_options(cfg, pid, opt["id"])
        for sub in subs:
            if sub.get("sub_sku") in variant_parts and sub.get("image_src"):
                return fix_image_url(sub["image_src"], cfg["brightstores_url"])
    return None

def load_cache(cfg):
    path = os.path.join(cfg["base_dir"], "cache", f"{cfg['store_name']}_bs_cache.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}

def save_cache(cfg, cache):
    path = os.path.join(cfg["base_dir"], "cache", f"{cfg['store_name']}_bs_cache.json")
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)
        
def load_conflict_flags(store_name):
    path = os.path.join("cache", "conflict_flags.json")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                flags = json.load(f)
                entry = flags.get(store_name.upper())
                if entry:
                    return set(entry.get("skus", [])), set(entry.get("pids", []))
        except:
            print(f"⚠️ Could not load conflict flags — continuing without filter")
    return set(), set()


def load_vendor_tag_map():
    path = os.path.join(os.path.dirname(__file__), "..", "global_config", "vendor_tag_map.json")
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ vendor_tag_map.json not found at {path}")
    with open(path, "r") as f:
        return json.load(f)

def apply_tag_logic(cfg, sku, vendors):
    tag_ids = set()
    tag_sources = {}
    sku_upper = sku.upper()

    if cfg.get("filter_mode") in ["sku", "sku_or_vendor", "all", "vendor"]:
        for prefix, tag_id in cfg.get("prefix_to_tag", {}).items():
            if prefix.upper() in sku_upper:
                tag_ids.add(tag_id)
                tag_sources[str(tag_id)] = [f"prefix:{prefix}"]
                break  # first match only

    if cfg.get("filter_mode") in ["vendor", "sku_or_vendor"] and vendors:
        vendor_map = load_vendor_tag_map()
        for v in vendors:
            name = v.get("name")
            tag_id = vendor_map.get(name)
            if tag_id:
                tag_ids.add(tag_id)
                tag_sources.setdefault(str(tag_id), []).append("vendor")

    return [{"tagId": tid} for tid in tag_ids], tag_sources


def sync_store(cfg):
    last_sync = (datetime.utcnow() - timedelta(days=cfg.get("inclusion_days", 90))).isoformat()
    products = fetch_updated_products(cfg, last_sync)
    recent_ids = {p["id"] for p in products}
    cache = load_cache(cfg)
    flagged_skus, flagged_pids = load_conflict_flags(cfg["store_name"])
    updated_cache = dict(cache)

    if cfg.get("include_uncached_active", True):
        for p in fetch_all_products(cfg):
            pid = p["id"]
            if pid not in recent_ids and p.get("active", True) and str(pid) not in cache:
                products.append(p)

    ready = []
    all_inventory = fetch_inventory(cfg)
    inv_by_pid = {}
    for row in all_inventory:
        if "final_sku" in row and "product_id" in row:
            pid = row["product_id"]
            inv_by_pid.setdefault(pid, []).append(row)

    for p in products:
        pid = p["id"]
        updated_at = p.get("updated_at")
        active = p.get("active", True)
        sku = p.get("sku", "")
        vendors = p.get("vendors", [])
        if sku in flagged_skus or str(pid) in flagged_pids:
            print(f"⛔ Skipping flagged SKU: {sku}")
            continue


        # Filtering logic
        mode = cfg.get("filter_mode", "sku")
        prefix_map = cfg.get("prefix_to_tag", {})
        sku_upper = sku.upper()
        sku_match = any(prefix.upper() in sku_upper for prefix in prefix_map)

        vendor_match = any(v.get("name") in load_vendor_tag_map() for v in vendors) if vendors else False

        include_product = (
            (mode == "sku" and sku_match) or
            (mode == "vendor" and vendor_match) or
            (mode == "sku_or_vendor" and (sku_match or vendor_match)) or
            (mode == "all")
        )


        if not include_product:
            continue

        cached = cache.get(str(pid))
        if cached:
            cached_dt = parse_date(cached["updated_at"]).replace(tzinfo=None)
            live_dt = parse_date(updated_at).replace(tzinfo=None)
            cached_vendors = set(v.get("name") for v in cached.get("vendors", []) or [])
            live_vendors = set(v.get("name") for v in vendors or [])

            if cached_dt >= live_dt and cached_vendors == live_vendors:
                continue

            if not cfg.get("include_inactive", False) and not active:
                continue

        product = fetch_product(cfg, pid)
        name = product.get("name", "")
        parent_sku = product.get("sku", "")
        base_img = product.get("image")
        vendors = product.get("vendors", [])
        final_skus = inv_by_pid.get(pid, [])

        for inv_row in final_skus:
            final_sku = inv_row["final_sku"]
            if base_img:
                image = fix_image_url(base_img, cfg["brightstores_url"])
            else:
                fallback = fetch_primary_image(cfg, pid)
                image = fix_image_url(fallback, cfg["brightstores_url"]) if fallback else try_match_sub_option_image(cfg, product, final_sku, parent_sku, pid)

            tags, tag_sources = apply_tag_logic(cfg, final_sku, vendors)

            ready.append({
                "sku": final_sku,
                "name": name,
                "imageUrl": image,
                "tags": tags,
                "_tag_sources": tag_sources
            })

        updated_cache[str(pid)] = {
            "id": pid,
            "parent_sku": parent_sku,
            "updated_at": updated_at,
            "active": active,
            "final_skus": final_skus,
            "vendors": vendors
        }

    if ready:
        os.makedirs("sync_ready", exist_ok=True)
        out_path = os.path.join("sync_ready", f"{cfg['store_name']}_sync_ready.json")
        with open(out_path, "w") as f:
            json.dump(ready, f, indent=2)
        print(f"✅ {len(ready)} SKUs written to {out_path}")

    # Push the sync file to SharePoint
    try:
        upload_file_to_sharepoint(
            local_path=out_path,
            sharepoint_folder="sync_ready",  # Or "Shared Documents/sync_ready" depending on your structure
            filename=os.path.basename(out_path)
        )
        print("☁️ Uploaded sync file to SharePoint.")
    except Exception as e:
        print(f"⚠️ Failed to upload to SharePoint: {e}")

    
    else:
        print(f"🟢 No SKUs to sync for {cfg['store_name']} — skipping file write.")

    save_cache(cfg, updated_cache)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❗ Usage: python sync_store.py [store_key]")
        sys.exit(1)

    key = sys.argv[1].lower()
    cfg = load_config(key)
    sync_store(cfg)
