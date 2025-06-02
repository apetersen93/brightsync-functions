def run_engine_sync(store=None):
    import os
    import tempfile
    from your_module import engine_main  # You will rename your core engine logic to this
    from sharepoint_utils import download_file_from_sharepoint, upload_file_to_sharepoint

    if store:
        filename = f"{store}_sync_ready.json"
        download_file_from_sharepoint("sync_ready", filename, f"/tmp/{filename}")
        result = engine_main(f"/tmp/{filename}")
    else:
        # Run all syncs in /tmp
        download_file_from_sharepoint("sync_ready", target_dir="/tmp/sync_ready")
        for file in os.listdir("/tmp/sync_ready"):
            if file.endswith("_sync_ready.json"):
                result = engine_main(os.path.join("/tmp/sync_ready", file))
    return "✅ Engine completed"
