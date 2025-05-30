import subprocess

def main(req):
    subprocess.run(["python", "sync_scripts/sync_all.py"], check=True)
    return "✅ Sync triggered."
