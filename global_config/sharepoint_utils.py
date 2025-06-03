import os
import requests

def get_graph_token():
    url = f"https://login.microsoftonline.com/{os.environ['GRAPH_TENANT_ID']}/oauth2/v2.0/token"
    headers = { "Content-Type": "application/x-www-form-urlencoded" }
    data = {
        "client_id": os.environ["GRAPH_CLIENT_ID"],
        "client_secret": os.environ["GRAPH_CLIENT_SECRET"],
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }
    r = requests.post(url, headers=headers, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

def upload_file_to_sharepoint(filename, file_bytes, target_path):
    access_token = get_graph_token()  # your auth code
    site_id = os.environ["GRAPH_SITE_ID"]
    drive_id = os.environ["GRAPH_DRIVE_ID"]

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{target_path}:/content"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream"
    }
    r = requests.put(url, headers=headers, data=file_bytes)
    r.raise_for_status()

def download_file_from_sharepoint(folder_path, filename):
    access_token = get_graph_token()
    site_id = os.environ["GRAPH_SITE_ID"]
    drive_id = os.environ["GRAPH_DRIVE_ID"]

    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
        f"/root:/{folder_path}/{filename}:/content"
    )

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.content

def delete_file_from_sharepoint(folder: str, filename: str):
    token = get_graph_token()

    ssite_id = os.environ["GRAPH_SITE_ID"]
    drive_id = os.environ["GRAPH_DRIVE_ID"]

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder}/{filename}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    r = requests.delete(url, headers=headers)
    if r.status_code == 204:
        print(f"🗑️ Deleted file: {filename}")
    elif r.status_code == 404:
        print(f"⚠️ File not found for deletion: {filename}")
    else:
        raise Exception(f"❌ Failed to delete file: {r.status_code} | {r.text}")



