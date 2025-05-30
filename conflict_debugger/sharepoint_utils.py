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
    token = get_graph_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    url = f"https://graph.microsoft.com/v1.0/sites/cpppromos.sharepoint.com:/sites/CreativeTWassets:/drive/root:/{target_path}:/content"

    r = requests.put(url, headers=headers, data=file_bytes)
    r.raise_for_status()
    return r.json()

