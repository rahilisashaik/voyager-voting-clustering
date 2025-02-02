import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO

def fetch_all_csv_from_drive(folder_id, credentials_path):

    SCOPES = ["https://www.googleapis.com/auth/drive"]
    credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    service = build("drive", "v3", credentials=credentials)

    query = f"'{folder_id}' in parents and mimeType='text/csv'"
    all_dataframes = []
    page_token = None

    while True:
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        files = results.get("files", [])
        page_token = results.get("nextPageToken")

        if not files:
            print("No more CSV files found in the folder.")
            break

        for file in files:
            file_id = file["id"]
            file_name = file["name"]

            print(f"Downloading {file_name}...")

            request = service.files().get_media(fileId=file_id)
            file_data = BytesIO()
            downloader = MediaIoBaseDownload(file_data, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Progress: {int(status.progress() * 100)}%")

            file_data.seek(0)
            df = pd.read_csv(file_data)
            df["source_file"] = file_name
            all_dataframes.append(df)

        if not page_token:
            break

    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else None
