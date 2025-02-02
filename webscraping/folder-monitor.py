import os
import time
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO

def fetch_new_csv_files(folder_id, credentials_path, processed_files):
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
            break

        for file in files:
            file_id = file["id"]
            file_name = file["name"]

            if file_id in processed_files:
                continue

            print(f"Downloading new file: {file_name}...")

            request = service.files().get_media(fileId=file_id)
            file_data = BytesIO()
            downloader = MediaIoBaseDownload(file_data, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

            file_data.seek(0)
            df = pd.read_csv(file_data)
            df["source_file"] = file_name
            all_dataframes.append(df)

            processed_files.add(file_id)

        if not page_token:
            break

    if all_dataframes:
        return pd.concat(all_dataframes, ignore_index=True)
    else:
        return pd.DataFrame()


def monitor_folder(folder_id, credentials_path, csv_file="live_data.csv", interval=60):
    processed_files = set()

    while True:
        try:
            print("Checking for new files...")
            new_data = fetch_new_csv_files(folder_id, credentials_path, processed_files)

            if not new_data.empty:
                print("New files detected, appending to CSV")
                if os.path.exists(csv_file):
                    new_data.to_csv(csv_file, mode="a", header=False, index=False)
                else:
                    new_data.to_csv(csv_file, index=False)
                print(f"New data appended to {csv_file}.")

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Monitoring stopped.")
            break


if __name__ == "__main__":
    folder_id = "1EafyzmaIUfPRXzwt8s4TSOQcQ00U4BTF"
    credentials_path = os.path.join(os.getcwd(), "voyager-voting-clusters-3e85add6e250.json")
    monitor_folder(folder_id, credentials_path, csv_file="live_data.csv", interval=60)
