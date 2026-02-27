#!/usr/bin/env python3
"""
Script to list files in Google Drive folder and generate download URLs.
"""

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

def list_files_in_folder(service, folder_id):
    """List all files in a Google Drive folder"""
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, webContentLink, webViewLink, mimeType)"
        ).execute()

        files = results.get('files', [])
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

def main():
    print("List Files in Google Drive Folder")
    print("=" * 50)

    # Configuration
    credentials_path = r"C:\Users\akshay.kumar17\Documents\Secret\akshay_kumar_ads.json"
    folder_id = "1MA2y1Xowpc9rYkhZ9OIL7cUmdraIY7xr"  # User's folder ID

    if not os.path.exists(credentials_path):
        print(f"Credentials file not found: {credentials_path}")
        return

    try:
        # Authenticate with Google Drive
        creds = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        service = build('drive', 'v3', credentials=creds)

        # List files in folder
        files = list_files_in_folder(service, folder_id)

        if not files:
            print(f"No files found in folder: https://drive.google.com/drive/folders/{folder_id}")
            return

        print(f"Found {len(files)} files in folder:")
        print()

        pla_file_id = None
        pca_file_id = None

        for file in files:
            file_id = file['id']
            file_name = file['name']
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            view_url = file['webViewLink']

            print(f"File: {file_name}")
            print(f"   File ID: {file_id}")
            print(f"   Download URL: {download_url}")
            print(f"   View URL: {view_url}")
            print()

            # Identify PLA and PCA files
            if 'pla' in file_name.lower() and 'onetim' in file_name.lower():
                pla_file_id = file_id
                print(f"IDENTIFIED: PLA file - {file_name}")
            elif 'pca' in file_name.lower() and 'onetim' in file_name.lower():
                pca_file_id = file_id
                print(f"IDENTIFIED: PCA file - {file_name}")

        print("=" * 50)
        print("STREAMLIT SECRETS CONFIGURATION")
        print("=" * 50)

        print(f"GOOGLE_DRIVE_FOLDER_URL = \"https://drive.google.com/drive/folders/{folder_id}\"")

        if pla_file_id:
            print(f"PLA_CSV_URL = \"https://drive.google.com/uc?export=download&id={pla_file_id}\"")

        if pca_file_id:
            print(f"PCA_CSV_URL = \"https://drive.google.com/uc?export=download&id={pca_file_id}\"")

        print()
        print("Copy these values to your .streamlit/secrets.toml file in Streamlit Cloud!")

        # Also show legacy Sheet IDs for fallback
        print()
        print("Legacy Sheet IDs (if using Google Sheets instead):")
        print("PLA_SHEET_ID = \"1xtKC7CRhOfczJzhMgnhatU_V1wxdj1BWfMs3ZbEhcjE\"")
        print("PCA_SHEET_ID = \"1RBQOOcwLuBW7PJiyrbAsVx0ToOQAd2hZyDyi-zXsWn4\"")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()