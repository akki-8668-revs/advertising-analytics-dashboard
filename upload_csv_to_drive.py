#!/usr/bin/env python3
"""
Script to upload CSV files to Google Drive folder and generate shareable download links.
"""

import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def create_drive_folder(service, folder_name="Advertising Analytics Data"):
    """Create a Google Drive folder and return its ID"""
    try:
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }

        folder = service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')

        # Make folder publicly viewable
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        service.permissions().create(fileId=folder_id, body=permission).execute()

        print(f"Created folder: {folder_name}")
        print(f"Folder ID: {folder_id}")
        print(f"Shareable link: https://drive.google.com/drive/folders/{folder_id}")

        return folder_id
    except Exception as e:
        print(f"Error creating folder: {e}")
        return None

def upload_csv_to_drive(service, file_path, folder_id, file_name=None):
    """Upload a CSV file to Google Drive folder"""
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None

        if file_name is None:
            file_name = os.path.basename(file_path)

        # File metadata
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }

        # Media upload
        media = MediaFileUpload(file_path, mimetype='text/csv', resumable=True)

        # Upload file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webContentLink'
        ).execute()

        file_id = file.get('id')
        file_name = file.get('name')

        # Make file publicly downloadable
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        service.permissions().create(fileId=file_id, body=permission).execute()

        # Generate direct download URL
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

        print(f"Uploaded: {file_name}")
        print(f"File ID: {file_id}")
        print(f"Direct download URL: {download_url}")
        print(f"View URL: https://drive.google.com/file/d/{file_id}/view")

        return file_id, download_url

    except Exception as e:
        print(f"Error uploading {file_path}: {e}")
        return None

def main():
    print("📤 Upload CSV Files to Google Drive")
    print("=" * 50)

    # Configuration
    credentials_path = r"C:\Users\akshay.kumar17\Documents\Secret\akshay_kumar_ads.json"
    pla_csv_path = "pla_onetim_2026-02-26.csv"
    pca_csv_path = "pca_onetim_2026-02-26.csv"

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

        # Create folder
        folder_id = create_drive_folder(service, "Advertising Analytics Data")

        if not folder_id:
            print("Failed to create folder")
            return

        uploaded_files = {}

        # Upload PLA CSV
        if os.path.exists(pla_csv_path):
            print(f"\nUploading PLA data...")
            result = upload_csv_to_drive(service, pla_csv_path, folder_id, "pla_advertising_data.csv")
            if result:
                uploaded_files['PLA'] = result

        # Upload PCA CSV
        if os.path.exists(pca_csv_path):
            print(f"\nUploading PCA data...")
            result = upload_csv_to_drive(service, pca_csv_path, folder_id, "pca_advertising_data.csv")
            if result:
                uploaded_files['PCA'] = result

        # Summary
        print("\n" + "=" * 50)
        print("📋 CONFIGURATION SUMMARY")
        print("=" * 50)

        print(f"GOOGLE_DRIVE_FOLDER_URL = \"https://drive.google.com/drive/folders/{folder_id}\"")

        if 'PLA' in uploaded_files:
            file_id, download_url = uploaded_files['PLA']
            print(f"PLA_CSV_URL = \"{download_url}\"")
            print(f"PLA_FILE_ID = \"{file_id}\"")

        if 'PCA' in uploaded_files:
            file_id, download_url = uploaded_files['PCA']
            print(f"PCA_CSV_URL = \"{download_url}\"")
            print(f"PCA_FILE_ID = \"{file_id}\"")

        print("\nCopy these values to your .streamlit/secrets.toml file in Streamlit Cloud!")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()