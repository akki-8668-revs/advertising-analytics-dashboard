#!/usr/bin/env python3
"""
Script to upload CSV data to Google Sheets for the Advertising Analytics dashboard.
"""

import pandas as pd
import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st

def upload_csv_to_sheets(csv_file_path, sheet_name, credentials_path):
    """
    Upload CSV data to Google Sheets (legacy function - creates new sheet)
    """
    try:
        # Authenticate with Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        gc = gspread.authorize(credentials)

        # Create or open spreadsheet
        try:
            sh = gc.open(sheet_name)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(sheet_name)

        # Read CSV data
        df = pd.read_csv(csv_file_path)

        # Clear existing data and upload new data
        worksheet = sh.get_worksheet(0)
        worksheet.clear()

        # Convert DataFrame to list of lists
        data = [df.columns.tolist()] + df.values.tolist()

        # Update the worksheet
        worksheet.update('A1', data)

        print(f"SUCCESS: Successfully uploaded {csv_file_path} to Google Sheets")
        print(f"Sheet URL: {sh.url}")
        print(f"Sheet ID: {sh.id}")

        return sh.id

    except Exception as e:
        print(f"ERROR uploading {csv_file_path}: {e}")
        return None

def upload_csv_to_existing_sheet(csv_file_path, sheet_id, credentials_path, worksheet_name="Sheet1"):
    """
    Upload CSV data to an existing Google Sheets worksheet
    """
    try:
        # Authenticate with Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        gc = gspread.authorize(credentials)

        # Open existing spreadsheet by ID
        sh = gc.open_by_key(sheet_id)

        # Try to get the worksheet by name, or create it if it doesn't exist
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_name, rows="1000", cols="50")

        # Read CSV data
        df = pd.read_csv(csv_file_path)

        # Handle NaN values by replacing them with empty strings
        df = df.fillna('')

        # Clear existing data and upload new data
        worksheet.clear()

        # Convert DataFrame to list of lists
        data = [df.columns.tolist()] + df.values.tolist()

        # Update the worksheet
        worksheet.update('A1', data)

        print(f"SUCCESS: Successfully uploaded {csv_file_path} to Google Sheets worksheet '{worksheet_name}'")
        print(f"Sheet URL: {sh.url}")

        return True

    except Exception as e:
        print(f"ERROR uploading {csv_file_path}: {e}")
        return False

def main():
    print("Advertising Analytics - Data Upload to Google Sheets")
    print("=" * 60)

    # Configuration - Hardcoded paths
    credentials_path = r"C:\Users\akshay.kumar17\Documents\Secret\akshay_kumar_ads.json"
    pla_csv_path = "pla_onetim_2026-02-26.csv"
    pca_csv_path = "pca_onetim_2026-02-26.csv"

    # Hardcoded sheet IDs
    pla_sheet_id = "1xtKC7CRhOfczJzhMgnhatU_V1wxdj1BWfMs3ZbEhcjE"
    pca_sheet_id = "1RBQOOcwLuBW7PJiyrbAsVx0ToOQAd2hZyDyi-zXsWn4"

    print(f"Using credentials: {credentials_path}")
    print(f"PLA Sheet ID: {pla_sheet_id}")
    print(f"PCA Sheet ID: {pca_sheet_id}")

    if not os.path.exists(credentials_path):
        print(f"❌ Credentials file not found: {credentials_path}")
        return

    # Upload PLA data to existing sheet
    if os.path.exists(pla_csv_path):
        print(f"\nUploading PLA data from {pla_csv_path}...")
        result = upload_csv_to_existing_sheet(pla_csv_path, pla_sheet_id, credentials_path, "PLA_Data")
        if result:
            print(f"SUCCESS: PLA data uploaded to sheet: {pla_sheet_id}")
        else:
            print(f"ERROR: Failed to upload PLA data")
    else:
        print(f"WARNING: PLA CSV file not found: {pla_csv_path}")

    # Upload PCA data to existing sheet
    if os.path.exists(pca_csv_path):
        print(f"\nUploading PCA data from {pca_csv_path}...")
        result = upload_csv_to_existing_sheet(pca_csv_path, pca_sheet_id, credentials_path, "PCA_Data")
        if result:
            print(f"SUCCESS: PCA data uploaded to sheet: {pca_sheet_id}")
        else:
            print(f"ERROR: Failed to upload PCA data")
    else:
        print(f"WARNING: PCA CSV file not found: {pca_csv_path}")

    print("\nNext Steps:")
    print("1. Copy the Sheet IDs above")
    print("2. Update your .streamlit/secrets.toml file")
    print("3. Share the Google Sheets with your service account email")
    print("4. Deploy to Streamlit Cloud!")

if __name__ == "__main__":
    main()