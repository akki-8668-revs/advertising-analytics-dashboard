import os
import streamlit as st
import requests
from google.oauth2 import service_account
import json

# Configuration for Google Drive shared credentials
GOOGLE_DRIVE_CREDENTIALS_URL = st.secrets.get("GOOGLE_DRIVE_CREDENTIALS_URL", "")
GOOGLE_SHEET_DATA_URL = st.secrets.get("GOOGLE_SHEET_DATA_URL", "")
GOOGLE_DOC_TEMPLATE_ID = st.secrets.get("GOOGLE_DOC_TEMPLATE_ID", "")

def get_google_credentials():
    """Get Google credentials from shared Google Drive link"""
    try:
        if not GOOGLE_DRIVE_CREDENTIALS_URL:
            st.error("Google Drive credentials URL not configured. Please contact administrator.")
            return None

        # Download credentials from Google Drive
        response = requests.get(GOOGLE_DRIVE_CREDENTIALS_URL)
        if response.status_code != 200:
            st.error("Failed to download Google credentials from Drive")
            return None

        credentials_dict = response.json()

        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/documents',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        return credentials
    except Exception as e:
        st.error(f"Error loading Google credentials: {e}")
        return None

def get_data_from_google_sheets(sheet_id, range_name="Sheet1"):
    """Fetch data from Google Sheets"""
    try:
        credentials = get_google_credentials()
        if credentials is None:
            return None

        from googleapiclient.discovery import build
        service = build('sheets', 'v4', credentials=credentials)

        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range_name
        ).execute()

        values = result.get('values', [])
        if not values:
            st.warning("No data found in Google Sheet")
            return None

        return values
    except Exception as e:
        st.error(f"Error fetching data from Google Sheets: {e}")
        return None

# Default configuration - will be overridden by Streamlit secrets
DEFAULT_CONFIG = {
    "GOOGLE_DRIVE_CREDENTIALS_URL": "",
    "PLA_SHEET_ID": "",
    "PCA_SHEET_ID": "",
    "GOOGLE_DOC_TEMPLATE_ID": ""
}