#!/usr/bin/env python3
"""
Setup script for Advertising Analytics Google Drive Configuration

This script helps you configure Google Drive sharing for your analytics data.
"""

import json
import os
import sys

def create_google_drive_share_link():
    """Guide user to create Google Drive share link"""
    print("🔗 Google Drive Setup Instructions:")
    print("=" * 50)
    print()
    print("1. Upload your service account JSON file to Google Drive")
    print("2. Right-click the file in Google Drive")
    print("3. Click 'Get shareable link'")
    print("4. Set permissions to 'Anyone with the link can view'")
    print("5. Copy the link and extract the file ID")
    print()
    print("Example link format:")
    print("https://drive.google.com/file/d/1ABC123...XYZ/view?usp=sharing")
    print()
    print("File ID is: 1ABC123...XYZ")
    print()
    print("Direct download URL format:")
    print("https://drive.google.com/uc?export=download&id=1ABC123...XYZ")
    print()

def create_google_sheets_for_data():
    """Guide user to create Google Sheets for data"""
    print("📊 Google Sheets Setup Instructions:")
    print("=" * 50)
    print()
    print("1. Create a new Google Sheet")
    print("2. Name it 'PLA Advertising Data' for brand-level data")
    print("3. Create another sheet named 'PCA Advertising Data' for campaign-level data")
    print("4. Copy your CSV data into these sheets (including headers)")
    print("5. Share both sheets with your service account email")
    print("6. Copy the Sheet IDs from the URLs")
    print()
    print("Sheet URL format:")
    print("https://docs.google.com/spreadsheets/d/1DEF456...UVW/edit")
    print()
    print("Sheet ID is: 1DEF456...UVW")
    print()

def generate_secrets_template():
    """Generate secrets template"""
    template = '''# Google API Configuration for Advertising Analytics
# Replace the empty values with your actual configuration

# Google Drive direct download link for service account JSON
# Format: https://drive.google.com/uc?export=download&id=YOUR_FILE_ID
GOOGLE_DRIVE_CREDENTIALS_URL = "https://drive.google.com/uc?export=download&id=YOUR_JSON_FILE_ID"

# Google Sheets IDs for data
PLA_SHEET_ID = "YOUR_PLA_SHEET_ID"
PCA_SHEET_ID = "YOUR_PCA_SHEET_ID"

# Google Doc template ID for insights export (optional)
GOOGLE_DOC_TEMPLATE_ID = ""
'''

    with open('.streamlit/secrets.toml', 'w') as f:
        f.write(template)

    print("✅ Created .streamlit/secrets.toml template")
    print("   Edit this file with your actual Google Drive links and Sheet IDs")

def main():
    print("🚀 Advertising Analytics - Google Drive Setup")
    print("=" * 50)
    print()

    print("This setup will help you configure Google Drive sharing for your analytics data.")
    print("You'll need:")
    print("- Google service account JSON file")
    print("- Google account with Drive access")
    print("- Your advertising data in CSV format")
    print()

    input("Press Enter to continue...")

    # Create secrets template
    generate_secrets_template()

    # Show Google Drive setup
    create_google_drive_share_link()

    # Show Google Sheets setup
    create_google_sheets_for_data()

    print("🎯 Next Steps:")
    print("=" * 50)
    print()
    print("1. Upload your service account JSON to Google Drive")
    print("2. Get the shareable link and update GOOGLE_DRIVE_CREDENTIALS_URL")
    print("3. Create Google Sheets and upload your data")
    print("4. Update PLA_SHEET_ID and PCA_SHEET_ID")
    print("5. Test the application locally: streamlit run app.py")
    print("6. Deploy to Streamlit Cloud")
    print()

    print("🔗 Useful Links:")
    print("- Streamlit Cloud: https://share.streamlit.io")
    print("- Google Cloud Console: https://console.cloud.google.com")
    print("- Google Drive: https://drive.google.com")
    print()

if __name__ == "__main__":
    main()