@echo off
echo 🚀 Advertising Analytics Deployment Script
echo ==========================================
echo.

echo 📋 Step 1: Setting up GitHub repository...
if not exist .git (
    git init
    echo ✅ Git repository initialized
) else (
    echo ℹ️ Git repository already exists
)

echo.
echo 📤 Step 2: Adding files to Git...
git add .
git status

echo.
set /p commit_msg="Enter commit message (or press Enter for default): "
if "%commit_msg%"=="" set commit_msg="Initial deployment: Advertising Analytics Dashboard"

git commit -m "%commit_msg%"

echo.
echo 🔗 Step 3: GitHub repository setup instructions:
echo 1. Create a new repository on GitHub.com
echo 2. Copy the repository URL
echo 3. Run: git remote add origin YOUR_REPO_URL
echo 4. Run: git push -u origin main
echo.

echo ☁️ Step 4: Streamlit Cloud deployment:
echo 1. Go to https://share.streamlit.io
echo 2. Connect your GitHub repository
echo 3. Set main file path to: app.py
echo 4. Add secrets from .streamlit/secrets.toml
echo.

echo 🎯 Your app will be live at:
echo https://your-username-your-repo-name.streamlit.app
echo.

pause