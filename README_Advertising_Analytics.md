# 📊 Advertising Performance Analytics Dashboard

A comprehensive Streamlit-based dashboard for analyzing advertising performance data with AI-powered insights and budget optimization.

## 🎯 Features

### Core Analytics
- **KPI Calculations**: CTR, Direct CVR, Indirect CVR, Direct ROI, Indirect ROI, ROAS
- **Interactive Visualizations**: Performance charts, ROI vs Spend analysis, scatter plots
- **Multi-level Analysis**: Brand, supercategory, and campaign-level insights

### Groundbreaking Budget Optimizer
- **Historical Performance Analysis**: Uses efficiency scores based on ROI and spend patterns
- **Smart Allocation**: Proportional budget distribution based on performance metrics
- **Expected Outcomes**: Predicts revenue and ROI for recommended budget allocations
- **Visual Optimization**: Treemaps and scatter plots for budget allocation visualization

### AI-Powered Insights
- **Automated Recommendations**: Top performers, underperformers, and optimization opportunities
- **Performance Alerts**: High spend/low ROI warnings and scaling opportunities
- **Strategic Insights**: Actionable recommendations for campaign optimization

### Data Integration
- **Google Sheets Export**: Automatically export analysis results to Google Sheets
- **Google Docs Reports**: Generate comprehensive insights reports in Google Docs
- **Real-time Data**: Fetches data directly from Google Sheets

## 🚀 Quick Deploy to Streamlit Cloud

### Step 1: GitHub Repository Setup
```bash
# Create a new GitHub repository
# Upload all files from this project

git add .
git commit -m "Initial commit: Advertising Analytics Dashboard"
git push origin main
```

### Step 2: Google Drive Configuration
```bash
# Run the setup script
python setup_google_drive.py
```

This will create the necessary configuration files and guide you through:
- Setting up Google Drive sharing for your service account JSON
- Creating Google Sheets for your data
- Configuring the secrets file

### Step 3: Deploy to Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Connect your GitHub repository
3. Select the main branch
4. Set the main file path to `app.py`
5. Add secrets in the Streamlit Cloud dashboard:

**For Google Drive (Recommended for Large Datasets):**
```toml
GOOGLE_DRIVE_CREDENTIALS_URL = "https://drive.google.com/uc?export=download&id=YOUR_JSON_FILE_ID"
GOOGLE_DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/YOUR_FOLDER_ID"
PLA_CSV_URL = "https://drive.google.com/uc?export=download&id=YOUR_PLA_CSV_FILE_ID"
PCA_CSV_URL = "https://drive.google.com/uc?export=download&id=YOUR_PCA_CSV_FILE_ID"
GOOGLE_DOC_TEMPLATE_ID = "YOUR_DOC_TEMPLATE_ID"  # Optional
```

**For Google Sheets (Limited to ~10M cells):**
```toml
PLA_SHEET_ID = "1xtKC7CRhOfczJzhMgnhatU_V1wxdj1BWfMs3ZbEhcjE"
PCA_SHEET_ID = "1RBQOOcwLuBW7PJiyrbAsVx0ToOQAd2hZyDyi-zXsWn4"
GOOGLE_DOC_TEMPLATE_ID = "YOUR_DOC_TEMPLATE_ID"  # Optional
```

### Step 4: Share with Everyone
- Your app will be live at: `https://your-username-your-repo-name.streamlit.app`
- Share this URL with anyone who needs access
- No installation required for users!

## 📋 Detailed Setup Instructions

### Prerequisites
```bash
pip install streamlit pandas numpy plotly google-auth google-api-python-client gspread requests
```

### Google Cloud Setup

1. **Create Service Account**:
   - Go to [Google Cloud Console](https://console.cloud.google.com)
   - Create a new project or select existing
   - Enable Google Sheets and Google Docs APIs
   - Create a service account with appropriate permissions
   - Download the JSON key file

2. **Upload Credentials to Google Drive**:
   - Upload your service account JSON file to Google Drive
   - Get shareable link: Right-click → "Get shareable link"
   - Set to "Anyone with the link can view"
   - Extract file ID from URL and create download link:
     ```
     https://drive.google.com/uc?export=download&id=YOUR_FILE_ID
     ```

3. **Create Data Sheets**:
   - Create Google Sheet for PLA data: "PLA Advertising Data"
   - Create Google Sheet for PCA data: "PCA Advertising Data"
   - Copy your CSV data into these sheets (including headers)
   - Share both sheets with your service account email
   - Get Sheet IDs from URLs

### Data Upload to Google Drive (Recommended for Large Datasets)
```bash
# Upload your CSV data to Google Drive (supports much larger files)
python upload_csv_to_drive.py
```

This creates a Google Drive folder and uploads your CSV files, then provides the direct download URLs for configuration.

**Benefits of Google Drive:**
- ✅ No row/cell limits (Google Sheets has ~10M cell limit)
- ✅ Supports very large CSV files
- ✅ Better performance with big datasets
- ✅ Direct CSV download (no API rate limits)

### Data Upload to Google Sheets (Fallback Method)
```bash
# Upload your CSV data to Google Sheets (limited to ~10M cells)
python upload_data_to_sheets.py
```

Use this method only if your data is small enough for Google Sheets.

### Local Development
```bash
# Clone your repository
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name

# Install dependencies
pip install -r requirements.txt

# Run locally
streamlit run app.py
```

## 🔐 Security & Access Control

### Data Security
- **Service Account**: Uses Google service account with minimal required permissions
- **Shared Access**: Data accessible only through shared Google Drive links
- **No Direct File Access**: No local file paths or credentials stored in code
- **Scalable Storage**: Google Drive supports much larger files than Google Sheets

### User Access
- **Public Access**: Anyone with the Streamlit Cloud URL can access
- **No Authentication**: No login required for simplicity
- **Read-Only**: Users can view and analyze data but cannot modify
- **Fast Loading**: Direct CSV downloads from Google Drive for better performance

## 📊 Data Architecture

### Data Storage Options

#### Google Drive (Recommended)
```
Advertising Analytics Data/
├── pla_advertising_data.csv (Brand Level)
│   ├── Headers: day_date, business_unit, brand, analytic_super_category, etc.
│   ├── Data rows with advertising metrics
│   └── No size limits - supports millions of rows
├── pca_advertising_data.csv (Campaign Level)
│   ├── Headers: day_date, brand, ad_account_id, super_category, etc.
│   ├── Data rows with campaign metrics
│   └── No size limits - supports millions of rows
└── Direct download URLs for fast access
```

#### Google Sheets (Limited)
```
PLA Advertising Data (Brand Level)
├── Headers: day_date, business_unit, brand, analytic_super_category, etc.
├── Data rows with advertising metrics
└── Limited to ~10M cells (~1M rows with 10 columns)

PCA Advertising Data (Campaign Level)
├── Headers: day_date, brand, ad_account_id, super_category, etc.
├── Data rows with campaign metrics
└── Limited to ~10M cells (~1M rows with 10 columns)
```

### Real-time Updates
- Data refreshes automatically from Google Sheets
- No manual data uploads required after initial setup
- Supports multiple users accessing the same data source

## 🎯 Usage Guide

### For End Users
1. **Access the Dashboard**: Visit the Streamlit Cloud URL
2. **Select Data Source**: Choose between PLA (Brand) or PCA (Campaign) data
3. **Explore Analytics**: Use the four tabs for different analysis types
4. **Budget Planning**: Enter your budget in the optimizer tab
5. **Export Results**: Download insights or export to Google Workspace

### For Administrators
1. **Update Data**: Upload new CSV files to Google Sheets
2. **Monitor Usage**: Check Streamlit Cloud analytics
3. **Add Features**: Push code updates to GitHub for auto-deployment
4. **User Support**: Share the public URL with team members

## 🛠️ Troubleshooting

### Common Issues

**"Unable to load data from Google Sheets"**
- Check if Google Sheets are shared with service account email
- Verify Sheet IDs in secrets configuration
- Ensure Google Drive credentials URL is accessible

**"No data available for analysis"**
- Confirm CSV data was properly uploaded to Google Sheets
- Check date ranges and filters
- Verify data format matches expected structure

**"Streamlit Cloud deployment failed"**
- Check secrets configuration in Streamlit Cloud dashboard
- Ensure all required dependencies are in requirements.txt
- Verify GitHub repository access

### Debug Mode
```bash
# Run with debug logging
streamlit run app.py --logger.level=debug
```

## 📈 Scaling & Performance

### Data Volume
- Supports millions of rows efficiently
- Automatic caching for improved performance
- Optimized queries for large datasets

### Concurrent Users
- Streamlit Cloud handles multiple simultaneous users
- Data caching prevents duplicate API calls
- Google Sheets rate limits may apply for high usage

### Feature Extensions
- Add new KPI calculations in the config
- Extend visualizations with additional chart types
- Integrate with other Google Workspace tools

## 🤝 Contributing

### Development Workflow
1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-feature`
3. Make changes and test locally
4. Push to GitHub: `git push origin feature/new-feature`
5. Create Pull Request

### Code Standards
- Follow PEP 8 Python style guide
- Add docstrings to all functions
- Include error handling and logging
- Test changes locally before deployment

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

### Getting Help
- Check the troubleshooting section above
- Review Streamlit Cloud logs for errors
- Open GitHub issues for bugs or feature requests
- Check Google API documentation for integration issues

### Resources
- [Streamlit Documentation](https://docs.streamlit.io)
- [Google Sheets API](https://developers.google.com/sheets/api)
- [Streamlit Cloud](https://share.streamlit.io)
- [Google Cloud Console](https://console.cloud.google.com)

---

## 🚀 Quick Start Checklist

- [ ] Create GitHub repository
- [ ] Set up Google Cloud service account
- [ ] Upload credentials to Google Drive
- [ ] Choose data storage method:
  - [ ] **Google Drive** (Recommended for large datasets)
    - [ ] Run `python upload_csv_to_drive.py`
    - [ ] Get folder and file URLs
  - [ ] **Google Sheets** (For smaller datasets)
    - [ ] Run `python upload_data_to_sheets.py`
    - [ ] Get Sheet IDs
- [ ] Configure Streamlit Cloud secrets
- [ ] Deploy application
- [ ] Share URL with team
- [ ] Test all features
- [ ] Set up monitoring

**Ready to deploy? Your advertising analytics dashboard will be live in minutes! 🎉**

## 📈 Key Metrics Explained

### Primary KPIs
- **CTR (Click-Through Rate)**: `clicks / total_views` - Measures ad engagement
- **Direct CVR (Conversion Rate)**: `direct_units / spend` - Measures direct sales efficiency
- **Indirect CVR**: `indirect_units / spend` - Measures indirect sales efficiency
- **Direct ROI**: `direct_revenue / spend` - Return on ad spend for direct sales
- **Indirect ROI**: `indirect_revenue / spend` - Return on ad spend for indirect sales
- **ROAS (Return on Ad Spend)**: `total_revenue / spend` - Overall advertising efficiency

### Performance Categories
- **Excellent ROI**: > 2.0x return on ad spend
- **Moderate ROI**: 1.0x - 2.0x return on ad spend
- **Poor ROI**: < 1.0x return on ad spend

## 💰 Budget Optimization Algorithm

### Efficiency Score Calculation
```
Efficiency_Score = ROI × log₁₀(1 + historical_spend)
```

### Budget Allocation Logic
1. **Historical Analysis**: Analyze past performance by brand/supercategory
2. **Efficiency Scoring**: Calculate efficiency scores based on ROI and spend patterns
3. **Proportional Allocation**: Distribute budget proportionally to efficiency scores
4. **Expected Outcomes**: Predict revenue and ROI based on historical performance

### Optimization Benefits
- **Data-Driven Decisions**: Based on actual historical performance
- **Risk Mitigation**: Reduces allocation to underperforming categories
- **Growth Focus**: Increases budget for high-ROI opportunities
- **Predictable Outcomes**: Provides expected revenue projections

## 🔍 AI Insights Engine

### Recommendation Types
- **🚀 Top Performers**: Categories with highest ROI for budget increases
- **⚠️ Underperformers**: Categories with lowest ROI for budget review
- **💰 High Spend Alerts**: Expensive campaigns with poor ROI
- **🎯 Scaling Opportunities**: High CTR campaigns ready for expansion

### Strategic Insights
- **Performance Health**: Overall campaign efficiency assessment
- **Engagement Analysis**: CTR and conversion pattern evaluation
- **Revenue Mix Analysis**: Direct vs indirect revenue optimization
- **Actionable Recommendations**: Specific steps for improvement

## 📊 Dashboard Tabs

### 1. 📈 Overview
- Key performance metrics summary
- Performance by brand and supercategory
- ROI vs spend scatter plot analysis

### 2. 🎯 Performance Analysis
- Custom metric selection and analysis
- Group-by functionality (brand, category, etc.)
- Top/bottom performer identification

### 3. 💰 Budget Optimizer
- Total budget input for upcoming campaigns
- Automated budget allocation recommendations
- Expected revenue and ROI projections
- Visual budget allocation displays

### 4. 🔍 Insights
- AI-generated recommendations and alerts
- Performance insights and strategic guidance
- Export capabilities to Google Workspace

## 🔗 Google Integration

### Setup Requirements
1. **Service Account**: Google Cloud service account with appropriate permissions
2. **API Credentials**: JSON key file stored at specified path
3. **API Access**: Enable Google Sheets and Docs APIs

### Export Features
- **Google Sheets**: Automated data export with formatting
- **Google Docs**: Comprehensive insights reports with recommendations
- **Sharing Links**: Direct links to created documents

## 🎨 Technical Architecture

### Data Processing
- **Efficient Loading**: Cached data loading with pandas
- **KPI Calculations**: Vectorized operations for performance
- **Memory Optimization**: Selective data processing for large datasets

### Visualization
- **Plotly Integration**: Interactive charts and graphs
- **Custom Styling**: Professional dashboard appearance
- **Responsive Design**: Optimized for different screen sizes

### AI/ML Components
- **Performance Clustering**: Automated categorization of campaigns
- **Trend Analysis**: Historical performance pattern recognition
- **Predictive Modeling**: Budget allocation optimization algorithms

## 🚀 Future Enhancements

### Planned Features
- **Predictive Analytics**: Machine learning models for performance forecasting
- **A/B Testing Analysis**: Statistical significance testing for campaign variations
- **Competitor Analysis**: Market benchmarking and competitive intelligence
- **Real-time Monitoring**: Live campaign performance tracking
- **Advanced Segmentation**: Customer behavior and demographic analysis

### Integration Opportunities
- **Marketing Platforms**: Direct integration with ad platforms (Facebook, Google Ads)
- **CRM Systems**: Customer data integration for enhanced insights
- **E-commerce Platforms**: Direct sales data integration
- **Business Intelligence**: Connection to enterprise BI tools

## 📝 Usage Tips

### For Best Results
1. **Data Quality**: Ensure CSV files are properly formatted and complete
2. **Regular Updates**: Refresh data regularly for current insights
3. **Budget Planning**: Use optimizer quarterly for campaign planning
4. **Action Implementation**: Track recommendation implementation impact

### Performance Optimization
- **Large Datasets**: Dashboard handles millions of rows efficiently
- **Caching**: Data loading is cached for fast subsequent access
- **Filtering**: Use date ranges and filters to focus analysis

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create feature branch
3. Implement enhancements
4. Add tests and documentation
5. Submit pull request

### Feature Requests
- Open issues with detailed feature descriptions
- Include use cases and expected benefits
- Provide sample data for testing

## 📄 License

This project is proprietary software. All rights reserved.

## 📞 Support

For support and questions:
- Check the troubleshooting section
- Review error messages and logs
- Ensure all prerequisites are met
- Verify data file formats and paths

---

*Built with ❤️ for data-driven advertising optimization*