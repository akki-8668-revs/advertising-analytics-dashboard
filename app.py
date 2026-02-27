import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import warnings
warnings.filterwarnings('ignore')

# Try to import gdown for Google Drive downloads
try:
    import gdown
    HAS_GDOWN = True
except ImportError:
    HAS_GDOWN = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    HAS_DRIVE_API = True
except ImportError:
    HAS_DRIVE_API = False

# BSD dates: 6-12 March (Fri to Thu)
BSD_DAYS = ['Fri 06-Mar', 'Sat 07-Mar', 'Sun 08-Mar', 'Mon 09-Mar', 'Tue 10-Mar', 'Wed 11-Mar', 'Thu 12-Mar']

# BU-Super Category phasing (7 days: Fri-Sun)
PHASING_DATA = {
    ('Electronics', 'Audio'): [0.229, 0.226, 0.069, 0.173, 0.182, 0.058, 0.064],
    ('Electronics', 'Automobile'): [0.203, 0.213, 0.068, 0.183, 0.194, 0.066, 0.073],
    ('Electronics', 'Camera'): [0.223, 0.219, 0.071, 0.180, 0.182, 0.059, 0.066],
    ('Electronics', 'DigitalVoucherCode'): [0.233, 0.225, 0.072, 0.174, 0.181, 0.054, 0.061],
    ('Electronics', 'Electronics'): [0.225, 0.218, 0.069, 0.177, 0.185, 0.059, 0.066],
    ('Electronics', 'ExtendedWarrantyNew'): [0.220, 0.217, 0.069, 0.182, 0.186, 0.059, 0.066],
    ('Electronics', 'Gaming'): [0.250, 0.244, 0.077, 0.156, 0.160, 0.053, 0.059],
    ('Electronics', 'IOT'): [0.223, 0.221, 0.072, 0.178, 0.185, 0.058, 0.064],
    ('Electronics', 'ITAccessory'): [0.211, 0.217, 0.069, 0.180, 0.194, 0.061, 0.068],
    ('Electronics', 'ITPeripherals'): [0.202, 0.205, 0.068, 0.191, 0.201, 0.063, 0.070],
    ('Electronics', 'LaptopAndDesktop'): [0.250, 0.235, 0.079, 0.166, 0.160, 0.052, 0.058],
    ('Electronics', 'MobileProtection'): [0.243, 0.240, 0.073, 0.161, 0.169, 0.054, 0.060],
    ('Electronics', 'PersonalHealthCare'): [0.225, 0.219, 0.070, 0.175, 0.183, 0.060, 0.067],
    ('Electronics', 'PowerBank'): [0.278, 0.214, 0.063, 0.149, 0.165, 0.062, 0.069],
    ('Electronics', 'RestOfMobileAccessory'): [0.272, 0.234, 0.065, 0.149, 0.164, 0.055, 0.061],
    ('Electronics', 'SHA'): [0.225, 0.224, 0.072, 0.176, 0.183, 0.057, 0.064],
    ('Electronics', 'Service'): [0.215, 0.215, 0.068, 0.179, 0.187, 0.064, 0.072],
    ('Electronics', 'Storage'): [0.222, 0.208, 0.065, 0.180, 0.189, 0.065, 0.072],
    ('Electronics', 'Tablet'): [0.226, 0.209, 0.070, 0.181, 0.187, 0.061, 0.068],
    ('Electronics', 'Video'): [0.212, 0.224, 0.073, 0.179, 0.184, 0.061, 0.068],
    ('LargeAppliances', 'AirConditioner'): [0.222, 0.256, 0.041, 0.201, 0.206, 0.033, 0.041],
    ('LargeAppliances', 'AppliancePasses'): [0.244, 0.259, 0.046, 0.188, 0.189, 0.030, 0.043],
    ('LargeAppliances', 'AppliancesService'): [0.254, 0.254, 0.047, 0.194, 0.186, 0.027, 0.038],
    ('LargeAppliances', 'CoreEA'): [0.236, 0.256, 0.051, 0.202, 0.184, 0.029, 0.042],
    ('LargeAppliances', 'HomeEntertainmentLarge'): [0.254, 0.244, 0.044, 0.197, 0.202, 0.026, 0.033],
    ('LargeAppliances', 'LargeAppliances'): [0.221, 0.247, 0.044, 0.206, 0.208, 0.033, 0.041],
    ('LargeAppliances', 'Microwave'): [0.207, 0.404, 0.038, 0.154, 0.149, 0.021, 0.026],
    ('LargeAppliances', 'PremiumEA'): [0.254, 0.283, 0.048, 0.180, 0.173, 0.025, 0.036],
    ('LargeAppliances', 'Refrigerator'): [0.248, 0.233, 0.041, 0.191, 0.212, 0.034, 0.042],
    ('LargeAppliances', 'SeasonalEA'): [0.209, 0.250, 0.044, 0.201, 0.205, 0.038, 0.054],
    ('LargeAppliances', 'WashingMachineDryer'): [0.215, 0.254, 0.043, 0.188, 0.223, 0.034, 0.042],
}

# Base curves for fallback when (BU, SuperCat) not in PHASING_DATA
ELEC_BASE = [0.22, 0.22, 0.07, 0.18, 0.19, 0.06, 0.06]
APP_BASE = [0.23, 0.25, 0.045, 0.20, 0.20, 0.035, 0.04]

URGENCY_CATEGORIES = ['Gaming', 'LaptopAndDesktop', 'RestOfMobileAccessory', 'PowerBank', 'MobileProtection']
DELIBERATION_CATEGORIES = ['AirConditioner', 'Refrigerator', 'HomeEntertainmentLarge', 'WashingMachineDryer', 'LargeAppliances']
URGENCY_MULT = [1.15, 1.10, 1.05, 0.85, 0.85, 0.90, 0.90]
DELIB_MULT = [0.90, 0.95, 1.00, 1.15, 1.20, 1.05, 1.05]

st.set_page_config(page_title="Spend Pulse | BSD Budget Optimizer", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
.main-header { font-size: 2rem; font-weight: bold; color: #1f77b4; text-align: center; margin-bottom: 1.5rem; }
.metric-card { background-color: #f0f2f6; padding: 0.8rem; border-radius: 8px; border-left: 4px solid #1f77b4; margin: 0.4rem 0; }
.recommendation-box { background-color: #e8f4f8; padding: 0.8rem; border-radius: 8px; border-left: 4px solid #17becf; margin: 0.4rem 0; }
.warning-box { background-color: #fff3cd; padding: 0.8rem; border-radius: 8px; border-left: 4px solid #ffc107; margin: 0.4rem 0; }
.spend-pulse-box { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1.25rem; border-radius: 12px; margin: 1rem 0; box-shadow: 0 4px 15px rgba(102,126,234,0.4); }
.spend-pulse-box h3 { color: white !important; margin: 0 0 0.5rem 0 !important; font-size: 1.25rem !important; }
.spend-pulse-box p { color: rgba(255,255,255,0.9) !important; margin: 0 0 1rem 0 !important; font-size: 0.9rem !important; }
.spend-pulse-table { background: white; border-radius: 8px; padding: 1rem; overflow-x: auto; }
.spend-pulse-table table { width: 100%; border-collapse: collapse; font-size: 0.95rem; }
.spend-pulse-table th { background: #667eea; color: white; padding: 0.6rem 0.8rem; text-align: left; border-radius: 6px 6px 0 0; }
.spend-pulse-table td { padding: 0.5rem 0.8rem; border-bottom: 1px solid #eee; }
.spend-pulse-table tr:hover { background: #f8f9ff; }
</style>
""", unsafe_allow_html=True)

# --- DATA LOADING ---
def _load_from_drive_api_by_name(folder_id, name_patterns):
    if not HAS_DRIVE_API:
        return None
    try:
        creds_json = None
        if hasattr(st, 'secrets') and st.secrets:
            creds_json = st.secrets.get("GCP_CREDENTIALS_JSON")
            if isinstance(creds_json, str):
                import json
                creds_json = json.loads(creds_json)
        if not creds_json:
            return None
        creds = service_account.Credentials.from_service_account_info(
            creds_json, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(q=f"'{folder_id}' in parents and trashed = false", fields="files(id, name)").execute()
        for f in results.get('files', []):
            if f['name'].lower().endswith('.csv') and all(p in f['name'].lower() for p in name_patterns):
                buf = io.BytesIO()
                downloader = MediaIoBaseDownload(buf, service.files().get_media(fileId=f['id']))
                while not downloader.next_chunk()[1]:
                    pass
                buf.seek(0)
                return pd.read_csv(buf)
    except Exception:
        pass
    return None

def _download_from_google_drive(url_or_id):
    if not url_or_id or not isinstance(url_or_id, str):
        return None
    url_or_id = url_or_id.strip()
    m = re.search(r'[?&]id=([a-zA-Z0-9_-]+)', url_or_id)
    if m:
        file_id = m.group(1)
    else:
        m2 = re.search(r'/d/([a-zA-Z0-9_-]+)', url_or_id)
        file_id = m2.group(1) if m2 else (url_or_id if not url_or_id.startswith('http') else None)
    if not file_id:
        return None
    if HAS_GDOWN:
        try:
            import tempfile, os
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                gdown.download(f"https://drive.google.com/uc?id={file_id}", tmp.name, quiet=True, fuzzy=True)
                with open(tmp.name, 'rb') as f:
                    data = f.read()
                os.unlink(tmp.name)
                return data
        except Exception:
            pass
    if HAS_REQUESTS:
        try:
            r = requests.get(f"https://drive.google.com/uc?export=download&id={file_id}", stream=True)
            for k, v in r.cookies.items():
                if 'download_warning' in k.lower():
                    r = requests.get(f"https://drive.google.com/uc?export=download&id={file_id}", params={'confirm': v}, stream=True)
                    break
            r.raise_for_status()
            return r.content
        except Exception:
            pass
    return None

def _process_pla_df(df):
    if 'day_date' in df.columns:
        df['day_date'] = pd.to_datetime(df['day_date'], format='%d-%m-%Y', errors='coerce')
    for col in ['unique_views', 'clicks', 'spend', 'atc', 'total_views', 'listings', 'direct_units', 'indirect_units', 'direct_rev', 'indirect_rev']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def _process_pca_df(df):
    if 'day_date' in df.columns:
        df['day_date'] = pd.to_datetime(df['day_date'], format='%d-%m-%Y', errors='coerce')
    if 'ad_spend' in df.columns and 'adspend' not in df.columns:
        df['adspend'] = pd.to_numeric(df['ad_spend'], errors='coerce').fillna(0)
    if 'view_count' in df.columns and 'viewcount' not in df.columns:
        df['viewcount'] = pd.to_numeric(df['view_count'], errors='coerce').fillna(0)
    for col in ['viewcount', 'clicks', 'adspend', 'direct_units', 'indirect_units', 'ppv', 'direct_rev', 'indirect_rev']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

@st.cache_data
def load_pla_data():
    folder_id = st.secrets.get("GOOGLE_DRIVE_FOLDER_ID") if hasattr(st, 'secrets') and st.secrets else None
    if not folder_id and hasattr(st, 'secrets') and st.secrets.get("GOOGLE_DRIVE_FOLDER_URL"):
        m = re.search(r'/folders/([a-zA-Z0-9_-]+)', str(st.secrets.get("GOOGLE_DRIVE_FOLDER_URL", "")))
        folder_id = m.group(1) if m else None
    if folder_id:
        df = _load_from_drive_api_by_name(folder_id, ['pla', 'onetim'])
        if df is not None:
            return _process_pla_df(df)
    pla_url = st.secrets.get("PLA_CSV_URL") or st.secrets.get("PLA_FILE_ID") if hasattr(st, 'secrets') and st.secrets else None
    if pla_url:
        data = _download_from_google_drive(pla_url)
        if data:
            return _process_pla_df(pd.read_csv(io.BytesIO(data)))
    st.error("Google Drive not configured. Add GOOGLE_DRIVE_FOLDER_ID and GCP_CREDENTIALS_JSON to Streamlit Secrets.")
    return None

@st.cache_data
def load_pca_data():
    folder_id = st.secrets.get("GOOGLE_DRIVE_FOLDER_ID") if hasattr(st, 'secrets') and st.secrets else None
    if not folder_id and hasattr(st, 'secrets') and st.secrets.get("GOOGLE_DRIVE_FOLDER_URL"):
        m = re.search(r'/folders/([a-zA-Z0-9_-]+)', str(st.secrets.get("GOOGLE_DRIVE_FOLDER_URL", "")))
        folder_id = m.group(1) if m else None
    if folder_id:
        df = _load_from_drive_api_by_name(folder_id, ['pca', 'onetim'])
        if df is not None:
            return _process_pca_df(df)
    pca_url = st.secrets.get("PCA_CSV_URL") or st.secrets.get("PCA_FILE_ID") if hasattr(st, 'secrets') and st.secrets else None
    if pca_url:
        data = _download_from_google_drive(pca_url)
        if data:
            return _process_pca_df(pd.read_csv(io.BytesIO(data)))
    st.error("Google Drive not configured.")
    return None

# --- KPI CALCULATIONS ---
def calculate_pla_kpis(df):
    df = df.copy()
    df['CTR'] = np.where(df['total_views'] > 0, df['clicks'] / df['total_views'], 0)
    df['Direct_CVR'] = np.where(df['spend'] > 0, df['direct_units'] / df['spend'], 0)
    df['Indirect_CVR'] = np.where(df['spend'] > 0, df['indirect_units'] / df['spend'], 0)
    df['Direct_ROI'] = np.where(df['spend'] > 0, df['direct_rev'] / df['spend'], 0)
    df['Indirect_ROI'] = np.where(df['spend'] > 0, df['indirect_rev'] / df['spend'], 0)
    df['Total_ROI'] = np.where(df['spend'] > 0, (df['direct_rev'] + df['indirect_rev']) / df['spend'], 0)
    df['Total_Revenue'] = df['direct_rev'] + df['indirect_rev']
    df['Total_Units'] = df['direct_units'] + df['indirect_units']
    return df

def calculate_pca_kpis(df):
    df = df.copy()
    df['CTR'] = np.where(df['viewcount'] > 0, df['clicks'] / df['viewcount'], 0)
    df['Direct_CVR'] = np.where(df['adspend'] > 0, df['direct_units'] / df['adspend'], 0)
    df['Indirect_CVR'] = np.where(df['adspend'] > 0, df['indirect_units'] / df['adspend'], 0)
    df['Direct_ROI'] = np.where(df['adspend'] > 0, df['direct_rev'] / df['adspend'], 0)
    df['Indirect_ROI'] = np.where(df['adspend'] > 0, df['indirect_rev'] / df['adspend'], 0)
    df['Total_ROI'] = np.where(df['adspend'] > 0, (df['direct_rev'] + df['indirect_rev']) / df['adspend'], 0)
    df['Total_Revenue'] = df['direct_rev'] + df['indirect_rev']
    df['Total_Units'] = df['direct_units'] + df['indirect_units']
    return df

# --- TRAFFIC PHASING ---
def get_phasing_for_bu_sc(business_unit, super_category):
    bu = str(business_unit).strip()
    if bu in ('Large', 'Large Appliances'):
        bu = 'LargeAppliances'
    key = (bu, str(super_category).strip())
    if key in PHASING_DATA:
        return PHASING_DATA[key]
    bu_str = str(business_unit)
    base = ELEC_BASE if 'Electronics' in bu_str else (APP_BASE if any(x in bu_str for x in ['Large', 'LargeAppliances', 'Large Appliances']) else ELEC_BASE)
    if super_category in URGENCY_CATEGORIES:
        mult = URGENCY_MULT
    elif super_category in DELIBERATION_CATEGORIES:
        mult = DELIB_MULT
    else:
        mult = [1.0] * 7
    adjusted = [base[i] * mult[i] for i in range(7)]
    adjusted[6] = adjusted[5] * 0.85
    total = sum(adjusted)
    return [a / total for a in adjusted]

def calculate_traffic_phasing(base_volume, category_name, business_unit, base_curve_percentages=None):
    pct = get_phasing_for_bu_sc(business_unit, category_name)
    return pct, [base_volume * x for x in pct]

def compute_day_level_budgets(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca):
    """
    Apply day-level phasing to allocation using BU-Super Category curves from PHASING_DATA.
    Returns DataFrame with Day, PLA_Budget, PCA_Budget, Total_Budget.
    """
    day_budgets = {d: {'PLA': 0.0, 'PCA': 0.0} for d in BSD_DAYS}
    cat_col = 'analytic_super_category' if 'analytic_super_category' in allocation_df.columns else 'super_category'
    for _, row in allocation_df.iterrows():
        try:
            fmt = str(row.get('Format', 'PLA') or 'PLA')
            budget = row.get('Recommended_Budget', row.get('Budget (₹)', 0))
            if pd.isna(budget) or budget <= 0:
                continue
            budget = float(budget)
            category = str(row.get(cat_col) or row.get('super_category') or row.get('Category') or 'Gaming').strip() or 'Gaming'
            if sel_bu != 'All':
                bu_val = str(sel_bu)
            else:
                src = df_pla if fmt == 'PLA' and df_pla is not None and not df_pla.empty else df_pca
                src = src if src is not None and not src.empty else None
                bu_val = 'Electronics'
                if src is not None and bu_col and bu_col in src.columns and 'brand' in src.columns:
                    sc_col_src = 'analytic_super_category' if 'analytic_super_category' in src.columns else 'super_category'
                    if sc_col_src in src.columns:
                        try:
                            match = src[(src['brand'].astype(str) == str(row.get('brand', ''))) & (src[sc_col_src].astype(str) == category)]
                            if not match.empty and bu_col in match.columns and match[bu_col].notna().any():
                                bu_val = str(match[bu_col].mode().iloc[0])
                        except Exception:
                            pass
            phasing = get_phasing_for_bu_sc(bu_val, category)
            for i, day in enumerate(BSD_DAYS):
                day_budgets[day][fmt] += budget * phasing[i]
        except Exception:
            continue
    rows = []
    for day in BSD_DAYS:
        pla_b = day_budgets[day]['PLA']
        pca_b = day_budgets[day]['PCA']
        tot = pla_b + pca_b
        rows.append({'Day': day, 'PLA Spend (₹)': round(pla_b, 2), 'PCA Spend (₹)': round(pca_b, 2), 'Total Spend (₹)': round(tot, 2),
                     'PLA %': f"{(pla_b/tot*100):.1f}%" if tot > 0 else "0%",
                     'PCA %': f"{(pca_b/tot*100):.1f}%" if tot > 0 else "0%"})
    return pd.DataFrame(rows)

def expand_allocation_to_daily(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca, include_pla_detail=True):
    """
    Expand allocation to day-level with page_context, slot for PLA. Uses BU-Super Category phasing.
    """
    cat_col = 'analytic_super_category' if 'analytic_super_category' in allocation_df.columns else 'super_category'
    daily_tables = []
    for day_idx, day_name in enumerate(BSD_DAYS):
        day_rows = []
        for _, row in allocation_df.iterrows():
            try:
                fmt = str(row.get('Format', 'PLA') or 'PLA')
                budget = row.get('Recommended_Budget', row.get('Budget (₹)', 0))
                if pd.isna(budget) or budget <= 0:
                    continue
                budget = float(budget)
                category = str(row.get(cat_col) or row.get('super_category') or row.get('Category') or '').strip() or 'Gaming'
                if sel_bu != 'All':
                    bu_val = str(sel_bu)
                else:
                    src = df_pla if fmt == 'PLA' and df_pla is not None and not df_pla.empty else df_pca
                    src = src if src is not None and not src.empty else None
                    bu_val = 'Electronics'
                    if src is not None and bu_col and bu_col in src.columns and 'brand' in src.columns:
                        sc_col_src = 'analytic_super_category' if 'analytic_super_category' in src.columns else 'super_category'
                        if sc_col_src in src.columns:
                            try:
                                match = src[(src['brand'].astype(str) == str(row.get('brand', ''))) & (src[sc_col_src].astype(str) == category)]
                                if not match.empty and bu_col in match.columns and match[bu_col].notna().any():
                                    bu_val = str(match[bu_col].mode().iloc[0])
                            except Exception:
                                pass
                phasing = get_phasing_for_bu_sc(bu_val, category)
                day_budget = budget * phasing[day_idx]
                r = {'Day': day_name, 'Format': fmt, 'Brand': row.get('brand', row.get('Brand', '')), 'Category': category,
                     'Budget (₹)': round(day_budget, 2)}
                if include_pla_detail and 'page_context' in allocation_df.columns:
                    r['Page Context'] = str(row.get('page_context', ''))
                if include_pla_detail and 'slot_type' in allocation_df.columns:
                    r['Slot Type'] = str(row.get('slot_type', ''))
                day_rows.append(r)
            except Exception:
                continue
        daily_tables.append(pd.DataFrame(day_rows) if day_rows else pd.DataFrame())
    return daily_tables

# --- BUDGET OPTIMIZATION ---
def optimize_budget(df, total_budget, data_type, kpi_col, group_cols_extra=None):
    spend_col = 'spend' if data_type == 'pla' else ('adspend' if 'adspend' in df.columns else 'ad_spend')
    group_cols = ['brand', 'analytic_super_category'] if data_type == 'pla' else ['brand', 'super_category']
    if group_cols_extra:
        group_cols = group_cols + [c for c in group_cols_extra if c in df.columns]
    if kpi_col not in df.columns:
        kpi_col = 'Total_ROI'
    agg_d = {spend_col: 'sum', kpi_col: 'mean'}
    if 'Total_Revenue' in df.columns:
        agg_d['Total_Revenue'] = 'sum'
    if 'Total_Units' in df.columns:
        agg_d['Total_Units'] = 'sum'
    group_cols = [c for c in group_cols if c in df.columns]
    if not group_cols:
        return pd.DataFrame()
    perf = df.groupby(group_cols).agg(agg_d).reset_index()
    if 'Total_Revenue' not in perf.columns:
        perf['Total_Revenue'] = 0
    if 'Total_Units' not in perf.columns:
        perf['Total_Units'] = 0
    perf['Efficiency_Score'] = perf[kpi_col] * np.log1p(perf[spend_col])
    perf['Efficiency_Score'] = perf['Efficiency_Score'].fillna(0)
    if perf['Efficiency_Score'].max() > 0:
        perf['Efficiency_Score'] = perf['Efficiency_Score'] / perf['Efficiency_Score'].max()
    tot_eff = perf['Efficiency_Score'].sum()
    perf['Recommended_Budget'] = (perf['Efficiency_Score'] / tot_eff * total_budget) if tot_eff > 0 else (total_budget / len(perf))
    # ROI = Revenue / Spend. Expected revenue at historical efficiency.
    perf['Expected_Revenue'] = np.where(perf[spend_col] > 0, perf['Total_Revenue'] / perf[spend_col] * perf['Recommended_Budget'], 0)
    perf['Expected_ROI'] = np.where(perf['Recommended_Budget'] > 0, perf['Expected_Revenue'] / perf['Recommended_Budget'], 0)
    return perf.sort_values('Efficiency_Score', ascending=False)

# --- MAIN ---
def main():
    try:
        _main()
    except Exception as e:
        st.error(f"An error occurred. Please refresh. Details: {str(e)}")
        st.stop()

def _main():
    st.markdown('<p style="text-align:center;color:#888;font-size:0.75rem;margin-bottom:0.25rem;">For Internal Use Only</p>', unsafe_allow_html=True)
    st.markdown('<div class="main-header">⚡ Spend Pulse — BSD Budget Optimizer</div>', unsafe_allow_html=True)

    pla_df = load_pla_data()
    pca_df = load_pca_data()
    if pla_df is None and pca_df is None:
        return

    pla_df = calculate_pla_kpis(pla_df) if pla_df is not None else None
    pca_df = calculate_pca_kpis(pca_df) if pca_df is not None else None

    # Filter: Electronics and Large Appliances only (no Mobile). Include Large, LargeAppliances.
    bu_col = 'business_unit' if 'business_unit' in (pla_df.columns if pla_df is not None else []) else 'analytic_vertical'
    if bu_col not in (pla_df.columns if pla_df is not None else []) and pca_df is not None:
        bu_col = 'business_unit' if 'business_unit' in pca_df.columns else None
    allowed_bu = ['Electronics', 'LargeAppliances', 'Large', 'Large Appliances']
    if pla_df is not None and bu_col and bu_col in pla_df.columns:
        pla_df = pla_df[pla_df[bu_col].astype(str).str.strip().isin(allowed_bu)]
    if pca_df is not None and bu_col and bu_col in pca_df.columns:
        pca_df = pca_df[pca_df[bu_col].astype(str).str.strip().isin(allowed_bu)]

    tab1, tab2, tab3 = st.tabs(["⚡ Budget Optimizer", "🔄 Backward Calculator", "📅 Day-Level Split"])

    # Build filter options from available data
    df_ref = pla_df if pla_df is not None else pca_df
    if df_ref is None:
        st.error("No data available.")
        return

    bu_col_opt = 'business_unit' if 'business_unit' in df_ref.columns else ('analytic_vertical' if 'analytic_vertical' in df_ref.columns else None)
    sc_col_opt = 'analytic_super_category' if 'analytic_super_category' in df_ref.columns else ('super_category' if 'super_category' in df_ref.columns else None)

    with st.sidebar.expander("📖 Where to click & what to find", expanded=False):
        st.markdown("""
**Budget Optimizer** — Enter budget → Click **Optimize** → allocation table

**Backward Calculator** — Select KPI, enter target → Click **Calculate** → required budget, allocation

**Day-Level Split** — 7-day PLA/PCA spend (from Budget Optimizer or Backward Calculator)
        """)
    st.sidebar.subheader("Filters (Budget Optimizer)")
    sel_bu = st.sidebar.selectbox("BU", ['All'] + sorted(df_ref[bu_col_opt].dropna().astype(str).unique().tolist())) if bu_col_opt and bu_col_opt in df_ref.columns else 'All'
    sel_brand = st.sidebar.selectbox("Brand", ['All'] + sorted(df_ref['brand'].dropna().astype(str).unique().tolist()))
    sel_sc = st.sidebar.selectbox("Super Category", ['All'] + sorted(df_ref[sc_col_opt].dropna().astype(str).unique().tolist())) if sc_col_opt and sc_col_opt in df_ref.columns else 'All'

    def apply_filters(df):
        if df is None or df.empty:
            return df
        out = df.copy()
        if bu_col_opt and bu_col_opt in out.columns and sel_bu != 'All':
            out = out[out[bu_col_opt].astype(str).str.strip() == sel_bu]
        if sel_brand != 'All':
            out = out[out['brand'].astype(str).str.strip() == sel_brand]
        if sc_col_opt and sc_col_opt in out.columns and sel_sc != 'All':
            out = out[out[sc_col_opt].astype(str).str.strip() == sel_sc]
        return out

    pla_f = apply_filters(pla_df) if pla_df is not None else None
    pca_f = apply_filters(pca_df) if pca_df is not None else None

    with tab1:
        st.subheader("Budget Optimizer (BSD 6–12 March)")
        st.caption("Uses full historical data. No date filter. Local filters: BU, Brand, Super Category.")

        total_budget = st.number_input("Total Budget (₹)", min_value=10000, max_value=10000000, value=100000, step=10000)
        kpi_options = [c for c in ['Total_ROI', 'Direct_ROI', 'Indirect_ROI', 'CTR', 'Direct_CVR', 'Indirect_CVR'] if c in df_ref.columns]
        selected_kpi = st.selectbox("Optimize for KPI", kpi_options or ['Total_ROI'])

        # PLA/PCA split by historical spend ratio
        pla_hist = pla_f['spend'].sum() if pla_f is not None and not pla_f.empty and 'spend' in pla_f.columns else 0
        pca_hist = pca_f['adspend'].sum() if pca_f is not None and not pca_f.empty and 'adspend' in pca_f.columns else 0
        tot_hist = pla_hist + pca_hist
        pla_share = pla_hist / tot_hist if tot_hist > 0 else 0.5
        pca_share = 1.0 - pla_share
        pla_budget = total_budget * pla_share
        pca_budget = total_budget * pca_share

        if st.button("🚀 Optimize"):
            results = []
            if pla_f is not None and not pla_f.empty and pla_budget > 0:
                pla_res = optimize_budget(pla_f, pla_budget, 'pla', selected_kpi, ['page_context', 'slot_type'] if all(c in pla_f.columns for c in ['page_context', 'slot_type']) else None)
                if not pla_res.empty:
                    pla_res = pla_res.copy()
                    pla_res['Format'] = 'PLA'
                    results.append(pla_res)
            if pca_f is not None and not pca_f.empty and pca_budget > 0:
                pca_res = optimize_budget(pca_f, pca_budget, 'pca', selected_kpi)
                if not pca_res.empty:
                    pca_res = pca_res.copy()
                    pca_res['Format'] = 'PCA'
                    results.append(pca_res)

            if not results:
                st.warning("No allocation possible.")
                return

            combined = pd.concat(results, ignore_index=True)
            total_rec = combined['Recommended_Budget'].sum()
            total_rev = combined['Expected_Revenue'].sum()
            avg_roi = total_rev / total_rec if total_rec > 0 else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Recommended Budget", f"₹{total_rec:,.2f}")
            col2.metric("Expected Revenue", f"₹{total_rev:,.2f}")
            col3.metric("Expected ROI (Revenue/Spend)", f"{avg_roi:.2f}")

            combined_copy = combined.copy()
            if 'Format' not in combined_copy.columns:
                combined_copy['Format'] = 'PLA'
            day_phasing_df = compute_day_level_budgets(combined_copy, sel_bu, bu_col_opt, sc_col_opt, pla_f, pca_f)
            try:
                st.session_state['day_level_optimizer'] = day_phasing_df.copy()
            except Exception:
                pass

            cat_col = 'analytic_super_category' if 'analytic_super_category' in combined.columns else 'super_category'
            base_cols = ['Format', 'brand', cat_col]
            if 'page_context' in combined.columns:
                base_cols.append('page_context')
            if 'slot_type' in combined.columns:
                base_cols.append('slot_type')
            base_cols += ['Recommended_Budget', 'Expected_Revenue', 'Expected_ROI', 'Efficiency_Score']
            disp = combined[[c for c in base_cols if c in combined.columns]].copy()
            disp = disp.rename(columns={'brand': 'Brand', cat_col: 'Category', 'page_context': 'Page Context', 'slot_type': 'Slot Type',
                                       'Recommended_Budget': 'Budget (₹)', 'Expected_Revenue': 'Expected Revenue (₹)', 'Expected_ROI': 'Expected ROI', 'Efficiency_Score': 'Efficiency Score'})
            st.dataframe(disp.round(2), use_container_width=True)

            try:
                daily_tables = expand_allocation_to_daily(combined_copy, sel_bu, bu_col_opt, sc_col_opt, pla_f, pca_f)
                with st.expander("📋 Day-wise PLA/PCA allocation (by page context & slot)", expanded=False):
                    for i, day_name in enumerate(BSD_DAYS):
                        if i < len(daily_tables) and not daily_tables[i].empty:
                            st.markdown(f"**{day_name}**")
                            st.dataframe(daily_tables[i], use_container_width=True, hide_index=True)
            except Exception:
                pass

    with tab2:
        st.subheader("🔄 Backward Budget Calculator")
        st.caption("Enter a KPI and target value → get required PLA (page context, slot) and PCA budget.")

        kpi_bw = st.selectbox("Select KPI", ['Total_ROI', 'Direct_ROI', 'Indirect_ROI', 'Total_Revenue', 'Total_Units', 'Direct_CVR', 'Indirect_CVR'], key='bw_kpi')
        target_value = st.number_input("Target Value", min_value=0.0, value=2.0, step=0.1, format="%.2f", key='bw_target',
                                       help="ROI: target ROI | Revenue: ₹ | Units: count | CVR: units per ₹ spend")
        target_revenue = 500000
        target_units_cvr = 100
        if kpi_bw in ['Total_ROI', 'Direct_ROI', 'Indirect_ROI']:
            target_revenue = st.number_input("Target Revenue (₹) - required for ROI calc", min_value=0, value=500000, step=10000, key='bw_rev')
            st.caption("Budget = Target Revenue / Target ROI")
        if kpi_bw in ['Direct_CVR', 'Indirect_CVR']:
            target_units_cvr = st.number_input("Target Units (for CVR calc)", min_value=0, value=100, key='bw_units')
            st.caption("Budget = Target Units / Target CVR")

        if st.button("🔄 Calculate Required Budget", key='bw_btn'):
            pla_hist_spend = pla_f['spend'].sum() if pla_f is not None and not pla_f.empty and 'spend' in pla_f.columns else 0
            pca_hist_spend = pca_f['adspend'].sum() if pca_f is not None and not pca_f.empty and 'adspend' in pca_f.columns else 0
            pla_hist_rev = pla_f['Total_Revenue'].sum() if pla_f is not None and not pla_f.empty else 0
            pca_hist_rev = pca_f['Total_Revenue'].sum() if pca_f is not None and not pca_f.empty else 0
            pla_hist_units = pla_f['Total_Units'].sum() if pla_f is not None and not pla_f.empty else 0
            pca_hist_units = pca_f['Total_Units'].sum() if pca_f is not None and not pca_f.empty else 0

            total_budget_req = 0
            if kpi_bw in ['Total_ROI', 'Direct_ROI', 'Indirect_ROI']:
                if target_value <= 0:
                    st.error("Target ROI must be > 0")
                    total_budget_req = 0
                else:
                    total_budget_req = target_revenue / target_value
            elif kpi_bw == 'Total_Revenue':
                hist_roi = (pla_hist_rev + pca_hist_rev) / (pla_hist_spend + pca_hist_spend) if (pla_hist_spend + pca_hist_spend) > 0 else 0
                total_budget_req = target_value / hist_roi if hist_roi > 0 else 0
            elif kpi_bw == 'Total_Units':
                hist_cvr = (pla_hist_units + pca_hist_units) / (pla_hist_spend + pca_hist_spend) if (pla_hist_spend + pca_hist_spend) > 0 else 0
                total_budget_req = target_value / hist_cvr if hist_cvr > 0 else 0
            elif kpi_bw in ['Direct_CVR', 'Indirect_CVR']:
                total_budget_req = target_units_cvr / target_value if target_value > 0 else 0

            if total_budget_req <= 0:
                st.warning("Could not compute budget. Check inputs.")
            else:
                tot_hist = pla_hist_spend + pca_hist_spend
                pla_share = pla_hist_spend / tot_hist if tot_hist > 0 else 0.5
                pca_share = 1.0 - pla_share
                pla_budget_req = total_budget_req * pla_share
                pca_budget_req = total_budget_req * pca_share

                st.success(f"**Total Budget Required: ₹{total_budget_req:,.2f}**")
                col1, col2 = st.columns(2)
                col1.metric("PLA Budget Required", f"₹{pla_budget_req:,.2f}")
                col2.metric("PCA Budget Required", f"₹{pca_budget_req:,.2f}")

                bw_parts = []
                if pla_f is not None and not pla_f.empty and pla_budget_req > 0:
                    st.subheader("PLA Budget Allocation (by Page Context & Slot)")
                    kpi_for_alloc = kpi_bw if kpi_bw in pla_f.columns else 'Total_ROI'
                    pla_res_bw = optimize_budget(pla_f, pla_budget_req, 'pla', kpi_for_alloc,
                                                 ['page_context', 'slot_type'] if all(c in pla_f.columns for c in ['page_context', 'slot_type']) else None)
                    bw_parts.append(pla_res_bw.assign(Format='PLA'))
                    cat_col = 'analytic_super_category' if 'analytic_super_category' in pla_res_bw.columns else 'super_category'
                    cols = ['Format', 'brand', cat_col, 'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI', 'Efficiency_Score']
                    if 'page_context' in pla_res_bw.columns:
                        cols.insert(4, 'page_context')
                    if 'slot_type' in pla_res_bw.columns:
                        cols.insert(5, 'slot_type')
                    cols = [c for c in cols if c in pla_res_bw.columns]
                    disp_pla = pla_res_bw[[c for c in cols if c in pla_res_bw.columns]].copy()
                    disp_pla = disp_pla.rename(columns={'brand': 'Brand', cat_col: 'Category', 'page_context': 'Page Context', 'slot_type': 'Slot Type',
                                                       'Recommended_Budget': 'Budget (₹)', 'Expected_Revenue': 'Expected Revenue (₹)', 'Expected_ROI': 'Expected ROI', 'Efficiency_Score': 'Efficiency Score'})
                    st.dataframe(disp_pla.round(2), use_container_width=True, hide_index=True)

                if pca_f is not None and not pca_f.empty and pca_budget_req > 0:
                    st.subheader("PCA Budget Allocation")
                    kpi_pca = kpi_bw if kpi_bw in pca_f.columns else 'Total_ROI'
                    pca_res_bw = optimize_budget(pca_f, pca_budget_req, 'pca', kpi_pca)
                    bw_parts.append(pca_res_bw.assign(Format='PCA'))
                    cat_col = 'super_category' if 'super_category' in pca_res_bw.columns else 'analytic_super_category'
                    disp_pca = pca_res_bw[['Format', 'brand', cat_col, 'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI', 'Efficiency_Score']].copy()
                    disp_pca = disp_pca.rename(columns={'brand': 'Brand', cat_col: 'Category', 'Recommended_Budget': 'Budget (₹)', 'Expected_Revenue': 'Expected Revenue (₹)', 'Expected_ROI': 'Expected ROI', 'Efficiency_Score': 'Efficiency Score'})
                    st.dataframe(disp_pca.round(2), use_container_width=True, hide_index=True)

                bw_combined = pd.concat(bw_parts, ignore_index=True) if len(bw_parts) > 1 else (bw_parts[0] if bw_parts else pd.DataFrame())
                if not bw_combined.empty:
                    day_phasing_bw = compute_day_level_budgets(bw_combined, sel_bu, bu_col_opt, sc_col_opt, pla_f, pca_f)
                    try:
                        st.session_state['day_level_backward'] = day_phasing_bw.copy()
                    except Exception:
                        pass
                    try:
                        daily_bw = expand_allocation_to_daily(bw_combined, sel_bu, bu_col_opt, sc_col_opt, pla_f, pca_f)
                        with st.expander("📋 Day-wise PLA/PCA allocation (by page context & slot)", expanded=False):
                            for i, day_name in enumerate(BSD_DAYS):
                                if i < len(daily_bw) and not daily_bw[i].empty:
                                    st.markdown(f"**{day_name}**")
                                    st.dataframe(daily_bw[i], use_container_width=True, hide_index=True)
                    except Exception:
                        pass

    with tab3:
        st.subheader("📅 Day-Level Split (BSD 6–12 March)")
        st.caption("7-day PLA vs PCA spend using BU–Super Category phasing curves. Run Budget Optimizer or Backward Calculator first.")
        st.markdown('<div class="spend-pulse-box"><h3>⚡ Spend Pulse — 7-Day Budget Split</h3><p>Uses BU–Super Category traffic curves (urgency vs deliberation)</p></div>', unsafe_allow_html=True)
        has_data = False
        try:
            if 'day_level_optimizer' in st.session_state:
                df_opt = st.session_state['day_level_optimizer']
                if isinstance(df_opt, pd.DataFrame) and not df_opt.empty:
                    st.markdown("**From Budget Optimizer**")
                    st.dataframe(df_opt, use_container_width=True, hide_index=True)
                    has_data = True
        except Exception:
            pass
        try:
            if 'day_level_backward' in st.session_state:
                df_bw = st.session_state['day_level_backward']
                if isinstance(df_bw, pd.DataFrame) and not df_bw.empty:
                    if has_data:
                        st.divider()
                    st.markdown("**From Backward Calculator**")
                    st.dataframe(df_bw, use_container_width=True, hide_index=True)
                    has_data = True
        except Exception:
            pass
        if not has_data:
            st.info("No day-level data yet. Click **Optimize** in Budget Optimizer or **Calculate** in Backward Calculator first.")

if __name__ == "__main__":
    main()
