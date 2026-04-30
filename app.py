import streamlit as st
from streamlit.column_config import NumberColumn
import pandas as pd
import numpy as np
import io
import os
import re
import json
import hashlib
import functools
import tempfile
import warnings
from collections import Counter, defaultdict
warnings.filterwarnings('ignore')

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Chunk size for large CSVs (rows per chunk). Tune via env CSV_CHUNK_ROWS if needed.
CSV_CHUNK_ROWS = int(os.environ.get("CSV_CHUNK_ROWS", "250000"))

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

# 2026 Event Dates (Friday to Friday)
BSD_EVENT_LABEL = "May 2026 (8-15 May)"
BSD_DAYS = ['Fri 08-May', 'Sat 09-May', 'Sun 10-May', 'Mon 11-May', 'Tue 12-May', 'Wed 13-May', 'Thu 14-May', 'Fri 15-May']

# Historic event phasing captured from May 2025 behavior (8 event days).
PHASING_DATA = {
    ('CoreElectronics', 'Automobile'): [0.139, 0.127, 0.136, 0.12, 0.118, 0.116, 0.124, 0.119],
    ('CoreElectronics', 'Camera'): [0.139, 0.14, 0.141, 0.128, 0.123, 0.109, 0.116, 0.104],
    ('CoreElectronics', 'ITAccessory'): [0.135, 0.142, 0.137, 0.126, 0.122, 0.112, 0.117, 0.109],
    ('CoreElectronics', 'ITPeripherals'): [0.138, 0.131, 0.13, 0.122, 0.124, 0.117, 0.13, 0.109],
    ('CoreElectronics', 'LaptopAndDesktop'): [0.147, 0.15, 0.138, 0.125, 0.118, 0.114, 0.123, 0.086],
    ('CoreElectronics', 'PersonalHealthCare'): [0.141, 0.137, 0.138, 0.132, 0.12, 0.11, 0.12, 0.101],
    ('CoreElectronics', 'Service'): [0.148, 0.14, 0.142, 0.127, 0.118, 0.111, 0.119, 0.096],
    ('CoreElectronics', 'Storage'): [0.149, 0.135, 0.132, 0.126, 0.126, 0.114, 0.124, 0.094],
    ('CoreElectronics', 'Tablet'): [0.167, 0.142, 0.139, 0.115, 0.112, 0.114, 0.124, 0.087],
    ('Electronics', 'Automobile'): [0.12, 0.12, 0.15, 0.146, 0.121, 0.118, 0.114, 0.111],
    ('Electronics', 'Gaming'): [0.117, 0.13, 0.084, 0.078, 0.117, 0.136, 0.149, 0.188],
    ('Electronics', 'ITAccessory'): [0.128, 0.133, 0.128, 0.131, 0.13, 0.119, 0.127, 0.104],
    ('Electronics', 'LaptopAndDesktop'): [0.154, 0.17, 0.17, 0.128, 0.126, 0.076, 0.107, 0.069],
    ('Electronics', 'MobileProtection'): [0.13, 0.13, 0.135, 0.129, 0.126, 0.119, 0.125, 0.108],
    ('Electronics', 'RestOfMobileAccessory'): [0.126, 0.124, 0.132, 0.13, 0.13, 0.126, 0.118, 0.112],
    ('Electronics', 'SHA'): [0.154, 0.0, 0.308, 0.154, 0.077, 0.077, 0.154, 0.077],
    ('Electronics', 'Service'): [0.079, 0.118, 0.171, 0.118, 0.118, 0.158, 0.171, 0.066],
    ('EmergingElectronics', 'Audio'): [0.146, 0.137, 0.132, 0.123, 0.121, 0.113, 0.119, 0.109],
    ('EmergingElectronics', 'DigitalVoucherCode'): [0.193, 0.16, 0.144, 0.121, 0.109, 0.097, 0.097, 0.078],
    ('EmergingElectronics', 'ExtendedWarrantyNew'): [0.138, 0.133, 0.137, 0.125, 0.122, 0.116, 0.125, 0.105],
    ('EmergingElectronics', 'Gaming'): [0.13, 0.13, 0.135, 0.119, 0.119, 0.118, 0.135, 0.113],
    ('EmergingElectronics', 'IOT'): [0.144, 0.133, 0.147, 0.12, 0.117, 0.11, 0.119, 0.11],
    ('EmergingElectronics', 'MobileProtection'): [0.133, 0.13, 0.133, 0.129, 0.127, 0.118, 0.124, 0.105],
    ('EmergingElectronics', 'PowerBank'): [0.134, 0.122, 0.129, 0.124, 0.13, 0.125, 0.127, 0.109],
    ('EmergingElectronics', 'RestOfMobileAccessory'): [0.136, 0.13, 0.132, 0.128, 0.127, 0.118, 0.122, 0.105],
    ('EmergingElectronics', 'SHA'): [0.142, 0.135, 0.139, 0.128, 0.126, 0.116, 0.117, 0.097],
    ('EmergingElectronics', 'Video'): [0.131, 0.133, 0.141, 0.124, 0.126, 0.112, 0.117, 0.116],
    ('LargeAppliances', 'AirConditioner'): [0.168, 0.157, 0.142, 0.127, 0.119, 0.101, 0.101, 0.085],
    ('LargeAppliances', 'AppliancePasses'): [0.175, 0.228, 0.16, 0.116, 0.097, 0.075, 0.078, 0.071],
    ('LargeAppliances', 'AppliancesService'): [0.13, 0.129, 0.144, 0.138, 0.124, 0.111, 0.121, 0.103],
    ('LargeAppliances', 'CoreEA'): [0.172, 0.143, 0.136, 0.129, 0.117, 0.103, 0.111, 0.089],
    ('LargeAppliances', 'HomeEntertainmentLarge'): [0.16, 0.152, 0.15, 0.128, 0.123, 0.103, 0.107, 0.078],
    ('LargeAppliances', 'Microwave'): [0.15, 0.14, 0.141, 0.124, 0.119, 0.11, 0.117, 0.1],
    ('LargeAppliances', 'PremiumEA'): [0.142, 0.142, 0.141, 0.13, 0.125, 0.107, 0.122, 0.092],
    ('LargeAppliances', 'Refrigerator'): [0.157, 0.15, 0.145, 0.13, 0.116, 0.096, 0.114, 0.092],
    ('LargeAppliances', 'SeasonalEA'): [0.165, 0.141, 0.137, 0.127, 0.116, 0.106, 0.113, 0.095],
    ('LargeAppliances', 'WashingMachineDryer'): [0.173, 0.15, 0.132, 0.114, 0.109, 0.108, 0.125, 0.088],
    ('Mobile', 'Mobile'): [0.155, 0.138, 0.142, 0.137, 0.123, 0.114, 0.105, 0.087],
}

# BU fallback curves (8 days)
COREELECTRONICS_BASE = [0.145, 0.138, 0.137, 0.125, 0.12, 0.113, 0.122, 0.101]
ELECTRONICS_BASE = [0.126, 0.116, 0.16, 0.127, 0.118, 0.116, 0.133, 0.104]
EMERGINGELECTRONICS_BASE = [0.143, 0.134, 0.137, 0.124, 0.122, 0.114, 0.12, 0.105]
LARGEAPPLIANCES_BASE = [0.159, 0.153, 0.143, 0.126, 0.116, 0.102, 0.111, 0.089]
MOBILE_BASE = [0.155, 0.138, 0.142, 0.137, 0.123, 0.114, 0.105, 0.087]

DOW_MULTIPLIERS = {
    'Mon': 0.95, 'Tue': 0.90, 'Wed': 0.90,
    'Thu': 0.95, 'Fri': 1.05, 'Sat': 1.15, 'Sun': 1.10
}

URGENCY_CATEGORIES = frozenset(['Gaming', 'LaptopAndDesktop', 'RestOfMobileAccessory', 'PowerBank', 'MobileProtection'])
DELIBERATION_CATEGORIES = frozenset(['AirConditioner', 'Refrigerator', 'HomeEntertainmentLarge', 'WashingMachineDryer', 'LargeAppliances'])
URGENCY_CATEGORIES_CF = frozenset(x.casefold() for x in URGENCY_CATEGORIES)
DELIBERATION_CATEGORIES_CF = frozenset(x.casefold() for x in DELIBERATION_CATEGORIES)

BU_ALIASES = {
    'Large': 'LargeAppliances',
    'Large Appliances': 'LargeAppliances',
    'Core Electronics': 'CoreElectronics',
    'Emerging Electronics': 'EmergingElectronics',
}

BU_BASE_CURVES = {
    'CoreElectronics': COREELECTRONICS_BASE,
    'Electronics': ELECTRONICS_BASE,
    'EmergingElectronics': EMERGINGELECTRONICS_BASE,
    'LargeAppliances': LARGEAPPLIANCES_BASE,
    'Mobile': MOBILE_BASE,
}

# Keep only columns needed by the app to reduce memory and parse time.
PLA_REQUIRED_COLS_LOWER = {
    'day_date', 'business_unit', 'brand', 'analytic_super_category', 'analytic_vertical',
    'marketplace', 'advertiser_id', 'page_context', 'slot_type',
    'unique_views', 'clicks', 'spend', 'atc', 'total_views', 'listings',
    'direct_units', 'indirect_units', 'direct_rev', 'indirect_rev', 'ppv', 'alpha_mp'
}
PCA_REQUIRED_COLS_LOWER = {
    'day_date', 'brand', 'ad_account_id', 'business_account_id', 'page_type',
    'super_category', 'business_unit', 'rev_model', 'viewcount', 'view_count',
    'clicks', 'adspend', 'ad_spend', 'direct_units', 'indirect_units', 'ppv',
    'direct_rev', 'indirect_rev', 'alpha_mp', 'alpha_mp_map', 'creative_type',
    'campaign_id', 'bannerid', 'marketplace'
}

st.set_page_config(page_title="Spend Pulse · BSD", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

# --- DATA LOADING ---
def _resolve_gcp_credentials():
    """
    Return (credentials dict or None, error hint or None).
    Error hints are safe to show users (no secret values).

    Streamlit Community Cloud: paste full JSON in Secrets as GCP_CREDENTIALS_JSON
    (multiline JSON must be wrapped in triple quotes in TOML).

    Locally: `.streamlit/secrets.toml` with GCP_CREDENTIALS_JSON or GCP_CREDENTIALS_PATH.
    """
    def _validate_sa(d):
        if not isinstance(d, dict):
            return False
        if d.get("type") != "service_account":
            return False
        pk = d.get("private_key")
        em = d.get("client_email")
        if not isinstance(pk, str) or not isinstance(em, str) or not em.strip():
            return False
        if not pk.strip():
            return False
        # JSON uses \n in the string; normalize so PEM markers are detectable
        norm = pk.replace("\\n", "\n")
        if "-----BEGIN" not in norm or "PRIVATE KEY" not in norm:
            return False
        if len(norm) < 200:
            return False
        return True

    try:
        if hasattr(st, 'secrets') and st.secrets:
            raw = st.secrets.get("GCP_CREDENTIALS_JSON")
            if raw is not None:
                if isinstance(raw, dict):
                    if _validate_sa(raw):
                        return raw, None
                    return None, "GCP_CREDENTIALS_JSON must be a service account JSON (type, client_email, private_key)."
                if isinstance(raw, str):
                    s = raw.strip().lstrip("\ufeff")
                    if not s:
                        return None, "GCP_CREDENTIALS_JSON is empty in Secrets."
                    try:
                        d = json.loads(s)
                    except json.JSONDecodeError:
                        return None, (
                            "GCP_CREDENTIALS_JSON is not valid JSON. In Streamlit Secrets use TOML triple quotes, e.g. "
                            "GCP_CREDENTIALS_JSON = ''' { paste entire json here } '''"
                        )
                    if _validate_sa(d):
                        return d, None
                    return None, "JSON parses but is not a valid service_account key (check type, client_email, private_key)."
            path_secret = st.secrets.get("GCP_CREDENTIALS_PATH")
            if path_secret:
                p = str(path_secret).strip().strip('"').strip("'")
                if p and os.path.isfile(p):
                    with open(p, "r", encoding="utf-8") as f:
                        d = json.load(f)
                    if _validate_sa(d):
                        return d, None
                    return None, "File at GCP_CREDENTIALS_PATH is not a valid service account JSON."
    except json.JSONDecodeError:
        return None, "Could not parse credential JSON. Use triple-quoted multiline string in Secrets."
    except Exception as e:
        return None, f"Error reading credentials (check Secrets format). {type(e).__name__}"

    env_json = os.environ.get("GCP_CREDENTIALS_JSON")
    if env_json and env_json.strip().startswith("{"):
        try:
            d = json.loads(env_json)
            if _validate_sa(d):
                return d, None
            return None, "GCP_CREDENTIALS_JSON env var is not a valid service account JSON."
        except json.JSONDecodeError:
            return None, "GCP_CREDENTIALS_JSON env var is not valid JSON."

    path = os.environ.get("GCP_CREDENTIALS_PATH")
    if path:
        p = path.strip().strip('"').strip("'")
        if p and os.path.isfile(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if _validate_sa(d):
                    return d, None
                return None, "GCP_CREDENTIALS_PATH file is not a valid service account JSON."
            except json.JSONDecodeError:
                return None, "GCP_CREDENTIALS_PATH file is not valid JSON."
        elif p:
            return None, f"GCP_CREDENTIALS_PATH not found: {p}"

    return None, "No GCP credentials: add GCP_CREDENTIALS_JSON to Streamlit Secrets (or GCP_CREDENTIALS_PATH locally)."


def _secrets_cache_key():
    """
    Hash of secret values so @st.cache_data reloads after you fix Secrets in the dashboard.
    """
    h = hashlib.sha256()
    if hasattr(st, 'secrets') and st.secrets:
        for name in (
            "GCP_CREDENTIALS_JSON",
            "GCP_CREDENTIALS_PATH",
            "GOOGLE_DRIVE_FOLDER_ID",
            "GOOGLE_DRIVE_FOLDER_URL",
            "PLA_CSV_URL",
            "PCA_CSV_URL",
        ):
            v = st.secrets.get(name)
            if v is None:
                continue
            if isinstance(v, dict):
                h.update(json.dumps(v, sort_keys=True).encode("utf-8"))
            else:
                h.update(str(v).encode("utf-8"))
    h.update(os.environ.get("GCP_CREDENTIALS_PATH", "").encode("utf-8"))
    return h.hexdigest()


def _read_csv_streaming(source, required_cols_lower, chunksize=None):
    """
    Stream large CSVs in chunks, apply marketplace / alpha_mp filters per chunk to cut memory.
    source: file path (str) or readable buffer.
    """
    cs = chunksize or CSV_CHUNK_ROWS
    usecols = lambda c: str(c).strip().lower() in required_cols_lower
    common = dict(chunksize=cs, iterator=True, low_memory=False)
    try:
        reader = pd.read_csv(source, usecols=usecols, **common)
    except Exception:
        reader = pd.read_csv(source, **common)
    parts = []
    for chunk in reader:
        chunk = _apply_source_filters(chunk)
        if chunk is not None and not chunk.empty:
            parts.append(chunk)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True, sort=False)


def _read_csv_optimized(buf, required_cols_lower):
    """Single-pass read when file is small enough or chunking not needed."""
    try:
        return pd.read_csv(
            buf,
            usecols=lambda c: str(c).strip().lower() in required_cols_lower,
            low_memory=False
        )
    except Exception:
        return pd.read_csv(buf, low_memory=False)


def _ingest_csv_bytes(data: bytes, required_cols_lower):
    """Parse CSV bytes with chunked streaming; spill to temp file to avoid huge RAM copies."""
    if not data:
        return pd.DataFrame()
    path = None
    try:
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        with open(path, "wb") as f:
            f.write(data)
        return _read_csv_streaming(path, required_cols_lower)
    finally:
        if path and os.path.isfile(path):
            try:
                os.unlink(path)
            except Exception:
                pass


def _downcast_groupby_strings(df):
    """Reduce memory for repeated dimension columns used in groupby."""
    if df is None or df.empty:
        return df
    out = df
    for c in ("brand", "business_unit", "analytic_super_category", "super_category", "page_context", "slot_type", "page_type"):
        if c in out.columns and out[c].dtype == object:
            try:
                out[c] = out[c].astype("category")
            except Exception:
                pass
    return out


def _ensure_unique_column_labels(df):
    """Pandas groupby and st.dataframe fail on duplicate column names (e.g. repeated CSV headers)."""
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    if len(cols) == len(set(cols)):
        return df
    seen = {}
    new_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__dup{seen[c]}")
    out = df.copy()
    out.columns = new_cols
    return out


def _load_from_drive_api_by_name(folder_id, name_patterns, required_cols_lower):
    """
    Returns (dataframe or None, diagnostic_message or None).
    diagnostic_message is safe to show (no secrets).
    """
    if not HAS_DRIVE_API:
        return None, "Google API libraries missing (google-auth, google-api-python-client)."
    try:
        creds_json, cred_err = _resolve_gcp_credentials()
        if not creds_json:
            return None, cred_err or "GCP credentials missing."
        creds = service_account.Credentials.from_service_account_info(
            creds_json, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        all_files = []
        page_token = None
        while True:
            req = service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, modifiedTime)",
                pageSize=1000,
                pageToken=page_token,
            )
            results = req.execute()
            all_files.extend(results.get('files', []))
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        matches = []
        for f in all_files:
            name_l = str(f.get('name', '')).lower()
            if name_l.endswith('.csv') and all(p in name_l for p in name_patterns):
                matches.append(f)
        matches = sorted(matches, key=lambda x: str(x.get('name', '')).lower())
        pat = " + ".join(name_patterns)
        if not matches:
            names_preview = ", ".join(str(x.get("name", "")) for x in all_files[:12])
            hint = f"No CSV in folder matching names containing [{pat}]. Files seen: {names_preview or '(none — check folder ID and sharing with service account)'}"
            return None, hint
        frames = []
        ingest_errors = []
        for f in matches:
            try:
                buf = io.BytesIO()
                downloader = MediaIoBaseDownload(buf, service.files().get_media(fileId=f['id']))
                while not downloader.next_chunk()[1]:
                    pass
                buf.seek(0)
                raw = buf.getvalue()
                df = _ingest_csv_bytes(raw, required_cols_lower)
                if df is not None and not df.empty:
                    df = df.copy()
                    df['_source_file'] = f.get('name', '')
                    frames.append(df)
                elif df is not None and df.empty:
                    ingest_errors.append(f"{f.get('name')}: 0 rows after marketplace/alpha_mp filters (or empty file)")
            except Exception as ex:
                ingest_errors.append(f"{f.get('name')}: {type(ex).__name__}")
                continue
        if not frames:
            msg = "CSV files matched but no rows left after filtering. "
            msg += "Try Secrets: APPLY_SOURCE_FILTERS = false to test. "
            if ingest_errors:
                msg += "Details: " + " | ".join(ingest_errors[:5])
            return None, msg
        return pd.concat(frames, ignore_index=True, sort=False), None
    except Exception as ex:
        msg = f"Drive API error: {type(ex).__name__}: {str(ex)[:280]}"
        low = str(ex).lower()
        if "invalid jwt" in low or "invalid_grant" in low:
            msg += (
                " — **Invalid JWT signature** almost always means `private_key` in Streamlit Secrets is wrong: "
                "truncated, edited, or broken when pasted (newlines must stay as in the downloaded JSON). "
                "**Fix:** Google Cloud Console → IAM → Service Accounts → select account → Keys → **Add key** → JSON. "
                "Replace **entire** `GCP_CREDENTIALS_JSON` in Secrets with one paste of that file inside "
                "`GCP_CREDENTIALS_JSON = ''' ... '''`. Do not change the key text by hand. Revoke the old key after."
            )
        return None, msg

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
    # Apply common source-level filters as soon as data is read.
    df = _apply_source_filters(df)
    if 'day_date' in df.columns:
        df['day_date'] = pd.to_datetime(df['day_date'], errors='coerce', dayfirst=True)
    for col in ['unique_views', 'clicks', 'spend', 'atc', 'total_views', 'listings', 'direct_units', 'indirect_units', 'direct_rev', 'indirect_rev', 'ppv']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float32')
    return df

def _process_pca_df(df):
    # Apply common source-level filters as soon as data is read.
    df = _apply_source_filters(df)
    if 'day_date' in df.columns:
        df['day_date'] = pd.to_datetime(df['day_date'], errors='coerce', dayfirst=True)
    if 'ad_spend' in df.columns and 'adspend' not in df.columns:
        df['adspend'] = pd.to_numeric(df['ad_spend'], errors='coerce').fillna(0).astype('float32')
    if 'view_count' in df.columns and 'viewcount' not in df.columns:
        df['viewcount'] = pd.to_numeric(df['view_count'], errors='coerce').fillna(0).astype('float32')
    for col in ['viewcount', 'clicks', 'adspend', 'direct_units', 'indirect_units', 'ppv', 'direct_rev', 'indirect_rev']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float32')
    return df


def _get_col_by_lower(df, target_name):
    target = str(target_name).strip().lower()
    for col in df.columns:
        if str(col).strip().lower() == target:
            return col
    return None


def _source_filters_enabled():
    """Set APPLY_SOURCE_FILTERS = false in Streamlit Secrets to bypass (debug empty-data issues)."""
    try:
        if hasattr(st, 'secrets') and st.secrets:
            v = st.secrets.get("APPLY_SOURCE_FILTERS", True)
            return str(v).strip().lower() not in ("false", "0", "no", "off")
    except Exception:
        pass
    return True


def _apply_source_filters(df):
    """Apply global ingestion filters requested by business."""
    if df is None or df.empty:
        return df
    if not _source_filters_enabled():
        return df

    out = df.copy()

    # Keep only Flipkart rows whenever marketplace exists.
    marketplace_col = _get_col_by_lower(out, 'marketplace')
    if marketplace_col is not None:
        out = out[out[marketplace_col].astype(str).str.strip().str.lower() == 'flipkart']

    # Exclude alpha_mp == mp whenever alpha_mp exists.
    alpha_mp_col = _get_col_by_lower(out, 'alpha_mp')
    if alpha_mp_col is not None:
        out = out[out[alpha_mp_col].astype(str).str.strip().str.lower() != 'mp']

    return out

@st.cache_data
def load_pla_data(_secrets_key: str):
    folder_id = st.secrets.get("GOOGLE_DRIVE_FOLDER_ID") if hasattr(st, 'secrets') and st.secrets else None
    if not folder_id and hasattr(st, 'secrets') and st.secrets.get("GOOGLE_DRIVE_FOLDER_URL"):
        m = re.search(r'/folders/([a-zA-Z0-9_-]+)', str(st.secrets.get("GOOGLE_DRIVE_FOLDER_URL", "")))
        folder_id = m.group(1) if m else None
    creds, cred_err = _resolve_gcp_credentials()
    if folder_id:
        if not creds:
            if cred_err:
                st.error(cred_err)
        else:
            # Prefer all PLA CSVs in the folder so old and new naming conventions both work.
            df, drv_err = _load_from_drive_api_by_name(folder_id, ['pla'], PLA_REQUIRED_COLS_LOWER)
            if df is not None:
                return _process_pla_df(df)
            if drv_err:
                st.warning(f"PLA Drive load: {drv_err}")
    pla_url = st.secrets.get("PLA_CSV_URL") or st.secrets.get("PLA_FILE_ID") if hasattr(st, 'secrets') and st.secrets else None
    if pla_url:
        data = _download_from_google_drive(pla_url)
        if data:
            raw = _ingest_csv_bytes(data, PLA_REQUIRED_COLS_LOWER)
            return _process_pla_df(raw)
    st.error(
        "Google Drive not configured or load failed. Set GOOGLE_DRIVE_FOLDER_ID (or URL) and valid "
        "GCP_CREDENTIALS_JSON in Streamlit Secrets (JSON must be valid — use TOML triple quotes for multiline)."
    )
    return None

@st.cache_data
def load_pca_data(_secrets_key: str):
    folder_id = st.secrets.get("GOOGLE_DRIVE_FOLDER_ID") if hasattr(st, 'secrets') and st.secrets else None
    if not folder_id and hasattr(st, 'secrets') and st.secrets.get("GOOGLE_DRIVE_FOLDER_URL"):
        m = re.search(r'/folders/([a-zA-Z0-9_-]+)', str(st.secrets.get("GOOGLE_DRIVE_FOLDER_URL", "")))
        folder_id = m.group(1) if m else None
    creds, cred_err = _resolve_gcp_credentials()
    if folder_id:
        if not creds:
            if cred_err:
                st.error(cred_err)
        else:
            # Prefer all PCA CSVs in the folder so old and new naming conventions both work.
            df, drv_err = _load_from_drive_api_by_name(folder_id, ['pca'], PCA_REQUIRED_COLS_LOWER)
            if df is not None:
                return _process_pca_df(df)
            if drv_err:
                st.warning(f"PCA Drive load: {drv_err}")
    pca_url = st.secrets.get("PCA_CSV_URL") or st.secrets.get("PCA_FILE_ID") if hasattr(st, 'secrets') and st.secrets else None
    if pca_url:
        data = _download_from_google_drive(pca_url)
        if data:
            raw = _ingest_csv_bytes(data, PCA_REQUIRED_COLS_LOWER)
            return _process_pca_df(raw)
    st.error("Google Drive not configured or PCA load failed.")
    return None

@st.cache_data
def load_pla_processed(_secrets_key: str):
    """Load PLA and compute KPIs — cached (data is static)."""
    df = load_pla_data(_secrets_key)
    if df is None:
        return None
    out = calculate_pla_kpis(df)
    return _ensure_unique_column_labels(_downcast_groupby_strings(out))

@st.cache_data
def load_pca_processed(_secrets_key: str):
    """Load PCA and compute KPIs — cached (data is static)."""
    df = load_pca_data(_secrets_key)
    if df is None:
        return None
    out = calculate_pca_kpis(df)
    return _ensure_unique_column_labels(_downcast_groupby_strings(out))

# --- KPI CALCULATIONS ---
def calculate_pla_kpis(df):
    df = df.copy()
    # CTR = clicks / impressions (total_views). Standard formula.
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
    # CTR = clicks / impressions. PCA: clicks / viewcount.
    df['CTR'] = np.where(df['viewcount'] > 0, df['clicks'] / df['viewcount'], 0)
    df['Direct_CVR'] = np.where(df['adspend'] > 0, df['direct_units'] / df['adspend'], 0)
    df['Indirect_CVR'] = np.where(df['adspend'] > 0, df['indirect_units'] / df['adspend'], 0)
    df['Direct_ROI'] = np.where(df['adspend'] > 0, df['direct_rev'] / df['adspend'], 0)
    df['Indirect_ROI'] = np.where(df['adspend'] > 0, df['indirect_rev'] / df['adspend'], 0)
    df['Total_ROI'] = np.where(df['adspend'] > 0, (df['direct_rev'] + df['indirect_rev']) / df['adspend'], 0)
    df['Total_Revenue'] = df['direct_rev'] + df['indirect_rev']
    df['Total_Units'] = df['direct_units'] + df['indirect_units']
    return df


def _canonical_map_most_frequent(values):
    """
    One display label per case-insensitive group: the spelling that appears most often
    in the feed (ties → lexicographically smallest for stable UI).
    """
    buckets = defaultdict(list)
    for v in values:
        s = str(v).strip()
        if not s:
            continue
        buckets[s.casefold()].append(s)
    out = {}
    for cf, variants in buckets.items():
        cnt = Counter(variants)
        best = min(cnt.keys(), key=lambda k: (-cnt[k], k))
        out[cf] = best
    return out


def _collect_super_category_values(pla_df, pca_df):
    raw = []
    if pla_df is not None and not pla_df.empty and 'analytic_super_category' in pla_df.columns:
        raw.extend(pla_df['analytic_super_category'].dropna().astype(str).str.strip().tolist())
    if pca_df is not None and not pca_df.empty and 'super_category' in pca_df.columns:
        raw.extend(pca_df['super_category'].dropna().astype(str).str.strip().tolist())
    return raw


def _map_dim_series(series, mmap):
    if mmap is None or not len(mmap):
        return series

    def one(x):
        if pd.isna(x):
            return x
        t = str(x).strip()
        if not t:
            return x
        return mmap.get(t.casefold(), t)

    return series.astype(object).map(one)


def _apply_brand_lowercase(df):
    if df is None or df.empty or 'brand' not in df.columns:
        return df
    out = df.copy()

    def to_lower(x):
        if pd.isna(x):
            return x
        t = str(x).strip()
        if not t:
            return x
        return t.lower()

    out['brand'] = out['brand'].astype(object).map(to_lower)
    return out


def _apply_sc_maps(df, sc_map):
    if df is None or df.empty or not sc_map:
        return df
    out = df.copy()
    if 'analytic_super_category' in out.columns:
        out['analytic_super_category'] = _map_dim_series(out['analytic_super_category'], sc_map)
    if 'super_category' in out.columns:
        out['super_category'] = _map_dim_series(out['super_category'], sc_map)
    return out


def _apply_col_map(df, col, mmap):
    if df is None or df.empty or col not in df.columns or not mmap:
        return df
    out = df.copy()
    out[col] = _map_dim_series(out[col], mmap)
    return out


def normalize_pla_pca_shared_labels(pla_df, pca_df):
    """
    Lowercase all brands; align super category and BU spellings across PLA/PCA
    (case-insensitive merge + one canonical spelling per group).
    """
    pla_df = _apply_brand_lowercase(pla_df)
    pca_df = _apply_brand_lowercase(pca_df)
    smap = _canonical_map_most_frequent(_collect_super_category_values(pla_df, pca_df))
    pla_df = _apply_sc_maps(pla_df, smap)
    pca_df = _apply_sc_maps(pca_df, smap)
    for col in ('business_unit', 'analytic_vertical'):
        raw = []
        for df in (pla_df, pca_df):
            if df is not None and not df.empty and col in df.columns:
                raw.extend(df[col].dropna().astype(str).str.strip().tolist())
        if not raw:
            continue
        mmap = _canonical_map_most_frequent(raw)
        pla_df = _apply_col_map(pla_df, col, mmap)
        pca_df = _apply_col_map(pca_df, col, mmap)
    return pla_df, pca_df


# --- TRAFFIC PHASING ---
@functools.lru_cache(maxsize=512)
def get_phasing_for_bu_sc(business_unit, super_category):
    bu_raw = str(business_unit).strip()
    bu = BU_ALIASES.get(bu_raw, bu_raw)
    sc_raw = str(super_category).strip()
    key = (bu, sc_raw)
    if key not in PHASING_DATA:
        key = None
        for (kbu, ksc) in PHASING_DATA:
            if kbu == bu and ksc.casefold() == sc_raw.casefold():
                key = (kbu, ksc)
                break
    if key is not None and key in PHASING_DATA:
        base_curve = PHASING_DATA[key]
    else:
        base_curve = BU_BASE_CURVES.get(bu, [1.0 / len(BSD_DAYS)] * len(BSD_DAYS))

    sc_cf = sc_raw.casefold()
    adjusted = []
    for i, day in enumerate(BSD_DAYS):
        dow = day[:3]
        multiplier = DOW_MULTIPLIERS.get(dow, 1.0)
        if sc_cf in URGENCY_CATEGORIES_CF and dow in ('Fri', 'Sat'):
            multiplier *= 1.10
        elif sc_cf in DELIBERATION_CATEGORIES_CF and dow in ('Mon', 'Tue', 'Wed'):
            multiplier *= 1.05
        adjusted.append(base_curve[i] * multiplier)
    total = sum(adjusted)
    if total <= 0:
        return [1.0 / len(BSD_DAYS)] * len(BSD_DAYS)
    return [a / total for a in adjusted]

def _super_category_sidebar_options(pla_df, pca_df):
    """Union of PLA analytic_super_category and PCA super_category (all data)."""
    raw = []
    if pla_df is not None and not pla_df.empty and 'analytic_super_category' in pla_df.columns:
        raw.extend(pla_df['analytic_super_category'].dropna().astype(str).str.strip().tolist())
    if pca_df is not None and not pca_df.empty and 'super_category' in pca_df.columns:
        raw.extend(pca_df['super_category'].dropna().astype(str).str.strip().tolist())
    return ['All'] + sorted(set(x for x in raw if x))


def _brand_options_for_bu(pla_df, pca_df, bu_col, sel_bu):
    """Brands in scope: all rows if BU is All, else rows for the selected BU only."""
    raw = []
    for df in (pla_df, pca_df):
        if df is None or df.empty or 'brand' not in df.columns:
            continue
        if sel_bu == 'All':
            raw.extend(df['brand'].dropna().astype(str).str.strip().tolist())
            continue
        if not bu_col or bu_col not in df.columns:
            continue
        sub = df[df[bu_col].astype(str).str.strip() == str(sel_bu).strip()]
        raw.extend(sub['brand'].dropna().astype(str).str.strip().tolist())
    return sorted(set(x for x in raw if x))


def _super_category_options_for_bu(pla_df, pca_df, bu_col, sel_bu):
    """Super categories scoped to the selected BU when BU is not All."""
    if sel_bu == 'All' or not bu_col:
        return _super_category_sidebar_options(pla_df, pca_df)
    raw = []
    for df in (pla_df, pca_df):
        if df is None or df.empty or bu_col not in df.columns:
            continue
        sub = df[df[bu_col].astype(str).str.strip() == str(sel_bu).strip()]
        if sub.empty:
            continue
        if 'analytic_super_category' in sub.columns:
            raw.extend(sub['analytic_super_category'].dropna().astype(str).str.strip().tolist())
        if 'super_category' in sub.columns:
            raw.extend(sub['super_category'].dropna().astype(str).str.strip().tolist())
    if not raw:
        return ['All']
    return ['All'] + sorted(set(x for x in raw if x))


def _rank_search_matches(candidates, query: str, max_show: int = 600):
    """
    Search ranking: prefix matches first, then substring matches; empty query returns sorted full list (capped).
    """
    items = sorted({str(x).strip() for x in candidates if x is not None and str(x).strip()})
    q = (query or "").strip().lower()
    if not q:
        return items[:max_show]
    pref = [x for x in items if x.lower().startswith(q)]
    rest = [x for x in items if q in x.lower() and x not in pref]
    out = pref + sorted(rest)
    return out[:max_show]


def _apply_sc_filter(df, sel_sc):
    """Filter super category on the column(s) each feed actually has (fixes PCA leak)."""
    if df is None or df.empty or sel_sc == 'All':
        return df
    out = df.copy()
    has_pla_sc = 'analytic_super_category' in out.columns
    has_pca_sc = 'super_category' in out.columns
    sc_cf = str(sel_sc).strip().casefold()
    if has_pla_sc and has_pca_sc:
        a = out['analytic_super_category'].astype(str).str.strip().str.casefold() == sc_cf
        b = out['super_category'].astype(str).str.strip().str.casefold() == sc_cf
        out = out.loc[a | b]
    elif has_pla_sc:
        out = out[out['analytic_super_category'].astype(str).str.strip().str.casefold() == sc_cf]
    elif has_pca_sc:
        out = out[out['super_category'].astype(str).str.strip().str.casefold() == sc_cf]
    return out


def _resolve_phasing_bu_sc(sel_bu, sel_sc, pla_f, pca_f):
    """Pick one BU + super-category for the 8-day curve only (event-level phasing, not per placement)."""
    if sel_sc != 'All':
        sc = str(sel_sc).strip()
    else:
        sc = 'Gaming'
        for df, col in ((pla_f, 'analytic_super_category'), (pca_f, 'super_category')):
            if df is not None and not df.empty and col in df.columns:
                m = df[col].dropna()
                if not m.empty:
                    sc = str(m.mode().iloc[0]).strip()
                    break
    if sel_bu != 'All':
        bu = str(sel_bu).strip()
    else:
        bu = 'Electronics'
        for df, col in ((pla_f, 'business_unit'), (pca_f, 'business_unit')):
            if df is not None and not df.empty and col in df.columns:
                m = df[col].dropna()
                if not m.empty:
                    bu = str(m.mode().iloc[0]).strip()
                    break
    bu = BU_ALIASES.get(bu, bu)
    return bu, sc


def _day_level_pla_pca_spend(pla_budget: float, pca_budget: float, bu: str, sc: str) -> pd.DataFrame:
    """Split total PLA and PCA event budgets across the 8 event days using one phasing curve."""
    ph = get_phasing_for_bu_sc(bu, sc)
    rows = []
    for i, day in enumerate(BSD_DAYS):
        pa = pla_budget * ph[i]
        pc = pca_budget * ph[i]
        tot = pa + pc
        rows.append({
            'Day': day,
            'PLA Spend (₹)': round(pa, 2),
            'PCA Spend (₹)': round(pc, 2),
            'Total Spend (₹)': round(tot, 2),
            'PLA %': f"{(pa / tot * 100):.1f}%" if tot > 0 else "0%",
            'PCA %': f"{(pc / tot * 100):.1f}%" if tot > 0 else "0%",
        })
    return pd.DataFrame(rows)


def _pla_rows_without_missing_slot_type(pla_f):
    """Exclude rows with null, empty, or literal none/nan in slot_type (PLA CPC table)."""
    if pla_f is None or pla_f.empty or 'slot_type' not in pla_f.columns:
        return pla_f
    s = pla_f['slot_type']

    def bad_slot(v):
        if pd.isna(v):
            return True
        t = str(v).strip()
        if not t:
            return True
        return t.lower() in ('none', 'nan', 'null')

    keep = ~s.map(bad_slot)
    return pla_f.loc[keep].copy()


def _pla_cpc_guidance_table(pla_f) -> pd.DataFrame:
    """Historical effective CPC by placement — bid / serving guidance only; does not assign ₹."""
    if pla_f is None or pla_f.empty or 'spend' not in pla_f.columns or 'clicks' not in pla_f.columns:
        return pd.DataFrame()
    pla_f = _pla_rows_without_missing_slot_type(pla_f)
    if pla_f.empty:
        return pd.DataFrame()
    gcols = [c for c in ('brand', 'analytic_super_category', 'page_context', 'slot_type') if c in pla_f.columns]
    if not gcols:
        return pd.DataFrame()
    a = pla_f.groupby(gcols, dropna=False).agg(spend=('spend', 'sum'), clicks=('clicks', 'sum')).reset_index()
    a['CPC'] = np.where(a['clicks'] > 0, a['spend'] / a['clicks'], np.nan)
    out = a[gcols + ['CPC']].rename(columns={
        'brand': 'Brand',
        'analytic_super_category': 'Category',
        'page_context': 'Page Context',
        'slot_type': 'Slot Type',
        'CPC': 'CPC (₹)',
    })
    return out


def _pca_cpc_guidance_table(pca_f) -> pd.DataFrame:
    """Historical effective CPC by page type — bid guidance only."""
    if pca_f is None or pca_f.empty or 'clicks' not in pca_f.columns:
        return pd.DataFrame()
    spend_col = 'adspend' if 'adspend' in pca_f.columns else ('ad_spend' if 'ad_spend' in pca_f.columns else None)
    if not spend_col:
        return pd.DataFrame()
    gcols = [c for c in ('brand', 'super_category', 'page_type') if c in pca_f.columns]
    if not gcols:
        return pd.DataFrame()
    a = pca_f.groupby(gcols, dropna=False).agg(spend=(spend_col, 'sum'), clicks=('clicks', 'sum')).reset_index()
    a['CPC'] = np.where(a['clicks'] > 0, a['spend'] / a['clicks'], np.nan)
    out = a[gcols + ['CPC']].rename(columns={
        'brand': 'Brand',
        'super_category': 'Category',
        'page_type': 'Page Type',
        'CPC': 'CPC (₹)',
    })
    return out


def _rupee_columns_config(display_df):
    """Pretty number formatting for ₹ columns in st.dataframe."""
    cfg = {}
    for col in display_df.columns:
        if '(₹)' in col:
            cfg[col] = NumberColumn(col, format="%.2f")
    return cfg


def _is_metric_column(series, col_name: str) -> bool:
    """True for amounts, shares as %, and other numeric KPI columns (not dimension IDs)."""
    if '(₹)' in col_name or col_name.strip().endswith('%'):
        return True
    if 'Spend' in col_name and col_name != 'Day':
        return True
    try:
        if pd.api.types.is_numeric_dtype(series):
            return True
    except Exception:
        pass
    return False


def _dimensions_first_then_metrics(df):
    """All dimension columns first (preserve order), then all metric columns (preserve order)."""
    if df is None or df.empty:
        return df
    dims, mets = [], []
    for c in df.columns:
        if _is_metric_column(df[c], str(c)):
            mets.append(c)
        else:
            dims.append(c)
    return df[dims + mets]


# --- MAIN ---
def main():
    try:
        _main()
    except Exception as e:
        st.error(f"An error occurred. Please refresh. Details: {str(e)}")
        st.stop()

def _main():
    st.caption("Internal · Flipkart Ads")
    st.title("Spend Pulse")

    _sk = _secrets_cache_key()
    pla_df = load_pla_processed(_sk)
    pca_df = load_pca_processed(_sk)
    if pla_df is not None or pca_df is not None:
        pla_df, pca_df = normalize_pla_pca_shared_labels(pla_df, pca_df)
    if pla_df is None and pca_df is None:
        return

    # Keep all BUs in filter options.
    bu_col = 'business_unit' if 'business_unit' in (pla_df.columns if pla_df is not None else []) else 'analytic_vertical'
    if bu_col not in (pla_df.columns if pla_df is not None else []) and pca_df is not None:
        bu_col = 'business_unit' if 'business_unit' in pca_df.columns else None

    # Build filter options from available data
    df_ref = pla_df if pla_df is not None else pca_df
    if df_ref is None:
        st.error("No data available.")
        return

    bu_col_opt = 'business_unit' if 'business_unit' in df_ref.columns else ('analytic_vertical' if 'analytic_vertical' in df_ref.columns else None)

    st.sidebar.subheader("Filters")
    st.sidebar.caption("Select **BU** to scope brand and super category options.")
    sel_bu = st.sidebar.selectbox(
        "BU",
        ['All'] + sorted(df_ref[bu_col_opt].dropna().astype(str).unique().tolist()),
        key="sidebar_bu",
    ) if bu_col_opt and bu_col_opt in df_ref.columns else 'All'

    st.sidebar.markdown("**Brand**")
    brand_q = st.sidebar.text_input(
        "brand_search",
        label_visibility="collapsed",
        placeholder="Search brands",
        key=f"brand_dyn_q_{sel_bu}",
    )
    brand_candidates = _brand_options_for_bu(pla_df, pca_df, bu_col_opt, sel_bu)
    brand_matches = _rank_search_matches(brand_candidates, brand_q)
    brand_opts = ["All"] + brand_matches
    if str(brand_q or "").strip() and not brand_matches:
        st.sidebar.caption("No matching brands. Clear the search or adjust the query.")
        brand_opts = ["All"]
    else:
        n_br = len(brand_matches)
        hint = f" (of {len(brand_candidates)} total)" if n_br < len(brand_candidates) else ""
        st.sidebar.caption(f"{n_br} option(s){hint}.")
    sel_brand = st.sidebar.selectbox(
        "brand_pick",
        brand_opts,
        label_visibility="collapsed",
        key=f"sidebar_brand_{sel_bu}",
    )

    st.sidebar.markdown("**Super category**")
    sc_q = st.sidebar.text_input(
        "sc_search",
        label_visibility="collapsed",
        placeholder="Search super categories",
        key=f"sc_dyn_q_{sel_bu}",
    )
    sc_candidates = _super_category_options_for_bu(pla_df, pca_df, bu_col_opt, sel_bu)
    sc_candidates_no_all = [x for x in sc_candidates if x != "All"]
    sc_matches = _rank_search_matches(sc_candidates_no_all, sc_q)
    sc_opts = ["All"] + sc_matches
    if str(sc_q or "").strip() and not sc_matches:
        st.sidebar.caption("No matching categories. Clear the search or adjust the query.")
        sc_opts = ["All"]
    else:
        n_sc = len(sc_matches)
        hint = f" (of {len(sc_candidates_no_all)} total)" if n_sc < len(sc_candidates_no_all) else ""
        st.sidebar.caption(f"{n_sc} option(s){hint}.")
    sel_sc = st.sidebar.selectbox(
        "sc_pick",
        sc_opts,
        label_visibility="collapsed",
        key=f"sidebar_sc_{sel_bu}",
    )

    def apply_filters(df):
        if df is None or df.empty:
            return df
        out = df.copy()
        if bu_col_opt and bu_col_opt in out.columns and sel_bu != 'All':
            out = out[out[bu_col_opt].astype(str).str.strip() == sel_bu]
        if sel_brand != 'All':
            b_cf = str(sel_brand).strip().casefold()
            out = out[out['brand'].astype(str).str.strip().str.casefold() == b_cf]
        out = _apply_sc_filter(out, sel_sc)
        return out

    pla_f = apply_filters(pla_df) if pla_df is not None else None
    pca_f = apply_filters(pca_df) if pca_df is not None else None
    # Drop PCA rows with empty super_category (source data quality)
    if pca_f is not None and not pca_f.empty and 'super_category' in pca_f.columns:
        pca_f = pca_f[pca_f['super_category'].notna() & (pca_f['super_category'].astype(str).str.strip() != '')].copy()

    st.markdown(f"### BSD day-level budget · {BSD_EVENT_LABEL}")
    st.caption(
        "Day-level spend is shown by format (PLA vs PCA). Slot, context, and page type appear below as **effective CPC** "
        "from filtered history for bid reference; they do **not** allocate budget."
    )

    # Up to ₹1000 Cr; step ₹1L (type any value the widget allows)
    total_budget = st.number_input(
        "Total event budget (₹)",
        min_value=10_000,
        max_value=10_000_000_000,
        value=100_000,
        step=100_000,
        key="opt_budget",
    )

    pla_hist = pla_f['spend'].sum() if pla_f is not None and not pla_f.empty and 'spend' in pla_f.columns else 0
    pca_hist = pca_f['adspend'].sum() if pca_f is not None and not pca_f.empty and 'adspend' in pca_f.columns else 0
    tot_hist = pla_hist + pca_hist
    pla_hist_rev = pla_f['Total_Revenue'].sum() if pla_f is not None and not pla_f.empty and 'Total_Revenue' in pla_f.columns else 0.0
    pca_hist_rev = pca_f['Total_Revenue'].sum() if pca_f is not None and not pca_f.empty and 'Total_Revenue' in pca_f.columns else 0.0
    hist_roi_blend = (pla_hist_rev + pca_hist_rev) / tot_hist if tot_hist > 0 else 0.0
    pla_share = pla_hist / tot_hist if tot_hist > 0 else 0.5
    pca_share = 1.0 - pla_share
    pla_budget = total_budget * pla_share
    pca_budget = total_budget * pca_share

    if st.button("Run day-level budget", type="primary", key='opt_btn'):
        if tot_hist <= 0:
            st.warning("No historical spend in the current filter — widen filters or check data.")
        else:
            ph_bu, ph_sc = _resolve_phasing_bu_sc(sel_bu, sel_sc, pla_f, pca_f)
            est_rev = total_budget * hist_roi_blend if hist_roi_blend > 0 else 0.0

            k1, k2, k3 = st.columns(3)
            k1.metric("Event budget (₹)", f"{total_budget:,.0f}")
            k2.metric("Blended historical ROI", f"{hist_roi_blend:.2f}")
            k3.metric("Illustrative revenue at blended ROI (₹)", f"{est_rev:,.0f}")
            st.caption(
                f"Phasing uses **BU `{ph_bu}` × category `{ph_sc}`** at event level. "
                "Revenue is illustrative from blended ROI; CPC tables do not allocate spend."
            )

            st.divider()
            st.markdown("#### 1 · Day-level PLA vs PCA spend (₹)")
            st.caption("Totals equal the event budget; PLA/PCA split follows historical mix; daily amounts follow the phasing curve.")
            day_show = _day_level_pla_pca_spend(pla_budget, pca_budget, ph_bu, ph_sc)
            if not day_show.empty:
                day_show = _dimensions_first_then_metrics(day_show)
                cfg = _rupee_columns_config(day_show)
                args = dict(use_container_width=True, hide_index=True)
                if cfg:
                    args['column_config'] = cfg
                st.dataframe(day_show, **args)

            st.divider()
            st.markdown("#### 2 · PLA · CPC reference (slot × context)")
            st.caption("Effective CPC (spend ÷ clicks) from filtered history. Reference for bids and delivery; not a budget allocation.")
            pla_sheet = _pla_cpc_guidance_table(pla_f)
            if pla_sheet.empty:
                st.info("No PLA CPC data for this filter (required columns or volume missing).")
            else:
                ps = _dimensions_first_then_metrics(pla_sheet.round(4))
                cfg = _rupee_columns_config(ps)
                args = dict(use_container_width=True, hide_index=True)
                if cfg:
                    args['column_config'] = cfg
                st.dataframe(ps, **args)

            st.markdown("#### 3 · PCA · CPC reference (page type)")
            st.caption("Effective CPC by page type from filtered history. Reference for bids and delivery; not a budget allocation.")
            pca_sheet = _pca_cpc_guidance_table(pca_f)
            if pca_sheet.empty:
                st.info("No PCA CPC data for this filter.")
            else:
                zs = _dimensions_first_then_metrics(pca_sheet.round(4))
                cfg = _rupee_columns_config(zs)
                args = dict(use_container_width=True, hide_index=True)
                if cfg:
                    args['column_config'] = cfg
                st.dataframe(zs, **args)

if __name__ == "__main__":
    main()
