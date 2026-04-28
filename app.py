import streamlit as st
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

# Historical windows used for the May-2026 budget predictor.
JAN_2025_RANGE = (pd.Timestamp('2025-01-01'), pd.Timestamp('2025-01-31'))
JAN_2026_RANGE = (pd.Timestamp('2026-01-01'), pd.Timestamp('2026-01-31'))
MAY_2025_RANGE = (pd.Timestamp('2025-05-01'), pd.Timestamp('2025-05-08'))

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

st.set_page_config(page_title="Spend Pulse | BSD Budget Optimizer", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

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

# --- TRAFFIC PHASING ---
@functools.lru_cache(maxsize=256)
def get_phasing_for_bu_sc(business_unit, super_category):
    bu_raw = str(business_unit).strip()
    bu = BU_ALIASES.get(bu_raw, bu_raw)
    sc = str(super_category).strip()
    key = (bu, sc)
    if key in PHASING_DATA:
        base_curve = PHASING_DATA[key]
    else:
        base_curve = BU_BASE_CURVES.get(bu, [1.0 / len(BSD_DAYS)] * len(BSD_DAYS))

    adjusted = []
    for i, day in enumerate(BSD_DAYS):
        dow = day[:3]
        multiplier = DOW_MULTIPLIERS.get(dow, 1.0)
        if sc in URGENCY_CATEGORIES and dow in ('Fri', 'Sat'):
            multiplier *= 1.10
        elif sc in DELIBERATION_CATEGORIES and dow in ('Mon', 'Tue', 'Wed'):
            multiplier *= 1.05
        adjusted.append(base_curve[i] * multiplier)
    total = sum(adjusted)
    if total <= 0:
        return [1.0 / len(BSD_DAYS)] * len(BSD_DAYS)
    return [a / total for a in adjusted]

def calculate_traffic_phasing(base_volume, category_name, business_unit, base_curve_percentages=None):
    pct = get_phasing_for_bu_sc(business_unit, category_name)
    return pct, [base_volume * x for x in pct]

def _build_bu_lookup(df_pla, df_pca, bu_col, sc_col):
    """Build (brand|category) -> BU mapping once. Returns dict for PLA and PCA."""
    def _from_df(df):
        if df is None or df.empty or bu_col not in df.columns or 'brand' not in df.columns:
            return {}
        sc = 'analytic_super_category' if 'analytic_super_category' in df.columns else 'super_category'
        if sc not in df.columns:
            return {}
        sub = df[[bu_col, 'brand', sc]].dropna(subset=[bu_col])
        sub = sub.assign(_key=sub['brand'].astype(str).str.strip() + '|' + sub[sc].astype(str).str.strip())
        def _mode(x):
            vc = x.value_counts()
            return vc.index[0] if len(vc) > 0 else 'Electronics'
        agg = sub.groupby('_key')[bu_col].agg(_mode)
        return agg.to_dict()
    return {'PLA': _from_df(df_pla), 'PCA': _from_df(df_pca)}

def _compute_day_level_and_expand(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca, bu_lookup, include_pla_detail=True):
    """Single pass: compute day-level budgets and expand to daily tables. Returns (day_phasing_df, daily_tables)."""
    day_budgets = {d: {'PLA': 0.0, 'PCA': 0.0} for d in BSD_DAYS}
    daily_rows = [[] for _ in BSD_DAYS]
    cat_col = 'analytic_super_category' if 'analytic_super_category' in allocation_df.columns else 'super_category'
    cols = list(allocation_df.columns)
    has_page_context = include_pla_detail and 'page_context' in allocation_df.columns
    has_slot_type = include_pla_detail and 'slot_type' in allocation_df.columns
    for row in allocation_df.itertuples(index=False):
        try:
            r = dict(zip(cols, row))
            fmt = str(r.get('Format', 'PLA') or 'PLA')
            budget = r.get('Recommended_Budget', r.get('Budget (₹)', 0))
            if pd.isna(budget) or budget <= 0:
                continue
            budget = float(budget)
            category = str(r.get(cat_col) or r.get('super_category') or r.get('Category') or 'Gaming').strip() or 'Gaming'
            bu_val = str(sel_bu) if sel_bu != 'All' else bu_lookup.get(fmt, {}).get(str(r.get('brand', '')).strip() + '|' + category, 'Electronics')
            phasing = get_phasing_for_bu_sc(bu_val, category)
            for i, day in enumerate(BSD_DAYS):
                day_budgets[day][fmt] += budget * phasing[i]
                out = {'Day': BSD_DAYS[i], 'Format': fmt, 'Category': category, 'Budget (₹)': round(budget * phasing[i], 2)}
                if has_page_context:
                    out['Page Context'] = str(r.get('page_context', ''))
                if has_slot_type:
                    out['Slot Type'] = str(r.get('slot_type', ''))
                daily_rows[i].append(out)
        except Exception:
            continue
    rows = []
    for day in BSD_DAYS:
        pla_b, pca_b = day_budgets[day]['PLA'], day_budgets[day]['PCA']
        tot = pla_b + pca_b
        rows.append({'Day': day, 'PLA Spend (₹)': round(pla_b, 2), 'PCA Spend (₹)': round(pca_b, 2), 'Total Spend (₹)': round(tot, 2),
                     'PLA %': f"{(pla_b/tot*100):.1f}%" if tot > 0 else "0%",
                     'PCA %': f"{(pca_b/tot*100):.1f}%" if tot > 0 else "0%"})
    daily_tables = [pd.DataFrame(dr) if dr else pd.DataFrame() for dr in daily_rows]
    return pd.DataFrame(rows), daily_tables

def compute_day_level_budgets(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca, bu_lookup=None):
    """Apply day-level phasing. Returns DataFrame with Day, PLA_Budget, PCA_Budget, Total_Budget."""
    if bu_lookup is None and sel_bu == 'All':
        bu_lookup = _build_bu_lookup(df_pla, df_pca, bu_col, sc_col)
    day_phasing_df, _ = _compute_day_level_and_expand(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca, bu_lookup, include_pla_detail=False)
    return day_phasing_df

def expand_allocation_to_daily(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca, include_pla_detail=True, bu_lookup=None):
    """Expand allocation to day-level with page_context, slot for PLA."""
    if bu_lookup is None and sel_bu == 'All':
        bu_lookup = _build_bu_lookup(df_pla, df_pca, bu_col, sc_col)
    _, daily_tables = _compute_day_level_and_expand(allocation_df, sel_bu, bu_col, sc_col, df_pla, df_pca, bu_lookup, include_pla_detail)
    return daily_tables


def _safe_divide(numerator, denominator):
    return float(numerator) / float(denominator) if denominator else 0.0


def _slice_period(df, start, end):
    if df is None or df.empty or 'day_date' not in df.columns:
        return pd.DataFrame()
    mask = (df['day_date'] >= start) & (df['day_date'] <= end)
    return df.loc[mask].copy()


def _format_totals(pla_df, pca_df):
    pla_spend = pla_df['spend'].sum() if not pla_df.empty and 'spend' in pla_df.columns else 0.0
    pca_spend = pca_df['adspend'].sum() if not pca_df.empty and 'adspend' in pca_df.columns else 0.0
    pla_rev = pla_df['Total_Revenue'].sum() if not pla_df.empty and 'Total_Revenue' in pla_df.columns else 0.0
    pca_rev = pca_df['Total_Revenue'].sum() if not pca_df.empty and 'Total_Revenue' in pca_df.columns else 0.0
    pla_units = pla_df['Total_Units'].sum() if not pla_df.empty and 'Total_Units' in pla_df.columns else 0.0
    pca_units = pca_df['Total_Units'].sum() if not pca_df.empty and 'Total_Units' in pca_df.columns else 0.0
    spend = pla_spend + pca_spend
    revenue = pla_rev + pca_rev
    units = pla_units + pca_units
    return {
        'spend': spend,
        'revenue': revenue,
        'units': units,
        'roi': _safe_divide(revenue, spend),
        'cvr': _safe_divide(units, spend),
        'pla_spend': pla_spend,
        'pca_spend': pca_spend,
    }


def _build_budget_prediction_context(pla_df, pca_df):
    jan25_pla = _slice_period(pla_df, *JAN_2025_RANGE)
    jan25_pca = _slice_period(pca_df, *JAN_2025_RANGE)
    jan26_pla = _slice_period(pla_df, *JAN_2026_RANGE)
    jan26_pca = _slice_period(pca_df, *JAN_2026_RANGE)
    may25_pla = _slice_period(pla_df, *MAY_2025_RANGE)
    may25_pca = _slice_period(pca_df, *MAY_2025_RANGE)

    jan25 = _format_totals(jan25_pla, jan25_pca)
    jan26 = _format_totals(jan26_pla, jan26_pca)
    may25 = _format_totals(may25_pla, may25_pca)

    jan_yoy_spend = _safe_divide(jan26['spend'], jan25['spend']) if jan25['spend'] > 0 else 1.0
    jan_yoy_revenue = _safe_divide(jan26['revenue'], jan25['revenue']) if jan25['revenue'] > 0 else jan_yoy_spend
    growth_blend = (0.55 * jan_yoy_spend) + (0.45 * jan_yoy_revenue)
    growth_blend = max(0.4, min(growth_blend, 3.0))
    projected_budget = may25['spend'] * growth_blend

    roi_candidates = [x for x in [may25['roi'], jan26['roi'], jan25['roi']] if x > 0]
    predicted_roi = (0.6 * may25['roi'] + 0.4 * jan26['roi']) if (may25['roi'] > 0 and jan26['roi'] > 0) else (roi_candidates[0] if roi_candidates else 0.0)
    predicted_revenue = projected_budget * predicted_roi

    share_basis = may25 if may25['spend'] > 0 else jan26
    pla_share = _safe_divide(share_basis['pla_spend'], share_basis['spend']) if share_basis['spend'] > 0 else 0.5
    return {
        'jan25': jan25,
        'jan26': jan26,
        'may25': may25,
        'jan_yoy_spend': jan_yoy_spend,
        'jan_yoy_revenue': jan_yoy_revenue,
        'growth_blend': growth_blend,
        'projected_budget': projected_budget,
        'predicted_roi': predicted_roi,
        'predicted_revenue': predicted_revenue,
        'pla_share': pla_share,
        'pca_share': 1.0 - pla_share,
        'may25_pla_df': may25_pla,
        'may25_pca_df': may25_pca,
    }

# --- BUDGET OPTIMIZATION ---
def optimize_budget(df, total_budget, data_type, kpi_col, group_cols_extra=None):
    spend_col = 'spend' if data_type == 'pla' else ('adspend' if 'adspend' in df.columns else 'ad_spend')
    views_col = 'total_views' if data_type == 'pla' else 'viewcount'
    group_cols = ['brand', 'analytic_super_category'] if data_type == 'pla' else ['brand', 'super_category']
    if group_cols_extra:
        group_cols = group_cols + [c for c in group_cols_extra if c in df.columns]
    if kpi_col not in df.columns:
        kpi_col = 'Total_ROI'
    # Aggregate sums for ratio metrics (never use mean of ratios — use sum(num)/sum(denom))
    agg_d = {spend_col: 'sum'}
    for c in ['clicks', views_col, 'Total_Revenue', 'Total_Units', 'direct_rev', 'indirect_rev', 'direct_units', 'indirect_units']:
        if c in df.columns:
            agg_d[c] = 'sum'
    group_cols = [c for c in group_cols if c in df.columns]
    if not group_cols:
        return pd.DataFrame()
    perf = df.groupby(group_cols).agg(agg_d).reset_index()
    # Recompute ratio metrics from sums (correct aggregation)
    if 'Total_Revenue' not in perf.columns:
        perf['Total_Revenue'] = 0
    if 'Total_Units' not in perf.columns:
        perf['Total_Units'] = 0
    if 'direct_rev' in perf.columns and 'indirect_rev' in perf.columns:
        perf['Total_Revenue'] = perf['direct_rev'] + perf['indirect_rev']
    if 'direct_units' in perf.columns and 'indirect_units' in perf.columns:
        perf['Total_Units'] = perf['direct_units'] + perf['indirect_units']
    perf['Total_ROI'] = np.where(perf[spend_col] > 0, perf['Total_Revenue'] / perf[spend_col], 0)
    if 'direct_rev' in perf.columns:
        perf['Direct_ROI'] = np.where(perf[spend_col] > 0, perf['direct_rev'] / perf[spend_col], 0)
    if 'indirect_rev' in perf.columns:
        perf['Indirect_ROI'] = np.where(perf[spend_col] > 0, perf['indirect_rev'] / perf[spend_col], 0)
    if 'direct_units' in perf.columns:
        perf['Direct_CVR'] = np.where(perf[spend_col] > 0, perf['direct_units'] / perf[spend_col], 0)
    if 'indirect_units' in perf.columns:
        perf['Indirect_CVR'] = np.where(perf[spend_col] > 0, perf['indirect_units'] / perf[spend_col], 0)
    if 'clicks' in perf.columns and views_col in perf.columns:
        perf['CTR'] = np.where(perf[views_col] > 0, perf['clicks'] / perf[views_col], 0)
    if 'Total_Revenue' not in perf.columns:
        perf['Total_Revenue'] = 0
    if 'Total_Units' not in perf.columns:
        perf['Total_Units'] = 0
    kpi_val = perf[kpi_col] if kpi_col in perf.columns else perf['Total_ROI']
    perf['Efficiency_Score'] = kpi_val * np.log1p(perf[spend_col])
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
    st.caption("For Internal Use Only")
    st.title("Spend Pulse — BSD Budget Optimizer")

    _sk = _secrets_cache_key()
    pla_df = load_pla_processed(_sk)
    pca_df = load_pca_processed(_sk)
    if pla_df is None and pca_df is None:
        return

    # Keep all BUs; May predictor can now learn from mixed history (Jan'25, Jan'26, May'25).
    bu_col = 'business_unit' if 'business_unit' in (pla_df.columns if pla_df is not None else []) else 'analytic_vertical'
    if bu_col not in (pla_df.columns if pla_df is not None else []) and pca_df is not None:
        bu_col = 'business_unit' if 'business_unit' in pca_df.columns else None

    # Build filter options from available data
    df_ref = pla_df if pla_df is not None else pca_df
    if df_ref is None:
        st.error("No data available.")
        return

    bu_col_opt = 'business_unit' if 'business_unit' in df_ref.columns else ('analytic_vertical' if 'analytic_vertical' in df_ref.columns else None)
    sc_col_opt = 'analytic_super_category' if 'analytic_super_category' in df_ref.columns else ('super_category' if 'super_category' in df_ref.columns else None)

    with st.sidebar.expander("📖 Where to click & what to find", expanded=False):
        st.markdown("""
**Budget Optimizer** (tab 1) — Enter budget → Click **Optimize** → metrics, allocation, day-level split (at end)

**Budget Estimator** (tab 2) — Select KPI, enter target → Click **Calculate** → budget, allocation, day-level split (at end)
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
    # Drop PCA rows with empty super_category (source data quality)
    if pca_f is not None and not pca_f.empty and 'super_category' in pca_f.columns:
        pca_f = pca_f[pca_f['super_category'].notna() & (pca_f['super_category'].astype(str).str.strip() != '')].copy()

    tab_opt, tab_bw, tab_pred = st.tabs(["Budget Optimizer", "Budget Estimator", "May 2026 Budget Predictor"])

    with tab_opt:
        st.subheader(f"Budget Optimizer (BSD {BSD_EVENT_LABEL})")
        st.caption("Uses full historical data. No date filter. Local filters: BU, Brand, Super Category.")

        total_budget = st.number_input("Total Budget (₹)", min_value=10000, max_value=10000000, value=100000, step=10000, key='opt_budget')
        kpi_options = [c for c in ['Total_ROI', 'Direct_ROI', 'Indirect_ROI', 'CTR', 'Direct_CVR', 'Indirect_CVR'] if c in df_ref.columns]
        selected_kpi = st.selectbox("Optimize for KPI", kpi_options or ['Total_ROI'], key='opt_kpi')

        pla_hist = pla_f['spend'].sum() if pla_f is not None and not pla_f.empty and 'spend' in pla_f.columns else 0
        pca_hist = pca_f['adspend'].sum() if pca_f is not None and not pca_f.empty and 'adspend' in pca_f.columns else 0
        tot_hist = pla_hist + pca_hist
        pla_hist_rev = pla_f['Total_Revenue'].sum() if pla_f is not None and not pla_f.empty and 'Total_Revenue' in pla_f.columns else 0
        pca_hist_rev = pca_f['Total_Revenue'].sum() if pca_f is not None and not pca_f.empty and 'Total_Revenue' in pca_f.columns else 0
        pla_share = pla_hist / tot_hist if tot_hist > 0 else 0.5
        pca_share = 1.0 - pla_share
        pla_budget = total_budget * pla_share
        pca_budget = total_budget * pca_share

        if st.button("🚀 Optimize", key='opt_btn'):
            results = []
            if pla_f is not None and not pla_f.empty and pla_budget > 0:
                pla_res = optimize_budget(pla_f, pla_budget, 'pla', selected_kpi, ['page_context', 'slot_type'] if all(c in pla_f.columns for c in ['page_context', 'slot_type']) else None)
                if not pla_res.empty:
                    pla_res = pla_res.copy()
                    pla_res['Format'] = 'PLA'
                    results.append(pla_res)
            if pca_f is not None and not pca_f.empty and pca_budget > 0:
                pca_res = optimize_budget(pca_f, pca_budget, 'pca', selected_kpi,
                                           ['page_type'] if 'page_type' in pca_f.columns else None)
                if not pca_res.empty:
                    pca_res = pca_res.copy()
                    pca_res['Format'] = 'PCA'
                    results.append(pca_res)

            if not results:
                st.warning("No allocation possible.")
            else:
                combined = pd.concat(results, ignore_index=True)
                total_rec = combined['Recommended_Budget'].sum()
                total_rev = combined['Expected_Revenue'].sum()
                total_rec_safe = total_rec if total_rec > 0 else 1
                def _exp(m): return (combined[m] * combined['Recommended_Budget']).sum() / total_rec_safe if m in combined.columns else 0
                # Expected ROI must match Expected Revenue ÷ recommended budget (portfolio-implied).
                tot_hist_rev = pla_hist_rev + pca_hist_rev
                exp_roi = total_rev / total_rec_safe if total_rec_safe > 0 else 0.0
                hist_roi_blended = tot_hist_rev / tot_hist if tot_hist > 0 else None
                # CTR: sum(clicks)/sum(views), exclude segments with zero views to avoid inflation
                _spend = pd.Series(0.0, index=combined.index)
                for c in ['spend', 'adspend']:
                    if c in combined.columns:
                        _spend = _spend + combined[c].fillna(0)
                _views = pd.Series(0.0, index=combined.index)
                for c in ['total_views', 'viewcount']:
                    if c in combined.columns:
                        _views = _views + combined[c].fillna(0)
                mask = (_spend > 0) & (_views > 0)
                if mask.any() and 'clicks' in combined.columns:
                    exp_clicks = (combined.loc[mask, 'clicks'] * combined.loc[mask, 'Recommended_Budget'] / _spend[mask]).sum()
                    exp_views = (_views[mask] * combined.loc[mask, 'Recommended_Budget'] / _spend[mask]).sum()
                    exp_ctr = exp_clicks / exp_views if exp_views > 0 else _exp('CTR')
                else:
                    exp_ctr = _exp('CTR')
                exp_dcvr = _exp('Direct_CVR')
                exp_icvr = _exp('Indirect_CVR')
                # Unified efficiency: show the best (max) efficiency score across formats for the same category/context
                cat_col = 'analytic_super_category' if 'analytic_super_category' in combined.columns else 'super_category'
                group_cols_for_eff = [cat_col] if cat_col in combined.columns else []
                if 'page_context' in combined.columns:
                    group_cols_for_eff.append('page_context')
                if 'slot_type' in combined.columns:
                    group_cols_for_eff.append('slot_type')
                if group_cols_for_eff:
                    combined['Efficiency_Score'] = combined.groupby(group_cols_for_eff)['Efficiency_Score'].transform('max')
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Recommended Budget", f"₹{total_rec:,.2f}")
                col2.metric("Expected Revenue", f"₹{total_rev:,.2f}")
                col3.metric("Expected ROI", f"{exp_roi:.2f}")
                col4, col5, col6 = st.columns(3)
                col4.metric("Expected CTR", f"{min(exp_ctr, 1.0)*100:.2f}%")
                col5.metric("Expected Direct CVR", f"{exp_dcvr*100:.4f}%")
                col6.metric("Expected Indirect CVR", f"{exp_icvr*100:.4f}%")
                if tot_hist > 0:
                    st.caption(
                        f"Historical blended ROI (Total_Revenue ÷ spend in filtered history): **{hist_roi_blended:.2f}** — "
                        "shown for context only; **Expected ROI** above is revenue implied by this allocation ÷ ₹ budget."
                    )

                combined_copy = combined.copy()
                if 'Format' not in combined_copy.columns:
                    combined_copy['Format'] = 'PLA'
                # Unified category: PLA has analytic_super_category, PCA has super_category
                combined = combined.copy()
                if 'analytic_super_category' in combined.columns and 'super_category' in combined.columns:
                    combined['_cat'] = combined['analytic_super_category'].fillna(combined['super_category'])
                elif 'analytic_super_category' in combined.columns:
                    combined['_cat'] = combined['analytic_super_category']
                else:
                    combined['_cat'] = combined['super_category'].fillna('') if 'super_category' in combined.columns else ''
                # No Efficiency Score in display; PLA uses page_context/slot_type, PCA uses page_type
                base_cols = ['Format', '_cat', 'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI']
                if 'page_context' in combined.columns:
                    base_cols.append('page_context')
                if 'slot_type' in combined.columns:
                    base_cols.append('slot_type')
                if 'page_type' in combined.columns:
                    base_cols.append('page_type')
                for m in ['CTR', 'Direct_CVR', 'Indirect_CVR']:
                    if m in combined.columns and m not in base_cols:
                        base_cols.append(m)
                disp = combined[[c for c in base_cols if c in combined.columns]].copy()
                for m in ['CTR', 'Direct_CVR', 'Indirect_CVR']:
                    if m in disp.columns:
                        vals = pd.to_numeric(disp[m], errors='coerce').fillna(0)
                        if m == 'CTR':
                            vals = np.minimum(vals, 1.0)
                        disp[m] = (vals * 100).round(4).astype(str) + '%'
                rename_map = {'_cat': 'Category', 'page_context': 'Page Context', 'slot_type': 'Slot Type', 'page_type': 'Page Type',
                             'Recommended_Budget': 'Budget (₹)', 'Expected_Revenue': 'Expected Revenue (₹)', 'Expected_ROI': 'Expected ROI'}
                for m, lbl in [('CTR', 'CTR %'), ('Direct_CVR', 'Direct CVR %'), ('Indirect_CVR', 'Indirect CVR %')]:
                    if m in disp.columns:
                        rename_map[m] = lbl
                disp = disp.rename(columns=rename_map)

                # Split into two separate tables: PLA and PCA
                if 'Format' in disp.columns:
                    pla_table = disp[disp['Format'] == 'PLA'].copy()
                    pca_table = disp[disp['Format'] == 'PCA'].copy()
                    if 'Format' in pla_table.columns:
                        pla_table = pla_table.drop(columns=['Format'])
                    if 'Format' in pca_table.columns:
                        pca_table = pca_table.drop(columns=['Format'])
                    # PLA: keep Page Context, Slot Type; drop Page Type
                    if 'Page Type' in pla_table.columns:
                        pla_table = pla_table.drop(columns=['Page Type'])
                    # PCA: keep Page Type; drop Page Context, Slot Type (PLA-only)
                    for drop_col in ['Page Context', 'Slot Type']:
                        if drop_col in pca_table.columns:
                            pca_table = pca_table.drop(columns=[drop_col])
                    if not pla_table.empty:
                        st.subheader("PLA Allocation (by Category / Page Context / Slot)")
                        st.dataframe(pla_table.round(2).fillna(''), use_container_width=True, hide_index=True)
                    if not pca_table.empty:
                        st.subheader("PCA Allocation (by Category / Page Type)")
                        st.dataframe(pca_table.round(2).fillna(''), use_container_width=True, hide_index=True)
                else:
                    st.dataframe(disp.round(2).fillna(''), use_container_width=True)

                bu_lookup = _build_bu_lookup(pla_f, pca_f, bu_col_opt, sc_col_opt) if sel_bu == 'All' else {}
                try:
                    with st.spinner("Calculating 8-day split..."):
                        day_phasing_df, daily_tables = _compute_day_level_and_expand(combined_copy, sel_bu, bu_col_opt, sc_col_opt, pla_f, pca_f, bu_lookup)
                    with st.expander("📋 Day-wise PLA/PCA allocation (by page context & slot)", expanded=True):
                        for i, day_name in enumerate(BSD_DAYS):
                            if i < len(daily_tables) and not daily_tables[i].empty:
                                df_day = daily_tables[i]
                                if 'Format' in df_day.columns and 'Budget (₹)' in df_day.columns:
                                    pla_day = df_day[df_day['Format'] == 'PLA']['Budget (₹)'].sum()
                                    pca_day = df_day[df_day['Format'] == 'PCA']['Budget (₹)'].sum()
                                    st.markdown(f"**{day_name}** — PLA: ₹{pla_day:,.0f} | PCA: ₹{pca_day:,.0f}")
                                else:
                                    st.markdown(f"**{day_name}**")
                                st.dataframe(daily_tables[i].fillna(''), use_container_width=True, hide_index=True)
                    st.subheader(f"Day-Level Split (BSD {BSD_EVENT_LABEL})")
                    st.caption("PLA vs PCA spend by day (BU–Super Category phasing)")
                    if not day_phasing_df.empty and 'PLA Spend (₹)' in day_phasing_df.columns and 'PCA Spend (₹)' in day_phasing_df.columns:
                        pla_total = day_phasing_df['PLA Spend (₹)'].sum()
                        pca_total = day_phasing_df['PCA Spend (₹)'].sum()
                        st.markdown(f"**PLA total: ₹{pla_total:,.0f}** | **PCA total: ₹{pca_total:,.0f}**")
                    st.dataframe(day_phasing_df.fillna(''), use_container_width=True, hide_index=True)
                    # Third table: simple Day x Ad-Format spends (3 columns)
                    try:
                        if {'Day', 'PLA Spend (₹)', 'PCA Spend (₹)'} <= set(day_phasing_df.columns):
                            day_melt = day_phasing_df[['Day', 'PLA Spend (₹)', 'PCA Spend (₹)']].copy()
                            day_melt = day_melt.melt(id_vars=['Day'], value_vars=['PLA Spend (₹)', 'PCA Spend (₹)'],
                                                     var_name='Ad Format', value_name='Spend (₹)')
                            day_melt['Ad Format'] = day_melt['Ad Format'].str.replace(' Spend \\(₹\\)', '').str.strip()
                            st.subheader('Day × Ad Format Spend')
                            st.dataframe(day_melt[['Day', 'Ad Format', 'Spend (₹)']].fillna('').round(2), use_container_width=True, hide_index=True)
                    except Exception:
                        pass
                except Exception as e:
                    st.warning(f"Could not compute day-level split: {e}")

    with tab_bw:
        st.subheader("Budget Estimator")
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
                    pla_res_bw = pla_res_bw.assign(Format='PLA')
                    bw_parts.append(pla_res_bw)
                    cat_col = 'analytic_super_category' if 'analytic_super_category' in pla_res_bw.columns else 'super_category'
                    cols = ['Format', cat_col, 'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI']
                    if 'page_context' in pla_res_bw.columns:
                        cols.insert(4, 'page_context')
                    if 'slot_type' in pla_res_bw.columns:
                        cols.insert(5, 'slot_type')
                    cols = [c for c in cols if c in pla_res_bw.columns]
                    disp_pla = pla_res_bw[[c for c in cols if c in pla_res_bw.columns]].copy()
                    disp_pla = disp_pla.rename(columns={cat_col: 'Category', 'page_context': 'Page Context', 'slot_type': 'Slot Type',
                                                       'Recommended_Budget': 'Budget (₹)', 'Expected_Revenue': 'Expected Revenue (₹)', 'Expected_ROI': 'Expected ROI'})
                    if 'Format' in disp_pla.columns:
                        disp_pla = disp_pla.drop(columns=['Format'])
                    st.dataframe(disp_pla.round(2).fillna(''), use_container_width=True, hide_index=True)

                if pca_f is not None and not pca_f.empty and pca_budget_req > 0:
                    st.subheader("PCA Budget Allocation")
                    kpi_pca = kpi_bw if kpi_bw in pca_f.columns else 'Total_ROI'
                    pca_res_bw = optimize_budget(pca_f, pca_budget_req, 'pca', kpi_pca,
                                                  ['page_type'] if 'page_type' in pca_f.columns else None)
                    pca_res_bw = pca_res_bw.assign(Format='PCA')
                    bw_parts.append(pca_res_bw)
                    cat_col = 'super_category' if 'super_category' in pca_res_bw.columns else 'analytic_super_category'
                    pca_disp_cols = ['Format', cat_col, 'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI']
                    if 'page_type' in pca_res_bw.columns:
                        pca_disp_cols.insert(3, 'page_type')
                    pca_disp_cols = [c for c in pca_disp_cols if c in pca_res_bw.columns]
                    disp_pca = pca_res_bw[pca_disp_cols].copy()
                    rename_pca = {cat_col: 'Category', 'Recommended_Budget': 'Budget (₹)', 'Expected_Revenue': 'Expected Revenue (₹)', 'Expected_ROI': 'Expected ROI'}
                    if 'page_type' in disp_pca.columns:
                        rename_pca['page_type'] = 'Page Type'
                    disp_pca = disp_pca.rename(columns=rename_pca)
                    if 'Format' in disp_pca.columns:
                        disp_pca = disp_pca.drop(columns=['Format'])
                    st.dataframe(disp_pca.round(2).fillna(''), use_container_width=True, hide_index=True)

                bw_combined = pd.concat(bw_parts, ignore_index=True) if len(bw_parts) > 1 else (bw_parts[0] if bw_parts else pd.DataFrame())
                # Unified efficiency for backward results: show max efficiency across same category/context
                if not bw_combined.empty:
                    cat_col_bw = 'analytic_super_category' if 'analytic_super_category' in bw_combined.columns else 'super_category'
                    group_cols_eff_bw = [cat_col_bw] if cat_col_bw in bw_combined.columns else []
                    if 'page_context' in bw_combined.columns:
                        group_cols_eff_bw.append('page_context')
                    if 'slot_type' in bw_combined.columns:
                        group_cols_eff_bw.append('slot_type')
                    if group_cols_eff_bw:
                        bw_combined['Efficiency_Score'] = bw_combined.groupby(group_cols_eff_bw)['Efficiency_Score'].transform('max')
                if not bw_combined.empty:
                    bu_lookup_bw = _build_bu_lookup(pla_f, pca_f, bu_col_opt, sc_col_opt) if sel_bu == 'All' else {}
                    try:
                        with st.spinner("Calculating 8-day split..."):
                            day_phasing_bw, daily_bw = _compute_day_level_and_expand(bw_combined, sel_bu, bu_col_opt, sc_col_opt, pla_f, pca_f, bu_lookup_bw)
                        with st.expander("📋 Day-wise PLA/PCA allocation (by page context & slot)", expanded=True):
                            for i, day_name in enumerate(BSD_DAYS):
                                if i < len(daily_bw) and not daily_bw[i].empty:
                                    df_day_bw = daily_bw[i]
                                    if 'Format' in df_day_bw.columns and 'Budget (₹)' in df_day_bw.columns:
                                        pla_day_bw = df_day_bw[df_day_bw['Format'] == 'PLA']['Budget (₹)'].sum()
                                        pca_day_bw = df_day_bw[df_day_bw['Format'] == 'PCA']['Budget (₹)'].sum()
                                        st.markdown(f"**{day_name}** — PLA: ₹{pla_day_bw:,.0f} | PCA: ₹{pca_day_bw:,.0f}")
                                    else:
                                        st.markdown(f"**{day_name}**")
                                    st.dataframe(daily_bw[i].fillna(''), use_container_width=True, hide_index=True)
                        st.subheader(f"Day-Level Split (BSD {BSD_EVENT_LABEL})")
                        st.caption("PLA vs PCA spend by day (BU–Super Category phasing)")
                        if not day_phasing_bw.empty and 'PLA Spend (₹)' in day_phasing_bw.columns and 'PCA Spend (₹)' in day_phasing_bw.columns:
                            pla_total_bw = day_phasing_bw['PLA Spend (₹)'].sum()
                            pca_total_bw = day_phasing_bw['PCA Spend (₹)'].sum()
                            st.markdown(f"**PLA total: ₹{pla_total_bw:,.0f}** | **PCA total: ₹{pca_total_bw:,.0f}**")
                        st.dataframe(day_phasing_bw.fillna(''), use_container_width=True, hide_index=True)
                        # Third table for backward calc: Day x Ad-Format spends (3 columns)
                        try:
                            if {'Day', 'PLA Spend (₹)', 'PCA Spend (₹)'} <= set(day_phasing_bw.columns):
                                day_melt_bw = day_phasing_bw[['Day', 'PLA Spend (₹)', 'PCA Spend (₹)']].copy()
                                day_melt_bw = day_melt_bw.melt(id_vars=['Day'], value_vars=['PLA Spend (₹)', 'PCA Spend (₹)'],
                                                               var_name='Ad Format', value_name='Spend (₹)')
                                day_melt_bw['Ad Format'] = day_melt_bw['Ad Format'].str.replace(' Spend \\(₹\\)', '').str.strip()
                                st.subheader('Day × Ad Format Spend (Budget Estimator)')
                                st.dataframe(day_melt_bw[['Day', 'Ad Format', 'Spend (₹)']].fillna('').round(2), use_container_width=True, hide_index=True)
                        except Exception:
                            pass
                    except Exception as e:
                        st.warning(f"Could not compute day-level split: {e}")

    with tab_pred:
        st.subheader(f"Budget Predictor (BSD {BSD_EVENT_LABEL})")
        st.caption("Uses Jan'25 + Jan'26 growth signals and May'25 event baseline to predict May'26 budget.")

        pred_ctx = _build_budget_prediction_context(pla_f if pla_f is not None else pd.DataFrame(), pca_f if pca_f is not None else pd.DataFrame())
        if pred_ctx['may25']['spend'] <= 0:
            st.warning("May 2025 event data is missing in current filter scope. Please check Drive files or sidebar filters.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Jan'25 Spend", f"₹{pred_ctx['jan25']['spend']:,.0f}")
            c2.metric("Jan'26 Spend", f"₹{pred_ctx['jan26']['spend']:,.0f}")
            c3.metric("May'25 Event Spend", f"₹{pred_ctx['may25']['spend']:,.0f}")

            c4, c5, c6 = st.columns(3)
            c4.metric("Jan Spend YoY", f"{pred_ctx['jan_yoy_spend']:.2f}x")
            c5.metric("Jan Revenue YoY", f"{pred_ctx['jan_yoy_revenue']:.2f}x")
            c6.metric("Growth Blend", f"{pred_ctx['growth_blend']:.2f}x")

            scenario_multiplier = st.slider("Scenario Multiplier", min_value=0.7, max_value=1.5, value=1.0, step=0.05)
            suggested_budget = pred_ctx['projected_budget'] * scenario_multiplier
            predicted_roi = pred_ctx['predicted_roi']
            projected_revenue = suggested_budget * predicted_roi if predicted_roi > 0 else 0.0

            target_revenue = st.number_input(
                "Target Revenue for May'26 BSD (₹)",
                min_value=0,
                value=int(projected_revenue) if projected_revenue > 0 else 500000,
                step=10000,
                key='pred_target_revenue',
            )
            required_budget_for_target = _safe_divide(target_revenue, predicted_roi) if predicted_roi > 0 else 0.0

            m1, m2, m3 = st.columns(3)
            m1.metric("Suggested Total Budget", f"₹{suggested_budget:,.0f}")
            m2.metric("Predicted ROI", f"{predicted_roi:.2f}")
            m3.metric("Predicted Revenue", f"₹{projected_revenue:,.0f}")

            st.metric("Budget Required for Target Revenue", f"₹{required_budget_for_target:,.0f}")
            budget_mode = st.radio(
                "Budget source for allocation",
                ["Use Suggested Budget", "Use Target-Revenue Budget"],
                horizontal=True,
                key='pred_budget_mode',
            )
            total_budget_pred = suggested_budget if budget_mode == "Use Suggested Budget" else required_budget_for_target

            if total_budget_pred <= 0:
                st.warning("Unable to compute predictor budget. Check historical ROI and input values.")
            else:
                pla_budget_pred = total_budget_pred * pred_ctx['pla_share']
                pca_budget_pred = total_budget_pred * pred_ctx['pca_share']
                st.caption(f"Format split from historical baseline: PLA {pred_ctx['pla_share']*100:.1f}% | PCA {pred_ctx['pca_share']*100:.1f}%")

                pred_parts = []
                pla_src = pred_ctx['may25_pla_df'] if not pred_ctx['may25_pla_df'].empty else (pla_f if pla_f is not None else pd.DataFrame())
                pca_src = pred_ctx['may25_pca_df'] if not pred_ctx['may25_pca_df'].empty else (pca_f if pca_f is not None else pd.DataFrame())

                if not pla_src.empty and pla_budget_pred > 0:
                    pla_pred = optimize_budget(
                        pla_src, pla_budget_pred, 'pla', 'Total_ROI',
                        ['page_context', 'slot_type'] if all(c in pla_src.columns for c in ['page_context', 'slot_type']) else None
                    )
                    if not pla_pred.empty:
                        pred_parts.append(pla_pred.assign(Format='PLA'))

                if not pca_src.empty and pca_budget_pred > 0:
                    pca_pred = optimize_budget(
                        pca_src, pca_budget_pred, 'pca', 'Total_ROI',
                        ['page_type'] if 'page_type' in pca_src.columns else None
                    )
                    if not pca_pred.empty:
                        pred_parts.append(pca_pred.assign(Format='PCA'))

                if not pred_parts:
                    st.warning("No predictor allocation could be generated for current filters.")
                else:
                    pred_combined = pd.concat(pred_parts, ignore_index=True)
                    st.subheader("Predicted Allocation (May'26 BSD)")
                    ps = pred_combined.copy()
                    if 'analytic_super_category' in ps.columns and 'super_category' in ps.columns:
                        ps['_cat'] = ps['analytic_super_category'].fillna(ps['super_category'])
                    elif 'analytic_super_category' in ps.columns:
                        ps['_cat'] = ps['analytic_super_category']
                    elif 'super_category' in ps.columns:
                        ps['_cat'] = ps['super_category']
                    else:
                        ps['_cat'] = ''
                    show_cols = ['Format', 'brand', '_cat', 'page_context', 'slot_type', 'page_type', 'Recommended_Budget', 'Expected_Revenue', 'Expected_ROI']
                    show_cols = [c for c in show_cols if c in ps.columns]
                    pred_show = ps[show_cols].rename(columns={
                        '_cat': 'Category',
                        'page_context': 'Page Context',
                        'slot_type': 'Slot Type',
                        'page_type': 'Page Type',
                        'Recommended_Budget': 'Budget (₹)',
                        'Expected_Revenue': 'Expected Revenue (₹)',
                        'Expected_ROI': 'Expected ROI',
                    })
                    st.dataframe(pred_show.fillna('').round(2), use_container_width=True, hide_index=True)

                    bu_lookup_pred = _build_bu_lookup(pla_src, pca_src, bu_col_opt, sc_col_opt) if sel_bu == 'All' else {}
                    day_pred, daily_pred = _compute_day_level_and_expand(pred_combined, sel_bu, bu_col_opt, sc_col_opt, pla_src, pca_src, bu_lookup_pred)

                    st.subheader(f"Predicted Day-Level Split (BSD {BSD_EVENT_LABEL})")
                    st.dataframe(day_pred.fillna(''), use_container_width=True, hide_index=True)
                    with st.expander("Daily allocation detail", expanded=False):
                        for i, day_name in enumerate(BSD_DAYS):
                            if i < len(daily_pred) and not daily_pred[i].empty:
                                st.markdown(f"**{day_name}**")
                                st.dataframe(daily_pred[i].fillna(''), use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
