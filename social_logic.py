import pandas as pd
import io
import re
from datetime import datetime
from database import get_connection

# --- 1. CONFIGURAZIONE ---
DATE_MAP = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
    "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
    "gen": 1, "feb": 2, "mar": 3, "apr": 4, "mag": 5, "giu": 6,
    "lug": 7, "ago": 8, "set": 9, "ott": 10, "nov": 11, "dic": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}
CURRENT_YEAR = datetime.now().year

# --- 2. LETTURA DATI ---
def get_data_health():
    conn = get_connection()
    try:
        last = conn.execute("SELECT MAX(date_recorded) FROM social_stats").fetchone()
        last_str = last[0] if last else None
        # Recupera tutto per i filtri globali
        query = "SELECT date_recorded, platform, metric_type, value FROM social_stats ORDER BY date_recorded DESC LIMIT 10000"
        return last_str, pd.read_sql_query(query, conn)
    except: return None, pd.DataFrame()
    finally: conn.close()

def get_content_health():
    conn = get_connection()
    try:
        q = """
        SELECT i.post_id, i.platform, i.date_published, i.caption, 
               p.views, p.likes, p.comments, p.shares, p.date_recorded
        FROM posts_inventory i
        JOIN posts_performance p ON i.post_id = p.post_id
        WHERE p.date_recorded = (SELECT MAX(date_recorded) FROM posts_performance WHERE post_id = i.post_id)
        ORDER BY p.views DESC
        """
        return pd.read_sql_query(q, conn)
    except: return pd.DataFrame()
    finally: conn.close()

def get_file_upload_history():
    conn = get_connection()
    try: return pd.read_sql_query("SELECT upload_date, filename, platform, status FROM upload_logs ORDER BY id DESC LIMIT 50", conn)
    except: return pd.DataFrame()
    finally: conn.close()

def check_file_log(filename, platform):
    conn = get_connection()
    try:
        res = conn.execute("SELECT upload_date FROM upload_logs WHERE filename=? AND platform=? AND status LIKE '%OK%' ORDER BY id DESC LIMIT 1", (filename, platform)).fetchone()
        return (True, res[0]) if res else (False, None)
    except: return False, None
    finally: conn.close()

def log_upload_event(filename, platform, status):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO upload_logs (filename, platform, status) VALUES (?, ?, ?)", (filename, platform, status))
        conn.commit()
    except: pass
    finally: conn.close()

# --- 4. PARSING ---
def parse_smart_date(date_str):
    if not isinstance(date_str, str): return None
    s = date_str.strip().lower()
    
    # ISO (IG) - Gestione T
    if 't' in s and '-' in s:
        try: return s.split('t')[0]
        except: pass
    
    # Testuale (TikTok)
    s_clean = s.split('t')[0]
    match = re.search(r'(\d{1,2})\s+([a-z]+)', s_clean)
    if match:
        d, m_str = int(match.group(1)), match.group(2)
        m = next((v for k,v in DATE_MAP.items() if k in m_str), None)
        if m:
            y = CURRENT_YEAR
            if datetime.now().month < 6 and m > 8: y -= 1
            try: return datetime(y, m, d).strftime('%Y-%m-%d')
            except: pass
            
    # Standard
    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d', '%d.%m.%Y']:
        try: return datetime.strptime(s_clean, fmt).strftime('%Y-%m-%d')
        except: continue
    return None

def clean_number(raw_val):
    s = str(raw_val).lower().strip()
    if not s or s in ['nan', 'none', '']: return 0
    mult = 1.0
    if 'k' in s: mult, s = 1000.0, s.replace('k','')
    if 'm' in s: mult, s = 1000000.0, s.replace('m','')
    
    s = s.strip()
    # Logica euristica: se c'è punto e 3 cifre finali, è migliaia.
    if '.' in s and re.search(r'\.\d{3}$', s) and s.count('.') >= 1: s = s.replace('.', '')
    
    s = s.replace(',', '.')
    s = re.sub(r'[^\d\.]', '', s)
    try: return int(float(s) * mult)
    except: return 0

# --- 5. SMART LOADER (AGGIORNATO) ---
def smart_csv_loader(uploaded_file):
    try:
        bytes_data = uploaded_file.getvalue()
        content = None
        for enc in ['utf-8', 'utf-16', 'latin-1', 'cp1252']:
            try: content = bytes_data.decode(enc); break
            except: continue
        if not content: return None, "Encoding Error"

        lines = content.splitlines()
        
        # 1. Rileva Separatore (scansiona prime righe)
        sep = ','
        comma_cnt = sum(l.count(',') for l in lines[:5])
        semi_cnt = sum(l.count(';') for l in lines[:5])
        if semi_cnt > comma_cnt: sep = ';'
        
        # 2. Header Hunting Aggressivo
        header_idx = 0
        found = False
        keywords = ["date", "time", "giorno", "data", "gender", "territories", "video title", "post time", "impression", "reach", "primary", "età", "nome dell'inserzione", "uomini", "donne"]
        
        for i, line in enumerate(lines[:50]):
            l_low = line.lower()
            # Deve contenere una keyword E il separatore (per evitare titoli)
            if any(k in l_low for k in keywords) and sep in line:
                header_idx = i
                found = True
                break
        
        if not found: header_idx = 0 # Fallback

        data_io = io.StringIO(content)
        # on_bad_lines='skip' è cruciale per i file sporchi
        df = pd.read_csv(data_io, sep=sep, skiprows=header_idx, dtype=str, on_bad_lines='skip')
        
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.dropna(how='all', inplace=True)
        return df, "OK"
    except Exception as e: return None, str(e)

def detect_metric_from_filename(filename):
    fn = filename.lower()
    if "content" in fn or "posts" in fn: return "SPECIAL_CONTENT"
    if "gender" in fn or "sesso" in fn or "pubblico" in fn: return "SPECIAL_GENDER"
    if "territor" in fn or "countr" in fn or "luoghi" in fn or "città" in fn: return "SPECIAL_GEO"
    if "copertura" in fn: return "Reach"
    if "interazioni" in fn: return "Interactions"
    if "follower" in fn and "activity" not in fn: return "Followers" # IG Follower
    if "clic" in fn: return "Link Clicks"
    if "activity" in fn: return "SPECIAL_ACTIVITY"
    if "eta" in fn or "inserzi" in fn or "destinazi" in fn: return "SPECIAL_ADS_META"
    return "AUTO_DETECT" 

# --- 6. SAVE BULK ---
def save_social_bulk(df, platform, metric_hint):
    conn = get_connection()
    df.columns = [c.lower().strip() for c in df.columns]
    processed_rows = 0
    today_str = datetime.now().strftime('%Y-%m-%d')

    try:
        # A. ADS META
        if metric_hint == "SPECIAL_ADS_META":
            if "nome dell'inserzione" in df.columns:
                for _, row in df.iterrows():
                    camp = str(row["nome dell'inserzione"])
                    spend = clean_number(row.get("importo speso (eur)", 0))
                    imp = clean_number(row.get("impression", 0))
                    # Salviamo come metrica social per visibilità immediata
                    upsert_stat(conn, "Meta Ads", f"Spend ({camp})", spend, today_str)
                    upsert_stat(conn, "Meta Ads", f"Impressions ({camp})", imp, today_str)
                    processed_rows += 1

        # B. CONTENT
        elif metric_hint == "SPECIAL_CONTENT":
            col_link = next((c for c in df.columns if "link" in c or "permalink" in c), None)
            col_pub = next((c for c in df.columns if "post time" in c or "posted" in c or "publish" in c), None)
            col_snap = next((c for c in df.columns if c in ["time", "date", "data"]), None)
            
            if col_link and col_pub:
                for _, row in df.iterrows():
                    link = str(row[col_link])
                    pid = link
                    m_tk = re.search(r'video/(\d+)', link)
                    m_ig = re.search(r'/(?:p|reel)/([^/]+)', link)
                    if m_tk: pid = m_tk.group(1)
                    elif m_ig: pid = m_ig.group(1)
                    
                    pdate = parse_smart_date(str(row[col_pub]))
                    sdate = parse_smart_date(str(row[col_snap])) if col_snap else today_str
                    if not pdate: continue

                    try:
                        conn.execute("INSERT OR REPLACE INTO posts_inventory (post_id, platform, date_published, caption, link) VALUES (?,?,?,?,?)", 
                                     (pid, platform, pdate, str(row.get('video title','')), link))
                    except: pass
                    
                    v = clean_number(row.get('total views', 0) or row.get('views', 0) or row.get('impressions', 0))
                    l = clean_number(row.get('total likes', 0) or row.get('likes', 0))
                    conn.execute("DELETE FROM posts_performance WHERE post_id=? AND date_recorded=?", (pid, sdate))
                    conn.execute("INSERT INTO posts_performance (post_id, date_recorded, views, likes, comments, shares) VALUES (?,?,?,?,?,?)", 
                                 (pid, sdate, v, l, 0, 0))
                    processed_rows += 1

        # C. DEMO
        elif metric_hint in ["SPECIAL_GENDER", "SPECIAL_GEO"]:
            if "uomini" in df.columns and "donne" in df.columns: # IG Pivot
                for _, row in df.iterrows():
                    age = str(row.iloc[0])
                    upsert_stat(conn, platform, f"Audience Gender Male ({age})", clean_number(row['uomini']), today_str)
                    upsert_stat(conn, platform, f"Audience Gender Female ({age})", clean_number(row['donne']), today_str)
                    processed_rows += 1
            else: # Standard
                cat, val = df.columns[0], df.columns[1]
                pre = "Audience Geo" if metric_hint == "SPECIAL_GEO" else "Audience Gender"
                for _, row in df.iterrows():
                    upsert_stat(conn, platform, f"{pre} {row[cat]}", clean_number(row[val]), today_str)
                    processed_rows += 1

        # D. TIME SERIES (IG/TIKTOK)
        else:
            d_col = next((c for c in df.columns if any(x in c for x in ['date', 'data', 'time', 'giorno'])), None)
            v_col = None
            for c in df.columns:
                if c != d_col: v_col = c
            
            if d_col and v_col:
                for _, row in df.iterrows():
                    d = parse_smart_date(str(row[d_col]))
                    if not d: continue
                    val = clean_number(row[v_col])
                    name = metric_hint if metric_hint != "AUTO_DETECT" else v_col.title()
                    upsert_stat(conn, platform, name, val, d)
                    processed_rows += 1

        conn.commit()
    except Exception as e:
        conn.close()
        return 0, str(e)
    
    conn.close()
    return processed_rows, "OK"

def upsert_stat(conn, platform, metric, value, date_val):
    try:
        conn.execute("DELETE FROM social_stats WHERE platform=? AND metric_type=? AND date_recorded=?", (platform, metric, date_val))
        conn.execute("INSERT INTO social_stats (platform, metric_type, value, date_recorded, source_type) VALUES (?,?,?,?,?)", (platform, metric, value, date_val, 'csv_gen'))
    except: pass