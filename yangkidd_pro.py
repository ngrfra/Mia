import streamlit as st
import ollama
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime
import requests
from urllib.parse import urlencode
import base64
import threading
import time
import os
import csv
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import io

# --- CONFIGURAZIONE ---
st.set_page_config(page_title="YANGKIDD ENTERPRISE OS", page_icon="💎", layout="wide")

# --- STILE ---
st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 100%); color: #e0e0e0; }
    .stTextInput > div > div > input { background-color: #1a1a1a; color: #00ff99; border: 2px solid #333; }
    [data-testid="stDataFrame"] { background-color: #1a1a1a; }
    .ai-thinking { color: #00ff99; animation: blink 1s linear infinite; font-weight: bold; }
    .alert-box { padding: 15px; border-radius: 5px; margin-bottom: 20px; font-weight: bold; }
    .alert-red { background-color: #ff4b4b33; border: 1px solid #ff4b4b; color: #ff4b4b; }
    .alert-green { background-color: #00ff9933; border: 1px solid #00ff99; color: #00ff99; }
    @keyframes blink { 50% { opacity: 0; } }
</style>
""", unsafe_allow_html=True)

# --- DATABASE ---
def init_advanced_db():
    conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS campaigns (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, platform TEXT, status TEXT, budget REAL, spend REAL, revenue REAL, roas REAL, impressions INTEGER, clicks INTEGER, ctr REAL, streams INTEGER, start_date TEXT, end_date TEXT, notes TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS chat_history (id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, role TEXT, content TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS api_credentials (id INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT UNIQUE, client_id TEXT, client_secret TEXT, access_token TEXT, refresh_token TEXT, expires_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS knowledge_base (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, content TEXT, added_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS social_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT, metric_type TEXT, value REAL, date_recorded DATE, source_type TEXT)''')
    conn.commit()
    conn.close()

init_advanced_db()

# --- HELPER PER STATO DATI (NUOVO) ---
def get_data_health():
    conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
    
    # 1. Trova l'ultima data registrata
    last_date_row = conn.execute("SELECT MAX(date_recorded) FROM social_stats").fetchone()
    last_date_str = last_date_row[0] if last_date_row and last_date_row[0] else None
    
    # 2. Riepilogo per data e piattaforma (Inventario)
    query = """
    SELECT date_recorded as 'Data', platform as 'Piattaforma', 
           COUNT(metric_type) as 'N_Metriche', 
           GROUP_CONCAT(metric_type, ', ') as 'Dettaglio_Metriche'
    FROM social_stats 
    GROUP BY date_recorded, platform 
    ORDER BY date_recorded DESC
    """
    df_summary = pd.read_sql_query(query, conn)
    
    conn.close()
    return last_date_str, df_summary

# --- CSV LOADER "UNIVERSALE" ---
def smart_csv_loader(uploaded_file):
    try:
        bytes_data = uploaded_file.getvalue()
        content = None
        encodings = ['utf-16', 'utf-8', 'latin-1', 'cp1252']
        for enc in encodings:
            try: content = bytes_data.decode(enc); break
            except UnicodeError: continue
        if content is None: return None, "Errore codifica fatale."

        lines = content.splitlines()
        header_row_index = -1
        sep = ',' 
        for line in lines[:10]:
            if line.count('\t') > line.count(','): sep = '\t'
            elif line.count(',') > line.count('\t'): sep = ','
            elif line.count(';') > line.count(','): sep = ';'

        for i, line in enumerate(lines):
            clean_line = line.lower()
            if ("data" in clean_line or "date" in clean_line or "giorno" in clean_line):
                if sep in clean_line or len(lines) > 1: header_row_index = i; break
        
        if header_row_index == -1: header_row_index = 0

        data_io = io.StringIO(content)
        df = pd.read_csv(data_io, sep=sep, skiprows=header_row_index, on_bad_lines='skip', dtype=str)
        df.dropna(how='all', inplace=True)
        return df, f"OK ({enc})"
    except Exception as e: return None, str(e)

def detect_metric_from_filename(filename):
    fn = filename.lower()
    if "follower" in fn: return "Followers"
    if "copertura" in fn or "reach" in fn: return "Reach"
    if "interazioni" in fn: return "Interazioni"
    if "visite" in fn: return "Visite Profilo"
    if "clic" in fn: return "Clic sul Link"
    if "impression" in fn: return "Impressions"
    if "eta" in fn: return "Demografica Età"
    return "Unknown"

def save_social_bulk(df, platform, metric_type):
    conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
    df.columns = [str(c).lower().strip() for c in df.columns]
    date_col = next((c for c in df.columns if "data" in c or "date" in c or "giorno" in c), None)
    if not date_col: return 0, "No Data Col"
    
    value_col = None
    for col in df.columns:
        if col != date_col: value_col = col; break
    if not value_col: return 0, "No Value Col"

    cnt = 0
    errors = 0
    for _, row in df.iterrows():
        try:
            raw_date = str(row[date_col]).split('T')[0].strip()
            valid_date = None
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d.%m.%Y', '%Y/%m/%d']:
                try: valid_date = datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d'); break
                except: continue
            if not valid_date: errors += 1; continue

            raw_val = str(row[value_col])
            if ',' in raw_val and '.' in raw_val: raw_val = raw_val.replace('.', '').replace(',', '.')
            elif ',' in raw_val: raw_val = raw_val.replace(',', '.')
            if not raw_val or raw_val.lower() == 'nan': continue
            val = float(raw_val)

            conn.execute("DELETE FROM social_stats WHERE platform=? AND metric_type=? AND date_recorded=?", (platform, metric_type, valid_date))
            conn.execute("INSERT INTO social_stats (platform, metric_type, value, date_recorded, source_type) VALUES (?,?,?,?,?)", (platform, metric_type, val, valid_date, 'csv_batch'))
            cnt += 1
        except: errors += 1; continue
    conn.commit(); conn.close()
    return cnt, f"Err:{errors}"

def delete_social_stat(stat_id):
    conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
    conn.execute("DELETE FROM social_stats WHERE id=?", (stat_id,))
    conn.commit(); conn.close()

# --- ALTRE FUNZIONI (PDF, ETC) ---
PDF_FOLDER = "knowledge_docs"
def ingest_local_pdfs():
    if not os.path.exists(PDF_FOLDER): os.makedirs(PDF_FOLDER); return "Cartella creata."
    files = [f for f in os.listdir(PDF_FOLDER) if f.endswith('.pdf')]
    if not files: return "Nessun PDF."
    conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
    c = 0
    for f in files:
        if conn.execute("SELECT count(*) FROM knowledge_base WHERE source=?",(f"PDF:{f}",)).fetchone()[0]==0:
            try:
                r=PdfReader(os.path.join(PDF_FOLDER,f)); txt="\n".join([p.extract_text() for p in r.pages])
                conn.execute("INSERT INTO knowledge_base (source,content) VALUES (?,?)",(f"PDF:{f}",txt)); c+=1
            except: pass
    conn.commit(); conn.close(); return f"Importati {c}"
def scrape_webpage(url):
    try:
        r=requests.get(url,headers={'User-Agent':'Mozilla/5.0'},timeout=10)
        s=BeautifulSoup(r.text,'html.parser'); [x.decompose() for x in s(["script","style"])]
        return s.title.string," ".join([p.text for p in s.find_all('p')])
    except Exception as e: return None,str(e)
def save_knowledge(s,c): 
    conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False); conn.execute("INSERT INTO knowledge_base (source,content) VALUES (?,?)",(s,c)); conn.commit(); conn.close()
def get_knowledge_context():
    conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False); r=conn.execute("SELECT source,content FROM knowledge_base").fetchall(); conn.close()
    return "\n".join([f"-- {x[0]} --\n{x[1][:2000]}" for x in r]) if r else ""
def get_campaigns():
    conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False)
    try: df=pd.read_sql_query("SELECT * FROM campaigns ORDER BY id DESC",conn); return df
    except: return pd.DataFrame()
    finally: conn.close()
def save_campaign(d):
    conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False)
    conn.execute("INSERT INTO campaigns (name,platform,status,budget,spend,revenue,roas,impressions,clicks,streams) VALUES (?,?,?,?,?,?,?,?,?,?)",
    (d['name'],d['platform'],'Active',0,d['spend'],d['revenue'],d['revenue']/d['spend'] if d['spend']>0 else 0,d['impressions'],0,d['streams'])); conn.commit(); conn.close()
class SpotifyAPI:
    def __init__(self): 
        conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False); r=conn.execute("SELECT client_id,client_secret,access_token FROM api_credentials WHERE platform='spotify'").fetchone(); conn.close()
        self.cid,self.csec,self.tok=r if r else (None,None,None)
    def save(self,i,s): 
        conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False); conn.execute("INSERT OR REPLACE INTO api_credentials (platform,client_id,client_secret) VALUES ('spotify',?,?)",(i,s)); conn.commit(); conn.close()
    def get_auth(self): return f"https://accounts.spotify.com/authorize?client_id={self.cid}&response_type=code&redirect_uri=http://127.0.0.1:8501&scope=user-read-private"
    def get_tok(self,code):
        r=requests.post("https://accounts.spotify.com/api/token", headers={'Authorization':f'Basic {base64.b64encode(f"{self.cid}:{self.csec}".encode()).decode()}'}, data={'grant_type':'authorization_code','code':code,'redirect_uri':'http://127.0.0.1:8501'})
        if r.status_code==200: 
            conn=sqlite3.connect('yangkidd_pro.db',check_same_thread=False); conn.execute("UPDATE api_credentials SET access_token=? WHERE platform='spotify'",(r.json()['access_token'],)); conn.commit(); conn.close(); return True
        return False
    def data(self):
        if not self.tok: return "No Token"
        try: 
            h={'Authorization':f'Bearer {self.tok}'}; a=requests.get(f"https://api.spotify.com/v1/search?q=YangKidd&type=artist&limit=1",headers=h).json()['artists']['items'][0]
            t=requests.get(f"https://api.spotify.com/v1/artists/{a['id']}/top-tracks?market=IT",headers=h).json()['tracks']
            return f"Followers:{a['followers']['total']}, Pop:{a['popularity']}\nTop:{[x['name'] for x in t[:3]]}"
        except: return "Error"

def ai_thread(msgs, sp_ctx, kb_ctx, soc_hist, resp):
    c=get_campaigns(); sp=c['spend'].sum() if not c.empty else 0; rv=c['revenue'].sum() if not c.empty else 0
    sys = f"SEI UN MANAGER. KB:{kb_ctx}. SPOTIFY:{sp_ctx}. ADS: Spend €{sp}, Rev €{rv}. SOCIAL TRENDS:\n{soc_hist}. Analizza correlazione Ads/Organico."
    try:
        for ch in ollama.chat(model="mistral-nemo", messages=[{'role':'system','content':sys}]+msgs, stream=True):
            resp['content']+=ch['message']['content']
        sqlite3.connect('yangkidd_pro.db').execute("INSERT INTO chat_history (session_id,role,content) VALUES ('MAIN','assistant',?)",(resp['content'],)).commit()
        resp['done']=True
    except Exception as e: resp['content']+=str(e); resp['done']=True

# --- UI ---
if 'init' not in st.session_state: st.session_state.update({'init':True,'messages':[],'thinking':False})
if not st.session_state.messages:
    conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
    st.session_state.messages = [{"role":r[0],"content":r[1]} for r in conn.execute("SELECT role,content FROM chat_history WHERE session_id='MAIN'").fetchall()]
    conn.close()

with st.sidebar:
    st.title("💎 ENTERPRISE OS")
    nav = st.radio("MENU", ["📈 Social Tracker", "💬 Strategy", "📚 Knowledge", "🔌 API", "⚙️ Ads"])

# --- MODULO SOCIAL TRACKER ---
if nav == "📈 Social Tracker":
    st.title("📈 Social Data Warehouse")
    
    # --- CHECK DATA STATUS (ALERT) ---
    last_date_str, df_summary = get_data_health()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if not last_date_str:
        st.markdown('<div class="alert-box alert-red">⚠️ NESSUN DATO NEL DATABASE. Inizia a caricare i CSV.</div>', unsafe_allow_html=True)
    elif last_date_str < today_str:
        st.markdown(f'<div class="alert-box alert-red">⚠️ DATI NON AGGIORNATI. Ultimo dato: {last_date_str}. Carica i CSV di oggi se hai pubblicato.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-box alert-green">✅ DATI AGGIORNATI A OGGI. Ottimo lavoro.</div>', unsafe_allow_html=True)

    # 1. MULTI-UPLOAD con LISTA FILE
    with st.expander("📂 Importazione CSV (Trascina qui tutti i file)", expanded=True):
        uploaded_files = st.file_uploader("Upload CSV", type=['csv','txt'], accept_multiple_files=True)
        
        col1, col2 = st.columns(2)
        plat = col1.selectbox("Piattaforma", ["Instagram", "TikTok", "Facebook", "YouTube"])
        
        # Mostra lista file pronti
        if uploaded_files:
            st.markdown(f"**📄 File pronti per l'analisi ({len(uploaded_files)}):**")
            for f in uploaded_files:
                metric_detected = detect_metric_from_filename(f.name)
                color = "green" if metric_detected != "Unknown" else "orange"
                st.markdown(f"- {f.name} -> <span style='color:{color}'>{metric_detected}</span>", unsafe_allow_html=True)
        
        if st.button("🚀 Elabora File"):
            if uploaded_files:
                log_text = ""
                total_saved = 0
                bar = st.progress(0)
                
                for i, file in enumerate(uploaded_files):
                    metric = detect_metric_from_filename(file.name)
                    if metric == "Unknown": metric = file.name
                    df, msg = smart_csv_loader(file)
                    if df is not None:
                        cnt, err_msg = save_social_bulk(df, plat, metric)
                        total_saved += cnt
                        log_text += f"✅ {file.name} [{metric}]: +{cnt} records\n"
                    else:
                        log_text += f"❌ {file.name}: {msg}\n"
                    bar.progress((i+1)/len(uploaded_files))
                
                st.success(f"Operazione completata! {total_saved} nuovi dati.")
                with st.expander("Log Dettagliato", expanded=False): st.text(log_text)
                time.sleep(1.5)
                st.rerun()

    # 2. INVENTARIO (TABELLA DEI BUCHI)
    st.divider()
    st.subheader("🗓️ Inventario Caricamenti (Controlla se manca qualcosa)")
    
    if not df_summary.empty:
        st.dataframe(df_summary, use_container_width=True)
    else:
        st.info("Nessun dato presente in inventario.")

    # 3. DATABASE COMPLETO
    with st.expander("🗄️ Visualizza Dati Grezzi Completi"):
        conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
        history_df = pd.read_sql_query("SELECT * FROM social_stats ORDER BY date_recorded DESC", conn)
        conn.close()
        
        if not history_df.empty:
            st.dataframe(history_df, use_container_width=True)
            if st.button("🗑️ RESET DB SOCIAL"):
                conn = sqlite3.connect('yangkidd_pro.db', check_same_thread=False)
                conn.execute("DELETE FROM social_stats")
                conn.commit(); conn.close()
                st.rerun()

# --- ALTRI MODULI ---
elif nav == "💬 Strategy":
    st.title("🧠 Strategy Room")
    if st.button("Reset"): 
        sqlite3.connect('yangkidd_pro.db').execute("DELETE FROM chat_history WHERE session_id='MAIN'").commit()
        st.session_state.messages=[]; st.rerun()
    for m in st.session_state.messages: st.chat_message(m["role"]).write(m["content"])
    if st.session_state.get('thinking'): st.chat_message("assistant").write(st.session_state.buf['content']+" ▌")
    if p:=st.chat_input():
        st.session_state.messages.append({"role":"user","content":p})
        sqlite3.connect('yangkidd_pro.db').execute("INSERT INTO chat_history (session_id,role,content) VALUES ('MAIN','user',?)",(p,)).commit()
        st.session_state.update({'thinking':True, 'buf':{'content':'','done':False}})
        sp=SpotifyAPI().data(); kb=get_knowledge_context()
        conn=sqlite3.connect('yangkidd_pro.db'); soc=pd.read_sql("SELECT * FROM social_stats ORDER BY date_recorded DESC LIMIT 30",conn).to_string(); conn.close()
        threading.Thread(target=ai_thread, args=(st.session_state.messages, sp, kb, soc, st.session_state.buf)).start()
        st.rerun()
    if st.session_state.get('thinking') and st.session_state.buf.get('done'):
        st.session_state.thinking=False; st.session_state.messages.append({"role":"assistant","content":st.session_state.buf['content']}); st.rerun()
    elif st.session_state.get('thinking'): time.sleep(0.1); st.rerun()

elif nav == "📚 Knowledge":
    st.title("Knowledge"); st.write(ingest_local_pdfs() if st.button("Scan PDF") else "")
    u=st.text_input("URL"); st.write(save_knowledge(*scrape_webpage(u)) if st.button("Scrape") and u else "")
    conn=sqlite3.connect('yangkidd_pro.db'); k=pd.read_sql("SELECT * FROM knowledge_base",conn); conn.close(); st.dataframe(k)

elif nav == "🔌 API":
    s=SpotifyAPI(); st.write(s.data() if s.tok else "No Token")
    if not s.tok:
        i=st.text_input("ID"); c=st.text_input("Secret"); 
        if st.button("Save"): s.save(i,c)
        if s.cid: st.write(f"[Login]({s.get_auth()})")
    if "code" in st.query_params: s.get_tok(st.query_params["code"]); st.rerun()

elif nav == "⚙️ Ads":
    st.title("Ads Manager")
    with st.form("a"):
        n=st.text_input("Name"); s=st.number_input("Spend"); r=st.number_input("Rev")
        if st.form_submit_button("Save"): save_campaign({'name':n,'platform':'Meta','spend':s,'revenue':r,'impressions':0,'streams':0}); st.rerun()
    st.dataframe(get_campaigns())