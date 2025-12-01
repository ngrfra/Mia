import streamlit as st
import pandas as pd
import threading
import time
import uuid
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# IMPORT MODULI
from database import init_advanced_db, get_connection
from social_logic import smart_csv_loader, detect_metric_from_filename, save_social_bulk, get_data_health, check_file_log, log_upload_event, get_file_upload_history, get_content_health
from knowledge_logic import ingest_local_pdfs, scrape_webpage, save_knowledge, get_knowledge_context
from campaign_logic import get_campaigns, save_campaign
from spotify_client import SpotifyAPI
from ai_engine import ai_thread, load_chat_history, save_chat_message, clear_chat_history

st.set_page_config(page_title="YANGKIDD ENTERPRISE OS", page_icon="💎", layout="wide")
st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 100%); color: #e0e0e0; }
    .stTextInput > div > div > input { background-color: #1a1a1a; color: #00ff99; border: 2px solid #333; }
    [data-testid="stDataFrame"] { background-color: #1a1a1a; }
    .metric-card { background: #222; padding: 15px; border-radius: 10px; border-left: 5px solid #00ff99; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    h1, h2, h3 { color: #00ff99 !important; }
</style>
""", unsafe_allow_html=True)

init_advanced_db()
if 'init' not in st.session_state:
    try: hist = load_chat_history()
    except: hist = []
    st.session_state.update({'init':True, 'messages':hist, 'thinking':False})

# --- SIDEBAR & FILTRI ---
with st.sidebar:
    st.title("💎 ENTERPRISE OS")
    nav = st.radio("MENU", ["📈 Social Tracker", "💬 Strategy", "📚 Knowledge", "🔌 API", "⚙️ Ads"])
    
    st.divider()
    st.header("🔍 Filtri Globali")
    
    # Initialize DataFrames
    df_stats_filtered = pd.DataFrame()
    df_content_filtered = pd.DataFrame()
    df_time = pd.DataFrame()
    df_demo = pd.DataFrame()
    
    _, df_stats_all = get_data_health()
    if not df_stats_all.empty: df_stats_all.columns = ['date_recorded', 'platform', 'metric_type', 'value']
    df_content_all = get_content_health()
    
    # Recupera tutte le piattaforme uniche dal DB
    plats_1 = df_stats_all['platform'].unique().tolist() if not df_stats_all.empty else []
    plats_2 = df_content_all['platform'].unique().tolist() if not df_content_all.empty else []
    available_plats = sorted(list(set(plats_1 + plats_2)))
    
    if available_plats:
        sel_plats = st.multiselect("Piattaforme Visualizzate", available_plats, default=available_plats)
    else:
        sel_plats = []
        st.caption("Nessun dato. Carica file CSV.")

# --- SOCIAL TRACKER ---
if nav == "📈 Social Tracker":
    st.title("📈 Social Data Warehouse")
    last_date, _ = get_data_health()
    
    if not last_date: st.error("⚠️ Database Vuoto.")
    else: st.success(f"✅ Dati aggiornati al {last_date}")

    st.subheader("📊 Analisi Visuale")
    
    # FILTRAGGIO DATI (SE CI SONO)
    if not df_stats_all.empty:
        df_stats_filtered = df_stats_all[df_stats_all['platform'].isin(sel_plats)]
        if not df_stats_filtered.empty:
            df_stats_filtered['date_recorded'] = pd.to_datetime(df_stats_filtered['date_recorded'])
            is_demo = df_stats_filtered['metric_type'].str.contains("Audience", case=False, na=False)
            df_time = df_stats_filtered[~is_demo]
            df_demo = df_stats_filtered[is_demo]

    if not df_content_all.empty:
        df_content_filtered = df_content_all[df_content_all['platform'].isin(sel_plats)]

    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["📉 Trend", "🎬 Content", "👥 Demografica", "🔢 Dati"])
    
    with tab1:
        if not df_time.empty:
            c1, c2 = st.columns([1,3])
            with c1:
                metrics = sorted(df_time['metric_type'].unique())
                main_met = [m for m in metrics if "Active H" not in m]
                sel_met = st.multiselect("Metriche (Max 2)", metrics, default=main_met[:2], max_selections=2)
            with c2:
                if len(sel_met) > 0:
                    df_p = df_time[df_time['metric_type'].isin(sel_met)]
                    if len(sel_met) == 1:
                        fig = px.line(df_p, x='date_recorded', y='value', color='platform', markers=True, template="plotly_dark", title=sel_met[0])
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        fig = make_subplots(specs=[[{"secondary_y": True}]])
                        cols = ['#00ff99', '#ff00ff', '#00ccff', '#ffff00']
                        m1, m2 = sel_met[0], sel_met[1]
                        
                        for i, p in enumerate(sel_plats):
                            d1 = df_p[(df_p['platform']==p) & (df_p['metric_type']==m1)]
                            if not d1.empty:
                                fig.add_trace(go.Scatter(x=d1['date_recorded'], y=d1['value'], name=f"{p}-{m1}", line=dict(color=cols[i%4], width=3)), secondary_y=False)
                            d2 = df_p[(df_p['platform']==p) & (df_p['metric_type']==m2)]
                            if not d2.empty:
                                fig.add_trace(go.Scatter(x=d2['date_recorded'], y=d2['value'], name=f"{p}-{m2}", line=dict(color=cols[i%4], width=2, dash='dot')), secondary_y=True)
                        fig.update_layout(template="plotly_dark", hovermode="x unified")
                        st.plotly_chart(fig, use_container_width=True)
        else: st.info("Nessun dato temporale.")

    with tab2:
        if not df_content_filtered.empty:
            fig = px.scatter(df_content_filtered, x='date_published', y='views', size='likes', color='platform', hover_data=['caption'], template="plotly_dark", title="Content Performance")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_content_filtered.sort_values('views', ascending=False).head(10)[['platform','caption','views','likes']], use_container_width=True)
        else: st.info("Nessun contenuto.")

    with tab3:
        if not df_demo.empty:
            latest = df_demo['date_recorded'].max()
            df_s = df_demo[df_demo['date_recorded'] == latest]
            c1, c2 = st.columns(2)
            with c1:
                df_g = df_s[df_s['metric_type'].str.contains("Gender")]
                if not df_g.empty:
                    df_g['label'] = df_g['metric_type'].str.replace("Audience Gender ", "")
                    st.plotly_chart(px.pie(df_g, values='value', names='label', facet_col='platform', title="Genere", template="plotly_dark"), use_container_width=True)
            with c2:
                df_geo = df_s[df_s['metric_type'].str.contains("Geo")]
                if not df_geo.empty:
                    df_geo['label'] = df_geo['metric_type'].str.replace("Audience Geo ", "")
                    st.plotly_chart(px.bar(df_geo, x='label', y='value', color='platform', barmode='group', title="Geo", template="plotly_dark"), use_container_width=True)
        else: st.info("Nessun dato demografico.")

    with tab4: st.dataframe(df_stats_filtered, use_container_width=True)

    # UPLOAD
    with st.expander("📂 Upload CSV", expanded=True):
        if "uploader_key" not in st.session_state: st.session_state["uploader_key"] = str(uuid.uuid4())
        up_files = st.file_uploader("Trascina file", accept_multiple_files=True, key=st.session_state["uploader_key"])
        c1, c2 = st.columns(2)
        plat = c1.selectbox("Piattaforma", ["Instagram", "TikTok", "Facebook", "YouTube", "Meta Ads"])
        force = c2.checkbox("Forza reload")
        
        if st.button("🚀 Elabora"):
            cnt = 0
            bar = st.progress(0)
            for i, f in enumerate(up_files):
                exists, _ = check_file_log(f.name, plat)
                
                # SE FORZA RELOAD E' ATTIVO, IGNORIAMO LO STORICO
                if exists and not force: 
                    st.toast(f"⏭️ Saltato {f.name}")
                else:
                    m = detect_metric_from_filename(f.name)
                    df, msg = smart_csv_loader(f)
                    
                    if df is not None:
                        rows, msg = save_social_bulk(df, plat, m)
                        if rows > 0: 
                            log_upload_event(f.name, plat, f"OK ({rows})")
                            cnt += 1
                            st.toast(f"✅ {f.name}: {rows} rows")
                        else:
                            st.error(f"❌ {f.name}: {msg}") # MOSTRA ERRORE ESPLICITO
                    else:
                        st.error(f"❌ {f.name}: {msg}") # MOSTRA ERRORE CSV
                        
            bar.progress(100)
            if cnt > 0:
                st.success(f"Fatto! {cnt} file elaborati.")
                time.sleep(2)
                st.session_state["uploader_key"] = str(uuid.uuid4())
                st.rerun()
    
    # RESET BUTTON
    if st.button("🗑️ RESET DATABASE"):
        conn = get_connection()
        conn.execute("DELETE FROM social_stats"); conn.execute("DELETE FROM upload_logs"); 
        conn.execute("DELETE FROM posts_inventory"); conn.execute("DELETE FROM posts_performance");
        conn.commit(); conn.close()
        st.warning("Database pulito.")
        time.sleep(1); st.rerun()

# --- ALTRE PAGINE ---
elif nav == "💬 Strategy":
    st.title("🧠 Strategy Room")
    if st.button("Clear Chat"): clear_chat_history(); st.session_state.messages=[]; st.rerun()
    for m in st.session_state.messages: st.chat_message(m["role"]).write(m["content"])
    if p:=st.chat_input():
        st.session_state.messages.append({"role":"user","content":p})
        save_chat_message("user",p)
        threading.Thread(target=ai_thread, args=(st.session_state.messages,"","", "", st.session_state.buf if 'buf' in st.session_state else None)).start()
        st.rerun()

elif nav == "📚 Knowledge":
    st.title("Knowledge Base")
    t1, t2 = st.tabs(["PDF", "Web"])
    with t1: st.write(ingest_local_pdfs() if st.button("Scan PDF") else "")
    with t2:
        u = st.text_input("URL")
        if st.button("Scrape") and u: save_knowledge("WEB: "+u, scrape_webpage(u)[1]); st.success("OK")
    conn=get_connection(); k=pd.read_sql("SELECT * FROM knowledge_base",conn); conn.close(); st.dataframe(k)

elif nav == "🔌 API":
    st.title("API Connect")
    s=SpotifyAPI(); st.write(s.data() if s.tok else "No Token")
    if not s.tok:
        i=st.text_input("ID"); c=st.text_input("Secret")
        if st.button("Save"): s.save(i,c)
        if s.cid: st.markdown(f"[Login]({s.get_auth()})")
    if "code" in st.query_params: s.get_tok(st.query_params["code"]); st.rerun()

elif nav == "⚙️ Ads":
    st.title("Ads Manager")
    with st.form("ads"):
        n=st.text_input("Name"); s=st.number_input("Spend"); r=st.number_input("Rev")
        if st.form_submit_button("Save"): save_campaign({'name':n,'platform':'Meta','spend':s,'revenue':r,'impressions':0,'streams':0}); st.rerun()
    st.dataframe(get_campaigns())