import streamlit as st
import ollama
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from duckduckgo_search import DDGS
from datetime import datetime, timedelta
import time

# --- CONFIGURAZIONE PAGINA ---
st.set_page_config(page_title="YANGKIDD ENTERPRISE", page_icon="💎", layout="wide")

# --- DATABASE MANAGER (La parte "Pro" che mancava) ---
def init_db():
    conn = sqlite3.connect('yangkidd_marketing.db')
    c = conn.cursor()
    
    # Tabella Campagne
    c.execute('''CREATE TABLE IF NOT EXISTS campaigns
                 (id INTEGER PRIMARY KEY, name TEXT, platform TEXT, status TEXT, 
                  budget REAL, spend REAL, revenue REAL, roas REAL, date TEXT)''')
    
    # Tabella Competitor
    c.execute('''CREATE TABLE IF NOT EXISTS competitors
                 (id INTEGER PRIMARY KEY, name TEXT, platform TEXT, followers TEXT, 
                  sentiment TEXT, last_check TEXT)''')
    
    conn.commit()
    conn.close()

def save_campaign(name, platform, budget, spend, revenue):
    conn = sqlite3.connect('yangkidd_marketing.db')
    c = conn.cursor()
    roas = revenue / spend if spend > 0 else 0
    c.execute("INSERT INTO campaigns (name, platform, status, budget, spend, revenue, roas, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              (name, platform, "Active", budget, spend, revenue, roas, datetime.now().strftime("%Y-%m-%d")))
    conn.commit()
    conn.close()

def get_campaigns():
    conn = sqlite3.connect('yangkidd_marketing.db')
    df = pd.read_sql_query("SELECT * FROM campaigns", conn)
    conn.close()
    return df

def save_competitor(name, platform, followers, sentiment):
    conn = sqlite3.connect('yangkidd_marketing.db')
    c = conn.cursor()
    # Controlla se esiste già, se sì aggiorna
    c.execute("DELETE FROM competitors WHERE name=? AND platform=?", (name, platform))
    c.execute("INSERT INTO competitors (name, platform, followers, sentiment, last_check) VALUES (?, ?, ?, ?, ?)",
              (name, platform, followers, sentiment, datetime.now().strftime("%Y-%m-%d")))
    conn.commit()
    conn.close()

def get_competitors():
    conn = sqlite3.connect('yangkidd_marketing.db')
    df = pd.read_sql_query("SELECT * FROM competitors", conn)
    conn.close()
    return df

# Inizializza il DB all'avvio
init_db()

# --- STILE CYBERPUNK ---
st.markdown("""
<style>
    .stApp { background-color: #000000; color: #e0e0e0; }
    .stTextInput > div > div > input { background-color: #111; color: #00ff99; border: 1px solid #333; }
    .stSelectbox > div > div { background-color: #111; color: white; }
    h1, h2, h3 { font-family: 'Helvetica Neue', sans-serif; letter-spacing: -0.5px; }
    
    /* Metrics Cards */
    div[data-testid="metric-container"] {
        background-color: #111;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(0, 255, 153, 0.1);
    }
    
    /* Custom Button */
    .stButton > button {
        background: linear-gradient(45deg, #00ff99, #00cc88);
        color: black;
        font-weight: bold;
        border: none;
        width: 100%;
        padding: 0.5rem;
    }
    .stButton > button:hover {
        box-shadow: 0 0 15px rgba(0, 255, 153, 0.4);
    }
    
    /* Hide Streamlit menu */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- ENGINE AI ---
MODEL = "mistral-nemo"

def stream_ai(messages):
    try:
        stream = ollama.chat(model=MODEL, messages=messages, stream=True)
        for chunk in stream:
            yield chunk['message']['content']
    except Exception as e:
        yield f"⚠️ Errore AI: {str(e)}"

# --- TOOLS DI RICERCA ---
def web_search(query, max_res=8):
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_res):
                results.append(r)
        return results
    except:
        return []

# --- INTERFACCIA ---

# Sidebar
with st.sidebar:
    st.title("💎 ENTERPRISE OS")
    st.caption("Local • Persistent • AI")
    nav = st.radio("SISTEMA", ["Dashboard (ROI)", "AI War Room", "Competitor Tracker", "Campaign Manager"])
    st.divider()
    
    # KPI Veloci
    df_c = get_campaigns()
    if not df_c.empty:
        tot_spend = df_c['spend'].sum()
        tot_rev = df_c['revenue'].sum()
        roi_tot = ((tot_rev - tot_spend) / tot_spend * 100) if tot_spend > 0 else 0
        st.metric("Total Spend", f"€{tot_spend:,.0f}")
        st.metric("Total Revenue", f"€{tot_rev:,.0f}")
        st.metric("Global ROI", f"{roi_tot:+.1f}%", delta_color="normal")

# --- MODULO 1: DASHBOARD (Visualizzazione Dati) ---
if nav == "Dashboard (ROI)":
    st.title("📊 Financial Command Center")
    
    df = get_campaigns()
    
    if df.empty:
        st.info("Nessuna campagna salvata. Vai su 'Campaign Manager' per inserirne una.")
    else:
        # Top Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Campagne Attive", len(df))
        c2.metric("Best ROAS", f"{df['roas'].max():.2f}x")
        c3.metric("Worst ROAS", f"{df['roas'].min():.2f}x")
        c4.metric("Avg. Budget", f"€{df['budget'].mean():.0f}")
        
        # Grafico 1: Performance Platform
        col_g1, col_g2 = st.columns(2)
        
        with col_g1:
            st.subheader("Performance per Piattaforma")
            fig_bar = px.bar(df, x="platform", y="roas", color="platform", 
                             title="ROAS per Piattaforma", template="plotly_dark",
                             color_discrete_sequence=["#00ff99", "#00ccff", "#ff00ff"])
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with col_g2:
            st.subheader("Spend vs Revenue")
            fig_scat = px.scatter(df, x="spend", y="revenue", size="budget", color="platform",
                                  hover_name="name", title="Efficienza Campagne", template="plotly_dark")
            st.plotly_chart(fig_scat, use_container_width=True)
            
        # Tabella Dati
        st.dataframe(df, use_container_width=True)

# --- MODULO 2: AI WAR ROOM (Strategia) ---
elif nav == "AI War Room":
    st.title("💬 Strategic AI Chat")
    
    # Inietta contesto dal database
    df_context = get_campaigns().to_string() if not get_campaigns().empty else "Nessun dato storico."
    
    if "messages" not in st.session_state:
        st.session_state.messages = [{
            "role": "system", 
            "content": f"Sei il Manager di YangKidd. Hai accesso a questi dati storici delle campagne: {df_context}. Usa questi dati per dare consigli basati sui numeri. Sii breve e diretto."
        }]

    # Mostra chat
    for msg in st.session_state.messages:
        if msg["role"] != "system":
            with st.chat_message(msg["role"]):
                st.markdown(msg.content)

    if prompt := st.chat_input("Chiedi strategia, analisi o copy..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
            
        with st.chat_message("assistant"):
            res_placeholder = st.empty()
            full_res = ""
            for chunk in stream_ai(st.session_state.messages):
                full_res += chunk
                res_placeholder.markdown(full_res + "▌")
            res_placeholder.markdown(full_res)
        
        st.session_state.messages.append({"role": "assistant", "content": full_res})

# --- MODULO 3: COMPETITOR TRACKER ---
elif nav == "Competitor Tracker":
    st.title("👁️ Competitor Intelligence")
    
    c1, c2 = st.columns([3, 1])
    target = c1.text_input("Competitor da tracciare", placeholder="Es: Lazza, Sfera, Geolier")
    if c2.button("Analizza & Salva"):
        with st.spinner(f"Analizzando {target}..."):
            # 1. Cerca dati
            res = web_search(f"{target} instagram followers spotify listeners stats", 5)
            
            # 2. AI Estrae i dati strutturati
            prompt = f"Dai seguenti risultati web su {target}, estrai: 1. Numero Followers (stima), 2. Sentiment (Positivo/Neutro/Negativo). Rispondi SOLO nel formato: 'FOLLOWERS|SENTIMENT'. Dati: {str(res)}"
            ai_extraction = ollama.chat(model=MODEL, messages=[{"role": "user", "content": prompt}])['message']['content']
            
            try:
                parts = ai_extraction.split("|")
                foll = parts[0].strip()
                sent = parts[1].strip()
            except:
                foll = "N/A"
                sent = "Neutro"
                
            # 3. Salva nel DB
            save_competitor(target, "Social/Music", foll, sent)
            st.success(f"Tracciato: {target} | {foll} | {sent}")
            
    # Mostra tabella competitor salvati
    st.subheader("Database Competitor")
    df_comp = get_competitors()
    if not df_comp.empty:
        st.dataframe(df_comp, use_container_width=True)
        
        # Grafico Sentiment
        fig = px.pie(df_comp, names='sentiment', title='Competitor Sentiment Analysis', 
                     template="plotly_dark", color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig, use_container_width=True)

# --- MODULO 4: CAMPAIGN MANAGER (Input Dati) ---
elif nav == "Campaign Manager":
    st.title("⚙️ Campaign Builder")
    st.info("I dati inseriti qui verranno salvati nel database locale (yangkidd_marketing.db)")
    
    with st.form("new_campaign"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Nome Campagna", "Lancio Singolo")
        plat = c2.selectbox("Piattaforma", ["Meta Ads", "TikTok Ads", "Spotify Marquee", "YouTube Ads"])
        
        c3, c4, c5 = st.columns(3)
        bud = c3.number_input("Budget (€)", 100.0)
        spd = c4.number_input("Spesa Effettiva (€)", 100.0)
        rev = c5.number_input("Ritorno/Valore (€)", 150.0)
        
        if st.form_submit_button("💾 Salva nel Database"):
            save_campaign(name, plat, bud, spd, rev)
            st.success("Campagna salvata con successo!")
            time.sleep(1)
            st.rerun()

    st.markdown("---")
    st.subheader("Storico Campagne")
    st.dataframe(get_campaigns(), use_container_width=True)