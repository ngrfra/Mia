import streamlit as st
import ollama
import sqlite3
import time

# --- CONFIGURAZIONE ---
st.set_page_config(page_title="YANGKIDD CHAT CORE", page_icon="🧠", layout="centered")

# --- STILE DARK ---
st.markdown("""
<style>
    .stApp { background-color: #0d0d0d; color: #e0e0e0; }
    .stTextInput > div > div > input { background-color: #1a1a1a; color: white; border: 1px solid #333; }
    .stChatMessage { background-color: #1a1a1a; border-radius: 10px; border: 1px solid #333; }
    h1 { color: #00ff88; }
</style>
""", unsafe_allow_html=True)

# --- DATABASE MANAGER (MEMORIA ETERNA) ---
def init_chat_db():
    conn = sqlite3.connect('yangkidd_chat.db')
    c = conn.cursor()
    # Tabella per salvare la cronologia
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  role TEXT, 
                  content TEXT, 
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

def save_message(role, content):
    conn = sqlite3.connect('yangkidd_chat.db')
    c = conn.cursor()
    c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", (role, content))
    conn.commit()
    conn.close()

def load_history():
    conn = sqlite3.connect('yangkidd_chat.db')
    # Carica gli ultimi 50 messaggi per dare contesto ma non intasare
    messages = []
    cursor = conn.cursor()
    cursor.execute("SELECT role, content FROM messages ORDER BY id ASC")
    rows = cursor.fetchall()
    conn.close()
    
    # Trasforma le righe del DB in dizionari per la sessione
    for row in rows:
        messages.append({"role": row[0], "content": row[1]})
    return messages

def clear_history():
    conn = sqlite3.connect('yangkidd_chat.db')
    c = conn.cursor()
    c.execute("DELETE FROM messages")
    conn.commit()
    conn.close()

# Inizializza DB
init_chat_db()

# --- MOTORE AI ---
MODEL = "mistral-nemo" # Assicurati di averlo installato

def stream_ai_response(messages):
    """Chiama Ollama e genera risposta in streaming"""
    try:
        stream = ollama.chat(model=MODEL, messages=messages, stream=True)
        for chunk in stream:
            yield chunk['message']['content']
    except Exception as e:
        yield f"⚠️ Errore AI: {str(e)}. Controlla che Ollama sia aperto."

# --- INTERFACCIA ---
st.title("🧠 Strategic AI Chat")
st.caption("Memoria Persistente Attiva • Database Locale")

# 1. Carica la storia dal Database (se la sessione è vuota)
if "messages" not in st.session_state or len(st.session_state.messages) == 0:
    history = load_history()
    if not history:
        # Messaggio di benvenuto se il DB è vuoto
        welcome_msg = "Ciao YangKidd. Sono il tuo AI Manager. I nostri dati sono al sicuro nel database locale. Su cosa lavoriamo oggi?"
        save_message("assistant", welcome_msg)
        st.session_state.messages = [{"role": "assistant", "content": welcome_msg}]
    else:
        st.session_state.messages = history

# 2. Visualizza la chat (FIX DELL'ERRORE PRECEDENTE)
# Qui usiamo msg['content'] perché siamo sicuri che sia un dizionario, non un oggetto
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# 3. Input Utente
if prompt := st.chat_input("Scrivi qui la tua strategia..."):
    
    # A. Visualizza e Salva messaggio utente
    st.session_state.messages.append({"role": "user", "content": prompt})
    save_message("user", prompt) # Salva nel DB
    with st.chat_message("user"):
        st.markdown(prompt)

    # B. Genera risposta AI
    with st.chat_message("assistant"):
        response_placeholder = st.empty()
        full_response = ""
        
        # Prepara il contesto per l'AI (aggiungiamo un system prompt invisibile)
        context_messages = [
            {"role": "system", "content": "Sei un Manager Discografico esperto e spietato. Parli italiano. Sei focalizzato su: numeri, ROI, strategie di crescita aggressive e analisi dati. Non dare risposte generiche. Se non sai un dato, chiedilo. Rispondi in modo conciso."}
        ] + st.session_state.messages # Aggiunge tutta la storia recente
        
        # Streaming
        for chunk in stream_ai_response(context_messages):
            full_response += chunk
            response_placeholder.markdown(full_response + "▌")
        
        response_placeholder.markdown(full_response)
    
    # C. Salva risposta AI
    st.session_state.messages.append({"role": "assistant", "content": full_response})
    save_message("assistant", full_response) # Salva nel DB

# --- SIDEBAR PER GESTIONE ---
with st.sidebar:
    st.header("⚙️ Gestione Memoria")
    if st.button("🗑️ Cancella Tutta la Memoria"):
        clear_history()
        st.session_state.messages = []
        st.rerun()
    
    st.info("Ogni messaggio viene salvato automaticamente nel file 'yangkidd_chat.db'. Puoi chiudere e riaprire quando vuoi.")