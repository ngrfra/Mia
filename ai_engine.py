import ollama
import sqlite3
from database import get_connection
from campaign_logic import get_campaigns

def load_chat_history():
    conn = get_connection()
    rows = conn.execute("SELECT role, content FROM chat_history WHERE session_id='MAIN' ORDER BY id ASC").fetchall()
    conn.close()
    return [{"role": r[0], "content": r[1]} for r in rows]

def save_chat_message(role, content):
    conn = get_connection()
    conn.execute("INSERT INTO chat_history (session_id, role, content) VALUES (?,?,?)", ('MAIN', role, content))
    conn.commit(); conn.close()

def clear_chat_history():
    conn = get_connection()
    conn.execute("DELETE FROM chat_history WHERE session_id='MAIN'"); conn.commit(); conn.close()

def ai_thread(msgs, sp_ctx, kb_ctx, soc_hist, resp):
    c=get_campaigns(); sp=c['spend'].sum() if not c.empty else 0; rv=c['revenue'].sum() if not c.empty else 0
    
    sys = f"""SEI UN MANAGER DI ETICHETTA DISCOGRAFICA (Data-Driven).
    
    1. TUA CONOSCENZA (Libri/PDF):
    {kb_ctx}
    
    2. DATI PIATTAFORME:
    - Spotify: {sp_ctx}
    - Ads Spend: €{sp}, Revenue: €{rv}
    
    3. TREND SOCIAL (Ultimi dati caricati):
    {soc_hist}
    
    OBIETTIVO:
    Analizza se la crescita social (punto 3) giustifica la spesa ads (punto 2).
    Sii critico e usa i dati.
    """
    
    try:
        # Usa un modello veloce se mistral-nemo è pesante, altrimenti lascia mistral-nemo
        for ch in ollama.chat(model="mistral-nemo", messages=[{'role':'system','content':sys}]+msgs, stream=True):
            resp['content']+=ch['message']['content']
        save_chat_message('assistant', resp['content'])
        resp['done']=True
    except Exception as e: resp['content']+=f"Errore AI: {str(e)}"; resp['done']=True