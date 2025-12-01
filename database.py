import sqlite3
import pandas as pd

DB_NAME = "enterprise_os.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_advanced_db():
    conn = get_connection()
    c = conn.cursor()
    
    # 1. TABELLA STATISTICHE GENERALI (Esistente)
    c.execute('''CREATE TABLE IF NOT EXISTS social_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT,
                    metric_type TEXT,
                    value REAL,
                    date_recorded DATE,
                    source_type TEXT
                )''')
    
    # 2. TABELLA LOG UPLOAD (Esistente)
    c.execute('''CREATE TABLE IF NOT EXISTS upload_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT,
                    platform TEXT,
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT
                )''')

    # 3. TABELLA KNOWLEDGE BASE (Esistente)
    c.execute('''CREATE TABLE IF NOT EXISTS knowledge_base (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT,
                    content TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
    
    # 4. TABELLA CAMPAGNE ADS (Esistente)
    c.execute('''CREATE TABLE IF NOT EXISTS campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    platform TEXT,
                    budget REAL,
                    spend REAL,
                    revenue REAL,
                    streams INTEGER,
                    impressions INTEGER,
                    start_date DATE,
                    end_date DATE
                )''')

    # --- NUOVE TABELLE (CONTENT INTELLIGENCE) ---

    # 5. INVENTARIO POST (Chi è il post?)
    c.execute('''CREATE TABLE IF NOT EXISTS posts_inventory (
                    post_id TEXT PRIMARY KEY,
                    platform TEXT,
                    date_published DATE,
                    caption TEXT,
                    link TEXT,
                    content_type TEXT,
                    duration INTEGER
                )''')

    # 6. PERFORMANCE POST (Come va il post nel tempo?)
    c.execute('''CREATE TABLE IF NOT EXISTS posts_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT,
                    date_recorded DATE,
                    views INTEGER,
                    likes INTEGER,
                    comments INTEGER,
                    shares INTEGER,
                    FOREIGN KEY(post_id) REFERENCES posts_inventory(post_id)
                )''')

    # 7. CHAT HISTORY (Fix per l'errore che avevi)
    c.execute('''CREATE TABLE IF NOT EXISTS chat_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    role TEXT,
                    content TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
    
    conn.commit()
    conn.close()