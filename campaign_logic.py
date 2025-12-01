import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from database import get_connection

def get_campaigns():
    conn = get_connection()
    try:
        # Recupera campagne ordinate per data inizio
        df = pd.read_sql_query("SELECT * FROM campaigns ORDER BY start_date DESC", conn)
        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()

def save_campaign(d):
    """
    Salva la campagna con logica di business intelligente.
    Se revenue è 0 ma ci sono streams, stima il guadagno (Spotify avg ~0.003).
    """
    conn = get_connection()
    
    # 1. AUTO-CALCOLO REVENUE DA STREAMS (Se non inserito esplicitamente)
    estimated_revenue = d['revenue']
    if d['revenue'] == 0 and d['streams'] > 0:
        estimated_revenue = d['streams'] * 0.003 # Stima media industria
        
    # 2. CALCOLO ROAS (Return on Ad Spend)
    roas = 0
    if d['spend'] > 0:
        roas = estimated_revenue / d['spend']
        
    # 3. GESTIONE DATE (Default a oggi se mancano)
    s_date = d.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    e_date = d.get('end_date', (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d'))

    try:
        # Query dinamica che crea la tabella se non esiste con le nuove colonne data
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                platform TEXT,
                status TEXT,
                budget REAL,
                spend REAL,
                revenue REAL,
                roas REAL,
                impressions INTEGER,
                clicks INTEGER,
                streams INTEGER,
                start_date TEXT,
                end_date TEXT
            )
        """)
        
        conn.execute("""
            INSERT INTO campaigns (name, platform, status, budget, spend, revenue, roas, impressions, clicks, streams, start_date, end_date) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (d['name'], d['platform'], 'Active', d['budget'], d['spend'], estimated_revenue, roas, d['impressions'], 0, d['streams'], s_date, e_date))
        
        conn.commit()
        return True, "Campagna salvata correttamente"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def analyze_campaign_impact(campaign_id):
    """
    INCROCIO DATI: Cerca correlazioni tra la campagna Ads e la crescita organica sui social.
    """
    conn = get_connection()
    try:
        # 1. Prendi dati campagna
        camp = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
        if not camp: return None
        
        # Accesso per indice (row factory standard è tuple)
        # id=0, name=1, plat=2, ..., start=11, end=12 (basato su insert sopra)
        # Per sicurezza uso query pandas
        df_c = pd.read_sql_query(f"SELECT * FROM campaigns WHERE id={campaign_id}", conn)
        if df_c.empty: return None
        
        c_row = df_c.iloc[0]
        start_date = c_row['start_date']
        end_date = c_row['end_date']
        platform = c_row['platform']
        
        # 2. Cerca dati social in quel periodo per la stessa piattaforma
        # Esempio: Se ho fatto Ads su TikTok, voglio vedere se i follower TikTok sono saliti
        query_social = f"""
            SELECT metric_type, SUM(value) as total_val 
            FROM social_stats 
            WHERE platform = '{platform}' 
            AND date_recorded BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY metric_type
        """
        df_impact = pd.read_sql_query(query_social, conn)
        
        return {
            "campaign": c_row['name'],
            "period": f"{start_date} -> {end_date}",
            "impact_data": df_impact
        }
    except Exception as e:
        return None
    finally:
        conn.close()