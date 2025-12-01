import os
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from database import get_connection

PDF_FOLDER = "knowledge_docs"

def ingest_local_pdfs():
    if not os.path.exists(PDF_FOLDER): os.makedirs(PDF_FOLDER); return "Cartella creata."
    files = [f for f in os.listdir(PDF_FOLDER) if f.endswith('.pdf')]
    if not files: return "Nessun PDF."
    conn = get_connection()
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
    conn=get_connection(); conn.execute("INSERT INTO knowledge_base (source,content) VALUES (?,?)",(s,c)); conn.commit(); conn.close()

def get_knowledge_context():
    conn=get_connection(); r=conn.execute("SELECT source,content FROM knowledge_base").fetchall(); conn.close()
    return "\n".join([f"-- {x[0]} --\n{x[1][:2000]}" for x in r]) if r else ""