import requests
import base64
from database import get_connection

class SpotifyAPI:
    # URL UFFICIALI SPOTIFY
    AUTH_URL = "https://accounts.spotify.com/authorize"
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    BASE_URL = "https://api.spotify.com/v1"

    def __init__(self): 
        conn=get_connection(); r=conn.execute("SELECT client_id,client_secret,access_token FROM api_credentials WHERE platform='spotify'").fetchone(); conn.close()
        self.cid,self.csec,self.tok=r if r else (None,None,None)
        
    def save(self,i,s): 
        conn=get_connection(); conn.execute("INSERT OR REPLACE INTO api_credentials (platform,client_id,client_secret) VALUES ('spotify',?,?)",(i,s)); conn.commit(); conn.close()
        
    def get_auth(self): 
        # Redirect URI deve combaciare con quello nelle impostazioni developer di Spotify
        return f"{self.AUTH_URL}?client_id={self.cid}&response_type=code&redirect_uri=http://127.0.0.1:8501&scope=user-read-private%20user-read-email%20user-top-read"
        
    def get_tok(self,code):
        auth_str = base64.b64encode(f"{self.cid}:{self.csec}".encode()).decode()
        headers = {'Authorization': f'Basic {auth_str}', 'Content-Type': 'application/x-www-form-urlencoded'}
        data = {'grant_type': 'authorization_code', 'code': code, 'redirect_uri': 'http://127.0.0.1:8501'}
        
        try:
            r=requests.post(self.TOKEN_URL, headers=headers, data=data)
            if r.status_code==200: 
                token = r.json()['access_token']
                conn=get_connection(); conn.execute("UPDATE api_credentials SET access_token=? WHERE platform='spotify'",(token,)); conn.commit(); conn.close()
                return True
            return False
        except: return False
        
    def data(self):
        if not self.tok: return "Spotify non connesso."
        try: 
            h={'Authorization':f'Bearer {self.tok}'}
            # Cerca l'artista (es. "YangKidd" o il nome dell'account)
            me = requests.get(f"{self.BASE_URL}/me", headers=h).json()
            # Se è un account utente, cerca l'artista
            search = requests.get(f"{self.BASE_URL}/search?q=YangKidd&type=artist&limit=1", headers=h)
            
            if search.status_code == 200 and search.json()['artists']['items']:
                a = search.json()['artists']['items'][0]
                t = requests.get(f"{self.BASE_URL}/artists/{a['id']}/top-tracks?market=IT", headers=h).json().get('tracks', [])
                top_str = ", ".join([x['name'] for x in t[:3]])
                return f"Followers: {a['followers']['total']}, Popolarità: {a['popularity']}/100. Top Tracks: {top_str}"
            else:
                return "Artista non trovato su Spotify."
        except Exception as e: return f"Errore API: {str(e)}"