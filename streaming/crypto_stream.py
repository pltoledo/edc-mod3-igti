import json
import requests
import os
import time
import sys
from dotenv import load_dotenv

load_dotenv()

def create_dirs(dirpath):
    """Creating directories."""
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

def join_path(*args):
    return os.path.join(*args).replace("\\", "/")

class CryptoStream:
    
    def __init__(self):
        self.params = {
            "accept": "application/json", 
            "X-API-Token": os.environ['API_TOKEN'],
        }
        self.base_url = "https://api.blockchain.com/v3/exchange/l2/"
        self.raw_path = 'data/btc_stream/raw'
        create_dirs(self.raw_path)
        
    def get_data(self, url):
        r = requests.get(url, params=self.params)
        if r.status_code == 200:
            return r.json()
        else:
            print('Error')
        
    def write_data(self, symbol, call_id):
        url = self.base_url + symbol
        data = self.get_data(url)
        save_path = join_path(self.raw_path, f'data_{call_id}.json')
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
    def start(self, symbol):
        calls = 0
        while calls < 600:
            self.write_data(symbol, calls)
            calls += 1
            time.sleep(.5)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        stream = CryptoStream()
        stream.start(sys.argv[1])