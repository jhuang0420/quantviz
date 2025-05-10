import json
import os
from pathlib import Path

def load_alpaca_keys():
    secrets_path = Path(__file__).parent.parent.parent / "config" / "secrets.json"
    
    try:
        with open(secrets_path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise Exception("secrets.json not found! Create it in config/ folder")
    