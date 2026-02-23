import json
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

# --- CONFIG ---
SALES_NEWS_FILE = Path(__file__).parent / "sales_news.json"
ARTICLE_TTL_DAYS = 14
MAX_SALES_ARTICLES = 150

# --- REGION DEFINITIONS (The "Brain") ---
REGION_MAP = {
    "usa": {
        "strong": ["fda", "usda", "walmart", "target store", "congress", "florida", "california", "bevnet"],
        "weak": ["usa", "united states", "american", "us market", "dollar", "usd"]
    },
    "uk": {
        "strong": ["tesco", "sainsbury", "asda", "waitrose", "the grocer", "hmrc", "london", "british soft drinks"],
        "weak": ["uk", "britain", "british", "gbp", "united kingdom"]
    },
    "germany": {
        "strong": ["rewe", "edeka", "aldi", "lidl germany", "getränke", "bundesrat", "berlin", "deutschland"],
        "weak": ["german", "germany", "dach", "euro"]
    },
    "france": {
        "strong": ["carrefour", "leclerc", "egalim", "paris", "danone", "boisson", "jus de fruits"],
        "weak": ["french", "france"]
    },
    "spain": {
        "strong": ["mercadona", "madrid", "barcelona", "zumo", "bebida", "horeca spain"],
        "weak": ["spanish", "spain", "espana"]
    },
    "italy": {
        "strong": ["esselunga", "conad", "milan", "rome", "succo", "aperitivo", "italia"],
        "weak": ["italian", "italy"]
    },
    "austria": {
        "strong": ["spar austria", "rauch", "red bull austria", "wien", "vienna", "hofer"],
        "weak": ["austria", "austrian", "oesterreich"]
    }
}

# --- UPDATED SOURCES ---
SALES_RSS_SOURCES = [
    # Global
    {"name": "BeverageDaily", "url": "https://www.beveragedaily.com/rss/editorial.rss", "regions": ["global"]},
    {"name": "Just-Drinks", "url": "https://www.just-drinks.com/feed/", "regions": ["global"]},
    # UK Specific
    {"name": "The Grocer UK", "url": "https://www.thegrocer.co.uk/34272.rss", "regions": ["uk"]},
    {"name": "GNews UK Beverages", "url": "
