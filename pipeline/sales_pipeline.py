#!/usr/bin/env python3
"""
pipeline/sales_pipeline.py — Beverage Sales Intelligence Pipeline v2

FIXES from v1:
  - Global articles now shared to ALL regions (not lost)
  - Wine/spirits/cocktails excluded (tighter relevance)
  - Product tags and why_it_matters actually populated
  - Region signals always show real data
  - Single pipeline — no conflicts with old news_fetcher

Produces: sales_news.json, market_stats.json, sales_briefings.json,
          data_health.json, briefing.json

Free sources only. No paid APIs. No AI calls.
"""

import json, hashlib, re, math
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from difflib import SequenceMatcher
from urllib.parse import urlparse, urlunparse, quote

try:
    import requests
except ImportError:
    raise SystemExit("pip install requests")

try:
    from bs4 import BeautifulSoup
    BS4 = True
except ImportError:
    BS4 = False

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
OUT_DIR         = Path(__file__).parent.parent
WINDOW_DAYS     = 28
FALLBACK_DAYS   = 42
MIN_ITEMS       = 10
TIMEOUT         = 12
NOW             = datetime.now(timezone.utc)
CUTOFF          = NOW - timedelta(days=WINDOW_DAYS)
FALLBACK_CUTOFF = NOW - timedelta(days=FALLBACK_DAYS)

# ═══════════════════════════════════════════════════════════════
# REGIONS
# ═══════════════════════════════════════════════════════════════
REGIONS = {
    "usa": {
        "name": "United States", "flag": "\U0001f1fa\U0001f1f8", "currency": "USD",
        "kw": ["usa", "united states", "u.s.", "american", "fda",
               "north america", "us market", "us beverage"],
        "neg": ["australia", "australian"],
    },
    "germany": {
        "name": "Germany", "flag": "\U0001f1e9\U0001f1ea", "currency": "EUR",
        "kw": ["germany", "german", "deutschland", "dach",
               "bundesrat", "lebensmittel", "aldi", "lidl", "rewe", "edeka"],
        "neg": [],
    },
    "france": {
        "name": "France", "flag": "\U0001f1eb\U0001f1f7", "currency": "EUR",
        "kw": ["france", "french", "francais", "egalim",
               "leclerc", "carrefour", "danone"],
        "neg": [],
    },
    "spain": {
        "name": "Spain", "flag": "\U0001f1ea\U0001f1f8", "currency": "EUR",
        "kw": ["spain", "spanish", "espana", "madrid",
               "barcelona", "mercadona", "catalonia", "horeca spain", "valencia"],
        "neg": [],
    },
    "italy": {
        "name": "Italy", "flag": "\U0001f1ee\U0001f1f9", "currency": "EUR",
        "kw": ["italy", "italian", "italia", "milan",
               "rome", "aperitivo", "esselunga", "campari", "san pellegrino"],
        "neg": [],
    },
    "austria": {
        "name": "Austria", "flag": "\U0001f1e6\U0001f1f9", "currency": "EUR",
        "kw": ["austria", "austrian", "wien", "vienna",
               "spar austria", "hofer", "alnatura", "pfand", "red bull"],
        "neg": ["australia", "australian"],
    },
}

# ═══════════════════════════════════════════════════════════════
# FEEDS
# ═══════════════════════════════════════════════════════════════
def gnews(q, hl="en", gl="US"):
    return f"https://news.google.com/rss/search?q={quote(q)}&hl={hl}&gl={gl}&ceid={gl}:{hl}"

FEEDS = [
    # Tier 1: Industry RSS — VERIFIED WORKING FEEDS
    {"n": "FoodDive",              "url": "https://www.fooddive.com/feeds/news/",                "t": 1, "r": ["global"]},
    {"n": "Just-Food",             "url": "https://www.just-food.com/feed/",                     "t": 1, "r": ["global"]},
    {"n": "Food Safety News",      "url": "https://www.foodsafetynews.com/feed/",                "t": 1, "r": ["global"]},
    {"n": "Prepared Foods",        "url": "https://www.preparedfoods.com/rss/articles",          "t": 1, "r": ["usa"]},
    {"n": "Italian Food Net",      "url": "https://news.italianfood.net/feed/",                  "t": 1, "r": ["italy"]},
    {"n": "FD Business EU",        "url": "https://www.fdbusiness.com/feed/",                    "t": 1, "r": ["global"]},
    {"n": "FoodBev Media",         "url": "https://www.foodbev.com/feed/",                       "t": 1, "r": ["global"]},
    {"n": "Food Business News",    "url": "https://www.foodbusinessnews.net/rss/articles",       "t": 1, "r": ["usa"]},
    {"n": "Food Ind Executive",    "url": "https://foodindustryexecutive.com/feed/",             "t": 1, "r": ["usa"]},

    # Tier 2: Government/Regulatory
    {"n": "FDA Press",             "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml", "t": 2, "r": ["usa"]},

    # Tier 4: Google News — region-specific beverage queries
    # USA
    {"n": "GN:USA:soft-drink",     "url": gnews('("soft drink" OR "energy drink" OR "functional beverage" OR RTD) (launch OR market OR trend) USA 2026'), "t": 4, "r": ["usa"]},
    {"n": "GN:USA:juice",          "url": gnews('(juice OR smoothie OR "cold pressed" OR NFC) (market OR launch OR trend) USA 2026'),                    "t": 4, "r": ["usa"]},
    {"n": "GN:USA:regulation",     "url": gnews('(FDA OR regulation OR labeling OR recall) (beverage OR drink OR juice) USA'),                            "t": 4, "r": ["usa"]},
    {"n": "GN:USA:pricing",        "url": gnews('(beverage OR drink OR juice) (price OR cost OR inflation OR commodity) USA'),                            "t": 4, "r": ["usa"]},
    {"n": "GN:USA:noalc",          "url": gnews('("non-alcoholic" OR "alcohol-free" OR "zero alcohol") beverage USA'),                                   "t": 4, "r": ["usa"]},
    {"n": "GN:USA:functional",     "url": gnews('("functional beverage" OR probiotic OR adaptogen OR electrolyte) USA market'),                           "t": 4, "r": ["usa"]},
    {"n": "GN:USA:brands",         "url": gnews('(Coca-Cola OR PepsiCo OR Celsius OR "Monster Beverage" OR "Keurig Dr Pepper") launch OR news 2026'),    "t": 4, "r": ["usa"]},
    {"n": "GN:USA:retail",         "url": gnews('beverage retail Walmart OR Costco OR Target OR Kroger 2026'),                                           "t": 4, "r": ["usa"]},
    # GERMANY
    {"n": "GN:DE:bev",             "url": gnews('(Getraenk OR beverage OR "energy drink") (Markt OR market OR launch) Deutschland', "de", "DE"),            "t": 4, "r": ["germany"]},
    {"n": "GN:DE:juice",           "url": gnews('(Saft OR juice OR Fruchtsaft) (Markt OR market OR trend) Germany', "en", "DE"),                          "t": 4, "r": ["germany"]},
    {"n": "GN:DE:reg",             "url": gnews('(Nutri-Score OR PPWR OR "sugar tax" OR packaging) beverage Germany OR EU', "en", "DE"),                   "t": 4, "r": ["germany"]},
    {"n": "GN:DE:organic",         "url": gnews('(Bio OR organic) (Getraenk OR beverage OR Saft) Germany', "de", "DE"),                                    "t": 4, "r": ["germany"]},
    {"n": "GN:DE:brands",          "url": gnews('(Coca-Cola OR Lidl OR Aldi OR Rewe) Getraenk OR beverage Germany', "de", "DE"),                           "t": 4, "r": ["germany"]},
    # FRANCE
    {"n": "GN:FR:bev",             "url": gnews('(boisson OR beverage OR "energy drink") (marche OR market OR lancement) France', "fr", "FR"),             "t": 4, "r": ["france"]},
    {"n": "GN:FR:juice",           "url": gnews('(jus OR juice OR NFC OR "cold pressed") (marche OR market) France', "fr", "FR"),                         "t": 4, "r": ["france"]},
    {"n": "GN:FR:reg",             "url": gnews('(EGAlim OR Nutri-Score OR "taxe sucre" OR "sugar tax") boisson France', "fr", "FR"),                      "t": 4, "r": ["france"]},
    {"n": "GN:FR:brands",          "url": gnews('(Danone OR Perrier OR Orangina OR Leclerc OR Carrefour) boisson OR beverage France', "fr", "FR"),         "t": 4, "r": ["france"]},
    # SPAIN
    {"n": "GN:ES:bev",             "url": gnews('(bebida OR beverage OR "energy drink") (mercado OR market OR lanzamiento) Spain', "es", "ES"),            "t": 4, "r": ["spain"]},
    {"n": "GN:ES:juice",           "url": gnews('(zumo OR juice OR Valencia OR naranja) (mercado OR market) Spain', "es", "ES"),                           "t": 4, "r": ["spain"]},
    {"n": "GN:ES:reg",             "url": gnews('(impuesto OR tax OR regulation OR Catalonia) bebida OR beverage Spain', "es", "ES"),                      "t": 4, "r": ["spain"]},
    # ITALY
    {"n": "GN:IT:bev",             "url": gnews('(bevanda OR beverage OR "energy drink") (mercato OR market OR lancio) Italy', "it", "IT"),                "t": 4, "r": ["italy"]},
    {"n": "GN:IT:aperitivo",       "url": gnews('(aperitivo OR Campari OR Aperol OR "San Pellegrino") beverage OR drink Italy', "en", "IT"),               "t": 4, "r": ["italy"]},
    {"n": "GN:IT:reg",             "url": gnews('("sugar tax" OR packaging OR regulation) beverage Italy', "en", "IT"),                                    "t": 4, "r": ["italy"]},
    # AUSTRIA
    {"n": "GN:AT:bev",             "url": gnews('(Getraenk OR beverage OR "Red Bull") (Markt OR market OR launch) Austria', "de", "AT"),                    "t": 4, "r": ["austria"]},
    {"n": "GN:AT:organic",         "url": gnews('(Bio OR organic) (Getraenk OR beverage) Austria', "de", "AT"),                                            "t": 4, "r": ["austria"]},
    {"n": "GN:AT:pfand",           "url": gnews('(Pfand OR deposit OR packaging) beverage Austria', "de", "AT"),                                            "t": 4, "r": ["austria"]},
    # GLOBAL THEMES
    {"n": "GN:GL:functional",      "url": gnews('"functional beverage" OR "sports drink" OR "protein drink" market 2026'),                                 "t": 4, "r": ["global"]},
    {"n": "GN:GL:energy",          "url": gnews('"energy drink" market OR launch OR regulation 2026'),                                                     "t": 4, "r": ["global"]},
    {"n": "GN:GL:noalc",           "url": gnews('"non-alcoholic" OR "alcohol-free" beverage market 2026'),                                                 "t": 4, "r": ["global"]},
    {"n": "GN:GL:sugar-tax",       "url": gnews('"sugar tax" OR "sugar levy" beverage Europe OR EU OR USA 2026'),                                          "t": 4, "r": ["global"]},
    {"n": "GN:GL:packaging",       "url": gnews('(PPWR OR "EU packaging" OR "deposit return") beverage 2026'),                                             "t": 4, "r": ["global"]},
    {"n": "GN:GL:rtd",             "url": gnews('"ready to drink" OR RTD beverage market launch 2026'),                                                    "t": 4, "r": ["global"]},
    {"n": "GN:GL:juice-market",    "url": gnews('("fruit juice" OR "juice market" OR "NFC juice") trend OR price OR supply 2026'),                         "t": 4, "r": ["global"]},
    {"n": "GN:GL:brands",          "url": gnews('(Coca-Cola OR PepsiCo OR Nestle OR Danone OR "Red Bull") beverage launch 2026'),                          "t": 4, "r": ["global"]},
]

# ═══════════════════════════════════════════════════════════════
# RELEVANCE FILTER — TIGHT (exclude wine/spirits/cocktails)
# ═══════════════════════════════════════════════════════════════
MUST_MATCH = [
    "beverage", "drink", "juice", "soft drink", "energy drink", "smoothie",
    "water", "tea", "coffee", "rtd", "ready to drink", "functional",
    "carbonat", "sparkling", "soda", "seltzer", "tonic",
    "boisson", "bebida", "getraenk", "getrank", "succo", "saft", "jus", "zumo",
    "probiotic", "prebiotic", "adaptogen", "nootropic",
    "protein drink", "collagen drink", "electrolyte",
    "non-alcoholic", "alcohol-free", "zero alcohol", "low alcohol",
    "kombucha", "kefir", "fermented",
    "sugar tax", "nutri-score", "ppwr", "fda", "efsa",
    "packaging regulation", "deposit return", "pfand",
    "launch", "new product", "innovation", "reformulat",
]

HARD_EXCLUDE = [
    # Wine & spirits — the #1 junk source
    "wine", "winery", "vineyard", "vintage", "burgundy", "bordeaux",
    "champagne", "prosecco", "pinot", "merlot", "cabernet", "chardonnay",
    "sauvignon", "rioja", "barolo", "chianti", "chablis", "riesling",
    "whisky", "whiskey", "bourbon", "scotch", "cognac", "brandy",
    "gin ", " gin,", "vodka", "rum ", "tequila", "mezcal",
    "cocktail", "mixolog", "bartender", "speakeasy",
    "sommelier", "cellar", "decant", "cork", "barrel aged",
    "distiller", "distillery", "brewery", "brewpub", "craft beer",
    "auction house", "christie's", "sotheby",
    # Non-beverage
    "cryptocurrency", "bitcoin", "stock market", "nasdaq",
    "real estate", "mortgage", "auto loan",
    "recipe ", "cooking tip", "how to make",
    "raspberry pi", "python programming",
    "cosmetic", "skincare", "shampoo",
]

def is_relevant(text):
    t = text.lower()
    if any(ex in t for ex in HARD_EXCLUDE):
        return False
    return any(kw in t for kw in MUST_MATCH)

# ═══════════════════════════════════════════════════════════════
# CATEGORIES
# ═══════════════════════════════════════════════════════════════
CAT_RULES = [
    ("regulatory",   ["regulation", "regulat", "law", "directive", "tax", "ban", "label",
                      "nutri-score", "ppwr", "egalim", "fda", "efsa", "recall", "compliance",
                      "sugar tax", "deposit", "pfand"]),
    ("pricing",      ["price", "cost", "inflation", "commodity", "margin", "tariff",
                      "promotion", "discount", "cheaper"]),
    ("launch",       ["launch", "new product", "new range", "introduces", "unveil", "debut",
                      "release", "rolls out", "enters market", "expands into"]),
    ("competitor",   ["acquisition", "acquire", "merger", "m&a", "takeover", "partnership",
                      "joint venture", "restructur", "layoff", "appoints"]),
    ("supply_chain", ["supply chain", "shortage", "logistics", "packaging", "aluminum",
                      "pet resin", "glass bottle", "recycl", "pcr"]),
    ("retail",       ["retail", "supermarket", "shelf space", "private label", "store brand",
                      "e-commerce", "online retail", "listing", "delist"]),
    ("innovation",   ["innovation", "patent", "r&d", "research", "technology", "formula",
                      "ingredient", "probiotic", "adaptogen", "ferment"]),
    ("trend",        ["trend", "consumer", "demand", "growth", "forecast", "wellness",
                      "health", "clean label", "organic", "plant-based"]),
    ("macro",        ["gdp", "economy", "consumer spending", "market size", "industry value"]),
]

def categorize(text):
    t = text.lower()
    for cat, kws in CAT_RULES:
        if any(kw in t for kw in kws):
            return cat
    return "trend"

# ═══════════════════════════════════════════════════════════════
# PRODUCT TAGS
# ═══════════════════════════════════════════════════════════════
TAG_MAP = {
    "energy":       ["energy drink", "energy shot", "caffeine", "taurine", "guarana", "celsius", "monster", "red bull"],
    "hydration":    ["electrolyte", "sports drink", "hydration", "isotonic", "gatorade", "powerade"],
    "protein":      ["protein drink", "protein shake", "protein water", "collagen"],
    "functional":   ["functional", "adaptogen", "nootropic", "probiotic", "prebiotic", "gut health", "immunity", "wellness drink"],
    "sugar_free":   ["sugar free", "zero sugar", "no sugar", "diet ", "light ", "low calorie", "stevia", "monk fruit"],
    "juice":        ["juice", "nfc", "cold pressed", "smoothie", "nectar", "concentrate", "fruit drink"],
    "rtd":          ["rtd", "ready to drink", "ready-to-drink", "canned", "bottled"],
    "carbonated":   ["carbonated", "sparkling", "soda", "fizzy", "tonic", "seltzer", "club soda"],
    "alcohol_free": ["non-alcoholic", "alcohol-free", "zero alcohol", "0%", "alcohol free", "no-alcohol", "low-alcohol", "mocktail"],
    "organic":      ["organic", " bio ", "biologique"],
    "premium":      ["premium", "luxury", "artisan", "craft"],
    "dairy_alt":    ["oat milk", "almond milk", "soy milk", "plant-based milk", "dairy alternative"],
    "coffee_tea":   ["coffee", "tea ", "matcha", "chai", "cold brew", "iced tea", "iced coffee"],
    "water":        ["water", "mineral water", "sparkling water", "flavored water", "enhanced water"],
}

def tag_product(text):
    t = text.lower()
    return [tag for tag, kws in TAG_MAP.items() if any(kw in t for kw in kws)]

# ═══════════════════════════════════════════════════════════════
# ENTITY EXTRACTION
# ═══════════════════════════════════════════════════════════════
COMPANIES = [
    "Coca-Cola", "PepsiCo", "Nestle", "Danone", "Red Bull", "Monster",
    "Celsius", "Keurig Dr Pepper", "Britvic", "Campari", "Diageo",
    "Starbucks", "Innocent", "Oatly", "Chobani", "Huel", "Fever-Tree",
    "San Pellegrino", "Rauch", "Eckes-Granini", "Tropicana",
    "Suntory", "Asahi", "Athletic Brewing",
    "Aldi", "Lidl", "Tesco", "Carrefour", "Leclerc", "Mercadona",
    "Walmart", "Costco", "Target", "Kroger", "Rewe", "Edeka",
]

INGREDIENTS = [
    "protein", "collagen", "electrolyte", "caffeine", "taurine",
    "guarana", "ginseng", "ashwagandha", "lion's mane",
    "probiotic", "prebiotic", "fiber", "vitamin", "mineral",
    "stevia", "erythritol", "monk fruit",
]

def extract_entities(text):
    t = text.lower()
    return {
        "companies":   [c for c in COMPANIES if c.lower() in t],
        "ingredients": [i for i in INGREDIENTS if i in t],
        "packaging":   [p for p in ["PET", "can", "glass", "tetra pak", "carton", "pouch", "aluminum"] if p.lower() in t],
        "channels":    [c for c in ["retail", "e-commerce", "online", "on-premise", "horeca", "foodservice", "convenience", "supermarket"] if c.lower() in t],
    }

# ═══════════════════════════════════════════════════════════════
# WHY IT MATTERS + SALES ANGLES
# ═══════════════════════════════════════════════════════════════
WHY = {
    "regulatory":   "Regulatory change may require reformulation, relabeling, or pricing adjustments. Monitor compliance timelines.",
    "pricing":      "Pricing shift affects margins and competitive positioning. Review impact on contracts and shelf price.",
    "launch":       "New product launch signals competitive activity. Assess portfolio overlap and customer interest.",
    "competitor":   "Competitor move may reshape market dynamics. Evaluate impact on distribution and shelf space.",
    "supply_chain": "Supply chain change could affect input costs or packaging availability. Check procurement exposure.",
    "retail":       "Retail landscape shift may create listing opportunities or threaten placements. Brief key accounts.",
    "innovation":   "Innovation trend signals emerging demand. Evaluate R&D alignment and first-mover potential.",
    "trend":        "Consumer trend shift. Consider portfolio alignment and marketing messaging.",
    "macro":        "Macro factor may influence spending patterns and distributor purchasing.",
}

ANGLES = {
    "regulatory":   ["Check compliance timeline", "Brief customers on regulatory impact"],
    "pricing":      ["Review pricing vs competitors", "Prepare margin impact analysis"],
    "launch":       ["Map against portfolio for overlap", "Brief sales team on positioning"],
    "competitor":   ["Update competitive intel file", "Prepare defensive talking points"],
    "supply_chain": ["Check procurement exposure", "Prepare customer communication"],
    "retail":       ["Brief key account managers", "Review listing strategy"],
    "innovation":   ["Share with R&D team", "Assess consumer relevance"],
    "trend":        ["Include in next customer presentation", "Align marketing messaging"],
    "macro":        ["Factor into demand planning", "Adjust forecasts if needed"],
}

# ═══════════════════════════════════════════════════════════════
# RSS FETCHING
# ═══════════════════════════════════════════════════════════════
def parse_date(s):
    if not s:
        return NOW
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%d %b %Y %H:%M:%S %z"]:
        try:
            dt = datetime.strptime(s.strip(), fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return NOW

def clean_html(raw):
    if not raw:
        return ""
    if BS4:
        return BeautifulSoup(raw, "html.parser").get_text(separator=" ", strip=True)
    return re.sub(r"<[^>]+>", " ", raw).strip()

def norm_url(url):
    try:
        p = urlparse(url)
        return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))
    except Exception:
        return url

def fetch_feed(feed):
    headers = {"User-Agent": "BevIntel/2.1", "Accept": "application/rss+xml, application/xml, text/xml"}
    try:
        r = requests.get(feed["url"], headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:
        return [], str(e)[:100]

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)
    out = []
    for item in items:
        def g(tag):
            el = item.find(tag) or item.find(f"atom:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""
        link = g("link")
        if not link:
            le = item.find("atom:link", ns)
            link = le.get("href", "") if le is not None else ""
        desc = clean_html(g("description") or g("content") or g("summary"))
        if len(desc) > 400:
            desc = desc[:397].rsplit(" ", 1)[0] + "..."
        out.append({
            "title": g("title"), "url": link, "summary": desc,
            "published": parse_date(g("pubDate") or g("published") or g("updated")),
            "source": feed["n"], "tier": feed["t"], "feed_regions": feed["r"],
        })
    return out, None

# ═══════════════════════════════════════════════════════════════
# REGION ASSIGNMENT — THE KEY FIX
# ═══════════════════════════════════════════════════════════════
def assign_regions(text, feed_regions):
    """
    KEY CHANGE: global articles are tagged '_global' and later
    shared to ALL regions with lower confidence.
    """
    t = text.lower()
    matched = []
    for rid, rdef in REGIONS.items():
        if any(neg in t for neg in rdef["neg"]):
            continue
        if any(kw in t for kw in rdef["kw"]):
            matched.append(rid)

    if not matched and feed_regions != ["global"]:
        matched = [r for r in feed_regions if r in REGIONS]

    return matched if matched else ["_global"]

# ═══════════════════════════════════════════════════════════════
# DEDUP
# ═══════════════════════════════════════════════════════════════
def dedup(articles):
    seen_urls, seen_titles, out = {}, [], []
    for a in articles:
        u = norm_url(a["url"])
        if u in seen_urls:
            continue
        seen_urls[u] = True
        tl = a["title"].lower().strip()
        if any(SequenceMatcher(None, tl, prev).ratio() > 0.8 for prev in seen_titles):
            continue
        seen_titles.append(tl)
        out.append(a)
    return out

# ═══════════════════════════════════════════════════════════════
# SCORING
# ═══════════════════════════════════════════════════════════════
CAT_PRI = {"regulatory":1.0, "pricing":0.9, "launch":0.85, "competitor":0.8,
           "supply_chain":0.7, "retail":0.65, "innovation":0.6, "trend":0.5, "macro":0.4}
SRC_REL = {1: 1.0, 2: 0.95, 3: 0.7, 4: 0.5}

def score_art(a, strength):
    age = max(0, (NOW - a["published"]).total_seconds() / 86400)
    rec = math.exp(-0.05 * age)
    cp = CAT_PRI.get(a.get("category", "trend"), 0.5)
    sr = SRC_REL.get(a.get("tier", 4), 0.5)
    rs = {"high": 1.0, "medium": 0.6, "low": 0.3}.get(strength, 0.3)
    return round(0.50 * rec + 0.25 * rs + 0.15 * cp + 0.10 * sr, 4)

# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════
def run():
    print("=" * 60)
    print(f"  Sales Intelligence Pipeline v2 -- {NOW.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    errors = []
    stats = {"ok": 0, "fail": 0}
    raw = []

    # ── 1. FETCH ──
    print(f"\n  [1/6] FETCHING {len(FEEDS)} feeds...")
    for f in FEEDS:
        items, err = fetch_feed(f)
        if err:
            errors.append({"region": ",".join(f["r"]), "source": f["n"], "error": err, "time": NOW.isoformat()})
            stats["fail"] += 1
            print(f"    x {f['n']}: {err[:50]}")
        else:
            stats["ok"] += 1
            if items:
                print(f"    + {f['n']}: {len(items)} items")
        raw.extend(items)
    print(f"  Raw total: {len(raw)}")

    # ── 2. FILTER ──
    print("\n  [2/6] FILTERING...")
    relevant = [a for a in raw if is_relevant(f"{a['title']} {a['summary']}")]
    print(f"  Relevant: {len(relevant)} / {len(raw)} (dropped {len(raw)-len(relevant)})")

    # ── 3. ENRICH ──
    print("\n  [3/6] ENRICHING...")
    for a in relevant:
        txt = f"{a['title']} {a['summary']}"
        a["category"] = categorize(txt)
        a["entities"] = extract_entities(txt)
        a["product_tags"] = tag_product(txt)
        a["regions"] = assign_regions(txt, a["feed_regions"])

    # ── 4. BUCKET ──
    print("\n  [4/6] BUCKETING...")
    region_arts = {r: [] for r in REGIONS}
    global_arts = []

    for a in relevant:
        if "_global" in a["regions"]:
            global_arts.append(a)
        else:
            for r in a["regions"]:
                if r in region_arts:
                    region_arts[r].append(a)

    # KEY FIX: global articles go to ALL regions
    for r in REGIONS:
        region_arts[r].extend(global_arts)

    print(f"  Global pool: {len(global_arts)} shared to all regions")

    region_news = {}
    health = {}

    for rid in REGIONS:
        arts = dedup(region_arts[rid])
        in_win = [a for a in arts if a["published"] >= CUTOFF]
        fallback = False
        if len(in_win) < MIN_ITEMS:
            in_win = [a for a in arts if a["published"] >= FALLBACK_CUTOFF]
            fallback = True

        for a in in_win:
            txt = f"{a['title']} {a['summary']}".lower()
            rdef = REGIONS[rid]
            hits = sum(1 for kw in rdef["kw"] if kw in txt)
            strength = "high" if hits >= 2 else "medium" if hits >= 1 else "low"
            a["_str"] = strength
            a["confidence"] = "high" if a["published"] >= CUTOFF and strength != "low" else "low" if fallback else "medium"
            a["score"] = score_art(a, strength)
            a["why_it_matters"] = WHY.get(a["category"], "")
            a["sales_angles"] = ANGLES.get(a["category"], [])

        in_win.sort(key=lambda x: (0 if x["_str"] == "high" else 1 if x["_str"] == "medium" else 2, -x["score"]))

        formatted = []
        for a in in_win[:50]:
            formatted.append({
                "title": a["title"], "url": a["url"], "source": a["source"],
                "published": a["published"].isoformat(),
                "country_region": rid, "category": a["category"],
                "entities": a["entities"], "product_tags": a["product_tags"],
                "summary": a["summary"],
                "why_it_matters": a["why_it_matters"],
                "sales_angles": a["sales_angles"],
                "confidence": a["confidence"], "score": a["score"],
            })

        region_news[rid] = formatted

        notes = []
        if fallback:
            notes.append(f"Extended to {FALLBACK_DAYS}d window")
        if len(formatted) < MIN_ITEMS:
            notes.append(f"Below {MIN_ITEMS} threshold: {len(formatted)}")

        last = max((a["published"] for a in in_win), default=None)
        hi_count = sum(1 for a in in_win if a.get("_str") == "high")
        health[rid] = {
            "status": "error" if not formatted else "warning" if len(formatted) < MIN_ITEMS or fallback else "ok",
            "items": len(formatted),
            "region_specific": hi_count,
            "sources_ok": stats["ok"], "sources_failed": stats["fail"],
            "last_item_date": last.isoformat() if last else None,
            "notes": notes,
        }
        print(f"    {rid}: {len(formatted)} items [{health[rid]['status']}] (region-specific: {hi_count})")

    # ── 5. BRIEFINGS ──
    print("\n  [5/6] BRIEFINGS...")
    briefings = {}
    for rid, arts in region_news.items():
        rn = REGIONS[rid]["name"]
        ex = [{"headline": a["title"], "detail": a["summary"][:200], "evidence_urls": [a["url"]], "confidence": a["confidence"]} for a in arts[:5]]
        launches = [a for a in arts if a["category"] == "launch"][:5]
        kl = [{"title": a["title"], "company": ", ".join(a["entities"]["companies"][:2]) or "-", "product": ", ".join(a["product_tags"][:3]) or "beverage", "angle": a["sales_angles"][0] if a["sales_angles"] else "", "evidence_url": a["url"], "date": a["published"]} for a in launches]
        cm = [{"title": a["title"], "company": ", ".join(a["entities"]["companies"][:2]) or "-", "move_type": "competitor", "impact": a["why_it_matters"], "evidence_url": a["url"], "date": a["published"]} for a in [a for a in arts if a["category"] == "competitor"][:5]]
        rw = [{"title": a["title"], "topic": ", ".join(a["product_tags"][:2]) or "regulation", "impact_on_sales": a["why_it_matters"], "evidence_url": a["url"], "date": a["published"]} for a in [a for a in arts if a["category"] == "regulatory"][:5]]
        pp = [{"title": a["title"], "what_changed": a["summary"][:150], "sales_risk_or_opportunity": a["why_it_matters"], "evidence_url": a["url"], "date": a["published"]} for a in [a for a in arts if a["category"] == "pricing"][:5]]

        tc = Counter(tag for a in arts for tag in a["product_tags"])
        sigs = [{"signal": f"{t.replace('_', ' ').title()} trending in {rn}", "explanation": f"{c} articles mention {t.replace('_', ' ')}", "support_count": c, "top_keywords": [t], "confidence": "high" if c >= 5 else "medium" if c >= 2 else "low"} for t, c in tc.most_common(5)]
        if not sigs:
            cc = Counter(a["category"] for a in arts)
            sigs = [{"signal": f"{cat.replace('_', ' ').title()} activity in {rn}", "explanation": f"{cnt} {cat} articles tracked", "support_count": cnt, "top_keywords": [cat], "confidence": "medium"} for cat, cnt in cc.most_common(3)]

        tp = []
        if launches:
            tp.append({"customer_type": "retail", "pitch": f"{len(launches)} new launches in {rn} - discuss shelf space for new formats", "supporting_evidence_urls": [a["url"] for a in launches[:3]]})
        if rw:
            tp.append({"customer_type": "key_account", "pitch": f"Regulatory changes in {rn} - position as proactive compliance partner", "supporting_evidence_urls": [a["evidence_url"] for a in rw[:3]]})
        if tc.get("functional", 0) >= 1:
            tp.append({"customer_type": "distributor", "pitch": f"Functional beverage demand rising in {rn} - expand functional SKU range", "supporting_evidence_urls": [a["url"] for a in arts if "functional" in a.get("product_tags", [])][:3]})
        if tc.get("energy", 0) >= 1:
            tp.append({"customer_type": "retail", "pitch": f"Energy drink segment active in {rn} - review energy portfolio positioning", "supporting_evidence_urls": [a["url"] for a in arts if "energy" in a.get("product_tags", [])][:3]})
        if not tp and arts:
            tp.append({"customer_type": "key_account", "pitch": f"{len(arts)} market developments tracked in {rn} - use in next review meeting", "supporting_evidence_urls": [a["url"] for a in arts[:3]]})

        act = []
        if launches:
            act.append({"owner": "sales", "action": f"Review {len(launches)} new launches for competitive overlap", "why_now": "Competitive response needed", "evidence_urls": [a["url"] for a in launches[:3]]})
        if rw:
            act.append({"owner": "sales", "action": "Brief quality team on regulatory developments", "why_now": "Compliance deadlines approaching", "evidence_urls": [a["evidence_url"] for a in rw[:3]]})
        if not act and arts:
            act.append({"owner": "sales", "action": f"Review {len(arts)} intelligence items for {rn}", "why_now": "Keep competitive awareness current", "evidence_urls": [a["url"] for a in arts[:3]]})

        briefings[rid] = {"executive_summary": ex, "key_launches": kl, "competitor_moves": cm, "regulatory_watch": rw, "pricing_promotions": pp, "signals": sigs, "talking_points": tp, "recommended_actions": act}

    # ── 6. MARKET STATS ──
    print("\n  [6/6] MARKET STATS...")
    MD = {
        "usa":     {"sz": 265, "u": "USD_B", "gr": 3.0, "src": "Statista", "surl": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/united-states"},
        "germany": {"sz": 29,  "u": "EUR_B", "gr": 2.0, "src": "Statista", "surl": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/germany"},
        "france":  {"sz": 22,  "u": "EUR_B", "gr": 2.0, "src": "Statista", "surl": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/france"},
        "spain":   {"sz": 12,  "u": "EUR_B", "gr": 3.0, "src": "Statista", "surl": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/spain"},
        "italy":   {"sz": 18,  "u": "EUR_B", "gr": 3.0, "src": "Statista", "surl": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/italy"},
        "austria": {"sz": 5,   "u": "EUR_B", "gr": 2.0, "src": "Statista", "surl": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/austria"},
    }
    mstats = {"generated_at": NOW.isoformat(), "regions": {}}
    for rid, m in MD.items():
        mstats["regions"][rid] = {
            "market_context": {
                "currency": REGIONS[rid]["currency"],
                "market_size": {"value": m["sz"], "unit": m["u"], "year": 2024, "method": "manual_estimate", "sources": [{"name": m["src"], "url": m["surl"]}], "last_verified": "2025-01-15", "confidence": "medium", "notes": "Order-of-magnitude estimate."},
                "growth": {"value": m["gr"], "unit": "pct", "period": "YoY", "method": "manual_estimate", "sources": [{"name": m["src"], "url": m["surl"]}], "last_verified": "2025-01-15", "confidence": "medium", "notes": "Approximate growth."},
            },
            "sales_relevance_notes": [f"{len(region_news.get(rid, []))} items tracked."],
        }

    # ── SAVE ──
    print("\n  SAVING...")

    def save(name, data):
        with open(OUT_DIR / name, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  + {name}")

    save("sales_news.json", {
        "generated_at": NOW.isoformat(), "window_days": WINDOW_DAYS,
        "regions": region_news,
        "meta": {
            "sources_used": {r: list({a["source"] for a in arts}) for r, arts in region_news.items()},
            "errors": errors,
            "counts": {r: len(a) for r, a in region_news.items()},
        },
    })

    save("sales_briefings.json", {"generated_at": NOW.isoformat(), "window_days": WINDOW_DAYS, "regions": briefings})
    save("market_stats.json", mstats)
    save("data_health.json", {
        "generated_at": NOW.isoformat(),
        "regions": health,
        "global": {"total_items": sum(len(a) for a in region_news.values()), "total_sources_ok": stats["ok"], "total_sources_failed": stats["fail"]},
    })

    # briefing.json for morning briefing panel
    all_arts = [a for arts in region_news.values() for a in arts]
    all_deduped = dedup(all_arts)
    total = len(all_deduped)
    active = sum(1 for a in region_news.values() if a)

    top5 = sorted(all_deduped, key=lambda x: x.get("score", 0), reverse=True)[:5]
    bsent = []
    bsent.append(f"Tracking {total} beverage intelligence items across {active} regions.")
    if top5:
        top_cats = Counter(a["category"] for a in all_deduped).most_common(3)
        cat_ph = {"launch": "product launches", "regulatory": "regulatory developments", "pricing": "pricing shifts", "trend": "consumer trends", "innovation": "innovation activity", "competitor": "competitor moves", "supply_chain": "supply chain updates", "retail": "retail developments"}
        themes = [cat_ph.get(c, c) for c, _ in top_cats]
        bsent.append(f"Top themes: {', '.join(themes)}.")
        bsent.append(f"Notable: {top5[0]['title'][:100]}.")

    def mk_sig(rid):
        arts = region_news.get(rid, [])
        if not arts:
            return f"Pipeline expanding sources for {REGIONS[rid]['name']}."
        tags = Counter(tag for a in arts for tag in a.get("product_tags", []))
        cats = Counter(a["category"] for a in arts)
        n = len(arts)
        if tags:
            top = tags.most_common(1)[0][0].replace("_", " ").title()
            return f"{top} leading. {n} items tracked."
        elif cats:
            top = cats.most_common(1)[0][0].replace("_", " ").title()
            return f"{top} activity dominant. {n} items."
        return f"{n} items tracked."

    save("briefing.json", {
        "generated_at": NOW.isoformat(),
        "generated_date": NOW.strftime("%A, %d %B %Y"),
        "briefing": " ".join(bsent),
        "signals": {r: mk_sig(r) for r in REGIONS},
        "meta": {
            "total_articles_analyzed": total,
            "method": "rss-analysis",
            "top_topics": dict(Counter(tag for a in all_deduped for tag in a.get("product_tags", [])).most_common(10)),
        },
    })

    # Summary
    print(f"\n  {'=' * 50}")
    print(f"  PIPELINE v2 COMPLETE")
    ti = sum(len(a) for a in region_news.values())
    print(f"  Total region items: {ti} | Unique: {total}")
    print(f"  Sources: {stats['ok']} OK / {stats['fail']} failed")
    for rid in REGIONS:
        h = health[rid]
        print(f"    {rid}: {h['items']} items [{h['status']}] (region-specific: {h.get('region_specific', 0)})")
    print(f"  {'=' * 50}\n")


if __name__ == "__main__":
    run()
