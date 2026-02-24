#!/usr/bin/env python3
"""
pipeline/sales_pipeline.py — Beverage Sales Intelligence Pipeline

Produces:
  sales_news.json       — region news (last 28 days)
  market_stats.json     — transparent market context
  sales_briefings.json  — sales-ready briefings per region
  data_health.json      — pipeline health + transparency

Free sources only. No paid APIs. No AI calls.
Runs daily via GitHub Actions.
"""

import json
import hashlib
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from difflib import SequenceMatcher
from urllib.parse import urlparse, urlunparse

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
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

OUT_DIR          = Path(__file__).parent.parent  # repo root
WINDOW_DAYS      = 28
FALLBACK_DAYS    = 42
MIN_ITEMS        = 10
REQUEST_TIMEOUT  = 12
NOW              = datetime.now(timezone.utc)
CUTOFF           = NOW - timedelta(days=WINDOW_DAYS)
FALLBACK_CUTOFF  = NOW - timedelta(days=FALLBACK_DAYS)

# ═══════════════════════════════════════════════════════════════
# REGION DEFINITIONS
# ═══════════════════════════════════════════════════════════════

REGIONS = {
    "usa": {
        "name": "United States", "flag": "🇺🇸", "currency": "USD",
        "keywords": ["usa", "united states", "u.s.", "american", "fda",
                     "north america", "us market"],
        "negative": ["australian", "austria"],
    },
    "germany": {
        "name": "Germany", "flag": "🇩🇪", "currency": "EUR",
        "keywords": ["germany", "german", "deutschland", "dach",
                     "bundesrat", "lebensmittel"],
        "negative": [],
    },
    "france": {
        "name": "France", "flag": "🇫🇷", "currency": "EUR",
        "keywords": ["france", "french", "francais", "egalim",
                     "leclerc", "carrefour"],
        "negative": [],
    },
    "spain": {
        "name": "Spain", "flag": "🇪🇸", "currency": "EUR",
        "keywords": ["spain", "spanish", "espana", "madrid",
                     "barcelona", "mercadona", "catalonia"],
        "negative": [],
    },
    "italy": {
        "name": "Italy", "flag": "🇮🇹", "currency": "EUR",
        "keywords": ["italy", "italian", "italia", "milan",
                     "rome", "aperitivo", "esselunga"],
        "negative": [],
    },
    "austria": {
        "name": "Austria", "flag": "🇦🇹", "currency": "EUR",
        "keywords": ["austria", "austrian", "wien", "vienna",
                     "spar austria", "hofer", "alnatura", "pfand"],
        "negative": ["australia"],
    },
}

# ═══════════════════════════════════════════════════════════════
# SOURCE FEEDS — TIER STRUCTURE
# ═══════════════════════════════════════════════════════════════

# Tier 1: Industry RSS
TIER1_FEEDS = [
    {"name": "FoodNavigator",              "url": "https://www.foodnavigator.com/rss/editorial.rss",        "tier": 1, "regions": ["global"]},
    {"name": "BeverageDaily",              "url": "https://www.beveragedaily.com/rss/editorial.rss",        "tier": 1, "regions": ["global"]},
    {"name": "Just-Drinks",                "url": "https://www.just-drinks.com/feed/",                      "tier": 1, "regions": ["global"]},
    {"name": "FoodNavigator-USA",          "url": "https://www.foodnavigator-usa.com/rss/editorial.rss",    "tier": 1, "regions": ["usa"]},
    {"name": "Beverage Industry Magazine",  "url": "https://www.bevindustry.com/rss/all",                    "tier": 1, "regions": ["usa"]},
    {"name": "Drinks Business",            "url": "https://www.thedrinksbusiness.com/feed/",                "tier": 1, "regions": ["global"]},
    {"name": "Food & Drink Technology",    "url": "https://www.foodanddrink-technology.com/feed/",          "tier": 1, "regions": ["global"]},
]

# Tier 2: Regulatory/Government
TIER2_FEEDS = [
    {"name": "FDA Recalls",  "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/food-safety-recalls/rss.xml", "tier": 2, "regions": ["usa"]},
    {"name": "EFSA News",    "url": "https://www.efsa.europa.eu/en/rss", "tier": 2, "regions": ["germany", "france", "spain", "italy", "austria"]},
]

# Tier 3: Company newsrooms
TIER3_FEEDS = [
    {"name": "Coca-Cola Press",  "url": "https://www.coca-colacompany.com/media-center/rss",                     "tier": 3, "regions": ["global"]},
    {"name": "Nestlé News",      "url": "https://www.nestle.com/media/mediaeventsnews/rss",                       "tier": 3, "regions": ["global"]},
]

# Tier 4: Google News RSS (region-specific query packs)
def gnews_url(q, hl="en", gl="US"):
    from urllib.parse import quote
    return f"https://news.google.com/rss/search?q={quote(q)}&hl={hl}&gl={gl}&ceid={gl}:{hl}"

TIER4_QUERIES = {
    "usa": [
        '(beverage OR drink OR juice OR "energy drink" OR RTD) (launch OR introduces OR new) USA',
        '(beverage OR drink OR juice) (regulation OR FDA OR recall OR labeling) USA',
        '(beverage OR drink) (price OR promotion OR retailer OR "private label") USA',
        '(packaging OR aluminum OR PET OR shortage) beverage USA',
        '(Coca-Cola OR PepsiCo OR Celsius OR "Monster Beverage") launch OR news',
        '"functional beverage" OR "sports drink" OR "protein drink" USA market',
        '"energy drink" OR "ready to drink" USA trend 2026',
        '"sugar free" OR "zero sugar" beverage USA',
        '(kombucha OR probiotic OR prebiotic) drink USA',
        '"non-alcoholic" OR "alcohol free" beverage USA',
        'beverage "supply chain" OR logistics OR cost USA',
        '"private label" OR "store brand" beverage USA retail',
    ],
    "germany": [
        '(Getraenk OR beverage OR drink OR Saft) (launch OR Markt OR market) Deutschland OR Germany',
        '(beverage OR drink OR juice) (regulation OR Nutri-Score OR PPWR OR packaging) Germany OR EU',
        '(beverage OR drink) (price OR Preis OR retail OR discount) Germany',
        '(Red Bull OR Coca-Cola OR Pepsi OR Lidl OR Aldi) beverage Germany',
        '"energy drink" OR "functional beverage" Germany trend',
        '(organic OR Bio) beverage OR drink OR Saft Germany',
        '"sugar tax" OR "Zuckersteuer" beverage Germany OR EU',
        'beverage packaging recycling deposit Germany Pfand',
    ],
    "france": [
        '(boisson OR beverage OR jus OR juice) (lancement OR launch OR marche OR market) France',
        '(beverage OR drink) (regulation OR EGAlim OR Nutri-Score OR "sugar tax") France',
        '(Danone OR Perrier OR Orangina OR Leclerc OR Carrefour) beverage France',
        '"functional beverage" OR "boisson fonctionnelle" France',
        '(organic OR bio) juice OR beverage France',
        '"premium juice" OR "NFC juice" OR "cold pressed" France',
    ],
    "spain": [
        '(bebida OR beverage OR zumo OR juice) (lanzamiento OR launch OR mercado OR market) Spain',
        '(beverage OR drink) (regulation OR tax OR Catalonia OR impuesto) Spain',
        '(Coca-Cola OR Mercadona OR Horeca) beverage Spain',
        '"energy drink" OR "functional beverage" Spain trend',
        '(Valencia OR orange) juice market Spain',
    ],
    "italy": [
        '(bevanda OR beverage OR succo OR juice) (lancio OR launch OR mercato OR market) Italy',
        '(beverage OR drink) (regulation OR "sugar tax" OR packaging) Italy',
        '(Campari OR Aperol OR San Pellegrino OR aperitivo) beverage Italy',
        '"functional beverage" OR "energy drink" Italy trend',
        '(organic OR bio) juice OR beverage Italy',
    ],
    "austria": [
        '(Getraenk OR beverage OR Saft OR drink) (launch OR Markt OR market) Austria',
        '(Red Bull OR Rauch OR Voelkel) launch OR innovation Austria',
        '(beverage OR drink) (Pfand OR deposit OR packaging OR organic) Austria',
        '"energy drink" OR "functional beverage" Austria DACH',
    ],
}

TIER4_FEEDS = []
for region, queries in TIER4_QUERIES.items():
    hl = {"usa": "en", "germany": "de", "france": "fr", "spain": "es", "italy": "it", "austria": "de"}[region]
    gl = {"usa": "US", "germany": "DE", "france": "FR", "spain": "ES", "italy": "IT", "austria": "AT"}[region]
    for i, q in enumerate(queries):
        TIER4_FEEDS.append({
            "name": f"GNews:{region}:{i}",
            "url": gnews_url(q, hl, gl),
            "tier": 4,
            "regions": [region],
        })

ALL_FEEDS = TIER1_FEEDS + TIER2_FEEDS + TIER3_FEEDS + TIER4_FEEDS

# ═══════════════════════════════════════════════════════════════
# BEVERAGE RELEVANCE + EXCLUSIONS
# ═══════════════════════════════════════════════════════════════

BEVERAGE_KEYWORDS = [
    "beverage", "drink", "juice", "soft drink", "energy drink", "smoothie",
    "water", "tea", "coffee", "rtd", "ready to drink", "functional",
    "carbonat", "sparkling", "soda", "seltzer", "tonic",
    "boisson", "bebida", "getraenk", "succo", "saft", "jus",
    "launch", "new product", "innovation",
    "sugar tax", "nutri-score", "packaging", "regulation", "labelling",
    "price", "pricing", "commodity", "concentrate",
    "probiotic", "prebiotic", "adaptogen", "nootropic",
    "protein drink", "collagen", "electrolyte",
    "non-alcoholic", "alcohol-free", "zero alcohol", "low alcohol",
    "kombucha", "kefir", "fermented",
]

EXCLUSIONS = [
    "cryptocurrency", "bitcoin", "stock market", "nasdaq", "nyse",
    "real estate", "mortgage", "auto loan", "car insurance",
    "celebrity gossip", "sports score", "football result",
    "recipe ", "cooking tip", "how to make", "diy ",
    "raspberry pi", "python programming", "javascript",
]

def is_beverage_relevant(text):
    t = text.lower()
    if any(ex in t for ex in EXCLUSIONS):
        return False
    return any(kw in t for kw in BEVERAGE_KEYWORDS)

# ═══════════════════════════════════════════════════════════════
# CATEGORY DETECTION (deterministic, priority-ordered)
# ═══════════════════════════════════════════════════════════════

CATEGORY_RULES = [
    ("regulatory",    ["regulation", "regulat", "law", "directive", "tax", "ban", "label",
                       "nutri-score", "ppwr", "egalim", "fda", "efsa", "recall", "compliance"]),
    ("pricing",       ["price", "cost", "inflation", "commodity", "margin", "tariff",
                       "import cost", "promotion", "discount", "cheaper", "expensive"]),
    ("launch",        ["launch", "new product", "new range", "introduces", "unveil", "debut",
                       "release", "rolls out", "enters market", "expands"]),
    ("competitor",    ["acquisition", "acquire", "merger", "m&a", "takeover", "partnership",
                       "joint venture", "restructur", "layoff", "appoint"]),
    ("supply_chain",  ["supply chain", "shortage", "logistics", "packaging", "aluminum",
                       "pet resin", "glass", "can ", "bottle", "recycl", "pcr"]),
    ("retail",        ["retail", "supermarket", "hypermarket", "convenience", "shelf space",
                       "private label", "store brand", "e-commerce", "online retail"]),
    ("innovation",    ["innovation", "patent", "r&d", "research", "technology", "formula",
                       "ingredient", "functional", "probiotic", "adaptogen"]),
    ("trend",         ["trend", "consumer", "demand", "growth", "market", "insight",
                       "report", "forecast", "wellness", "health"]),
    ("macro",         ["gdp", "economy", "inflation", "interest rate", "consumer spending",
                       "market size", "industry"]),
]

def detect_category(text):
    t = text.lower()
    for cat, keywords in CATEGORY_RULES:
        if any(kw in t for kw in keywords):
            return cat
    return "trend"

# ═══════════════════════════════════════════════════════════════
# ENTITY EXTRACTION (dictionaries + regex)
# ═══════════════════════════════════════════════════════════════

ENTITY_COMPANIES = [
    "Coca-Cola", "PepsiCo", "Nestlé", "Danone", "Red Bull", "Monster",
    "Celsius", "Keurig Dr Pepper", "Britvic", "Campari", "Diageo",
    "AB InBev", "Heineken", "Starbucks", "Nespresso", "Innocent",
    "Oatly", "Chobani", "Huel", "Athletic Brewing", "Fever-Tree",
    "San Pellegrino", "Rauch", "Voelkel", "Eckes-Granini", "Tropicana",
    "Suntory", "Asahi", "Kirin", "Lactalis", "Fonterra",
    "Aldi", "Lidl", "Tesco", "Carrefour", "Leclerc", "Mercadona",
    "Walmart", "Costco", "Target", "Kroger", "Rewe", "Edeka",
]

ENTITY_INGREDIENTS = [
    "protein", "collagen", "electrolyte", "caffeine", "taurine",
    "guarana", "ginseng", "ashwagandha", "lion's mane", "adaptogens",
    "probiotic", "prebiotic", "fiber", "vitamin", "mineral",
    "stevia", "erythritol", "monk fruit", "aspartame", "sucralose",
    "cbd", "thc", "hemp",
]

ENTITY_PACKAGING = ["PET", "can", "glass", "tetra pak", "carton", "pouch", "aluminum"]
ENTITY_CHANNELS  = ["retail", "e-commerce", "online", "on-premise", "horeca", "foodservice", "convenience", "supermarket"]

PRODUCT_TAG_MAP = {
    "energy":       ["energy drink", "energy shot", "caffeine", "taurine", "guarana"],
    "hydration":    ["electrolyte", "sports drink", "hydration", "isotonic"],
    "protein":      ["protein drink", "protein shake", "protein water", "collagen"],
    "functional":   ["functional", "adaptogen", "nootropic", "probiotic", "prebiotic", "gut health", "immunity"],
    "sugar_free":   ["sugar free", "zero sugar", "no sugar", "diet", "light"],
    "juice":        ["juice", "nfc", "cold pressed", "smoothie", "nectar"],
    "rtd":          ["rtd", "ready to drink", "ready-to-drink", "canned cocktail"],
    "carbonated":   ["carbonated", "sparkling", "soda", "fizzy", "tonic", "seltzer"],
    "alcohol_free": ["non-alcoholic", "alcohol-free", "zero alcohol", "0%", "alcohol free", "no-alcohol", "low-alcohol"],
    "organic":      ["organic", "bio ", "biologique"],
    "premium":      ["premium", "luxury", "artisan", "craft"],
    "dairy_alt":    ["oat milk", "almond milk", "soy milk", "plant-based", "dairy alternative"],
}

def extract_entities(text):
    t = text.lower()
    return {
        "companies":   [c for c in ENTITY_COMPANIES if c.lower() in t],
        "brands":      [],  # brands overlap with companies; extend as needed
        "ingredients": [i for i in ENTITY_INGREDIENTS if i.lower() in t],
        "packaging":   [p for p in ENTITY_PACKAGING if p.lower() in t],
        "channels":    [c for c in ENTITY_CHANNELS if c.lower() in t],
    }

def extract_product_tags(text):
    t = text.lower()
    return [tag for tag, keywords in PRODUCT_TAG_MAP.items() if any(kw in t for kw in keywords)]

# ═══════════════════════════════════════════════════════════════
# WHY IT MATTERS — rule-based templates
# ═══════════════════════════════════════════════════════════════

WHY_TEMPLATES = {
    "regulatory":   "Regulatory change may require product reformulation, relabeling, or pricing adjustments. Monitor compliance timelines.",
    "pricing":      "Pricing shift affects margins and competitive positioning. Review impact on current contracts and shelf price strategy.",
    "launch":       "New product launch signals competitive activity. Assess overlap with our portfolio and potential customer interest.",
    "competitor":   "Competitor move may reshape market dynamics. Evaluate impact on distribution, shelf space, and customer relationships.",
    "supply_chain": "Supply chain development could affect input costs, lead times, or packaging availability. Review procurement exposure.",
    "retail":       "Retail landscape shift may create new listing opportunities or threaten existing placements. Brief key account teams.",
    "innovation":   "Innovation trend signals emerging consumer demand. Evaluate R&D alignment and potential first-mover advantage.",
    "trend":        "Market trend indicates shifting consumer preferences. Consider portfolio alignment and marketing messaging.",
    "macro":        "Macroeconomic factor may influence consumer spending patterns and distributor purchasing behavior.",
}

SALES_ANGLE_TEMPLATES = {
    "regulatory":   ["Check compliance timeline with quality team", "Brief customers on regulatory impact"],
    "pricing":      ["Review pricing vs competitors", "Prepare margin impact analysis"],
    "launch":       ["Map against our portfolio for overlap", "Brief sales team on competitive positioning"],
    "competitor":   ["Update competitive intelligence file", "Prepare defensive talking points for key accounts"],
    "supply_chain": ["Check with procurement on exposure", "Prepare customer communication if delays expected"],
    "retail":       ["Brief key account managers", "Review listing strategy for affected channels"],
    "innovation":   ["Share with R&D/innovation team", "Assess consumer relevance for our markets"],
    "trend":        ["Include in next customer presentation", "Align marketing messaging"],
    "macro":        ["Factor into demand planning", "Adjust forecasts if needed"],
}

# ═══════════════════════════════════════════════════════════════
# RSS FETCHING
# ═══════════════════════════════════════════════════════════════

def parse_date(date_str):
    if not date_str:
        return NOW
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%d %b %Y %H:%M:%S %z"]:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
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

def normalize_url(url):
    try:
        p = urlparse(url)
        return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))
    except Exception:
        return url

def fetch_feed(feed):
    """Fetch a single RSS feed, return list of raw items."""
    headers = {
        "User-Agent": "BeverageSalesIntel/2.0 (market-research)",
        "Accept": "application/rss+xml, application/xml, text/xml",
    }
    try:
        resp = requests.get(feed["url"], headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        return [], str(e)

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)
    results = []

    for item in items:
        def get(tag):
            el = item.find(tag) or item.find(f"atom:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        link = get("link")
        if not link:
            link_el = item.find("atom:link", ns)
            link = link_el.get("href", "") if link_el is not None else ""

        pub = parse_date(get("pubDate") or get("published") or get("updated"))
        title = get("title")
        desc = clean_html(get("description") or get("content") or get("summary"))
        if len(desc) > 400:
            desc = desc[:397].rsplit(" ", 1)[0] + "…"

        results.append({
            "title": title,
            "url": link,
            "summary": desc,
            "published": pub,
            "source": feed["name"],
            "tier": feed["tier"],
            "feed_regions": feed["regions"],
        })

    return results, None

# ═══════════════════════════════════════════════════════════════
# REGION ASSIGNMENT
# ═══════════════════════════════════════════════════════════════

def assign_region(text, feed_regions):
    """Assign article to specific regions based on keyword matching."""
    t = text.lower()
    matched = []
    for rid, rdef in REGIONS.items():
        # Check negative keywords first
        if any(neg in t for neg in rdef["negative"]):
            continue
        if any(kw in t for kw in rdef["keywords"]):
            matched.append(rid)

    # If feed is region-specific and no match found, trust feed assignment
    if not matched and feed_regions != ["global"]:
        matched = [r for r in feed_regions if r in REGIONS]

    return matched if matched else ["global"]

# ═══════════════════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════════════════

def deduplicate(articles):
    """Remove exact URL dupes and near-duplicate titles."""
    seen_urls = {}
    seen_titles = []
    unique = []

    for a in articles:
        nurl = normalize_url(a["url"])
        if nurl in seen_urls:
            continue
        seen_urls[nurl] = True

        # Title similarity check
        title_lower = a["title"].lower().strip()
        is_dupe = False
        for prev_title in seen_titles:
            if SequenceMatcher(None, title_lower, prev_title).ratio() > 0.8:
                is_dupe = True
                break
        if is_dupe:
            continue

        seen_titles.append(title_lower)
        unique.append(a)

    return unique

# ═══════════════════════════════════════════════════════════════
# SCORING
# ═══════════════════════════════════════════════════════════════

CATEGORY_PRIORITY = {
    "regulatory": 1.0, "pricing": 0.9, "launch": 0.85, "competitor": 0.8,
    "supply_chain": 0.7, "retail": 0.65, "innovation": 0.6, "trend": 0.5, "macro": 0.4,
}

SOURCE_RELIABILITY = {1: 1.0, 2: 0.95, 3: 0.7, 4: 0.5}

def score_article(article, region_match_strength):
    pub = article["published"]
    age_days = max(0, (NOW - pub).total_seconds() / 86400)

    # Recency: exponential decay, 14-day half-life
    import math
    recency = math.exp(-0.05 * age_days)

    cat_priority = CATEGORY_PRIORITY.get(article.get("category", "trend"), 0.5)
    source_rel = SOURCE_RELIABILITY.get(article.get("tier", 4), 0.5)
    region_score = {"high": 1.0, "medium": 0.6, "low": 0.3}.get(region_match_strength, 0.3)

    return round(0.50 * recency + 0.25 * region_score + 0.15 * cat_priority + 0.10 * source_rel, 4)

# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_pipeline():
    print("=" * 60)
    print(f"  Sales Intelligence Pipeline — {NOW.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    errors = []
    source_stats = {"ok": 0, "failed": 0}
    all_raw = []

    # ── Step 1: Fetch all feeds ──
    print(f"\n  [1/6] FETCHING {len(ALL_FEEDS)} feeds...")
    for feed in ALL_FEEDS:
        items, err = fetch_feed(feed)
        if err:
            errors.append({"region": ",".join(feed["regions"]), "source": feed["name"], "error": err, "time": NOW.isoformat()})
            source_stats["failed"] += 1
        else:
            source_stats["ok"] += 1
        all_raw.extend(items)
        if items:
            print(f"    ✓ {feed['name']}: {len(items)} items")
        elif err:
            print(f"    ✗ {feed['name']}: {err[:60]}")

    print(f"  Total raw items: {len(all_raw)}")

    # ── Step 2: Filter for beverage relevance ──
    print("\n  [2/6] FILTERING for relevance...")
    relevant = [a for a in all_raw if is_beverage_relevant(f"{a['title']} {a['summary']}")]
    print(f"  Relevant: {len(relevant)} / {len(all_raw)}")

    # ── Step 3: Enrich (category, entities, tags, regions) ──
    print("\n  [3/6] ENRICHING articles...")
    for a in relevant:
        text = f"{a['title']} {a['summary']}"
        a["category"] = detect_category(text)
        a["entities"] = extract_entities(text)
        a["product_tags"] = extract_product_tags(text)
        a["assigned_regions"] = assign_region(text, a["feed_regions"])

    # ── Step 4: Assign to regions, dedupe, score, window ──
    print("\n  [4/6] BUILDING region buckets...")
    region_articles = {r: [] for r in REGIONS}
    global_articles = []

    for a in relevant:
        for r in a["assigned_regions"]:
            if r in region_articles:
                region_articles[r].append(a)
            else:
                global_articles.append(a)

    # Dedupe per region and apply time window
    region_news = {}
    health = {}

    for rid in REGIONS:
        articles = region_articles[rid]
        articles = deduplicate(articles)

        # Apply window
        in_window = [a for a in articles if a["published"] >= CUTOFF]
        used_fallback = False

        if len(in_window) < MIN_ITEMS:
            # Expand to fallback window
            in_window = [a for a in articles if a["published"] >= FALLBACK_CUTOFF]
            used_fallback = True

        # Score
        for a in in_window:
            text = f"{a['title']} {a['summary']}"
            t = text.lower()
            # Region match strength
            rdef = REGIONS[rid]
            strong = sum(1 for kw in rdef["keywords"] if kw in t)
            strength = "high" if strong >= 2 else "medium" if strong >= 1 else "low"
            a["region_match"] = strength
            a["confidence"] = "high" if a["published"] >= CUTOFF and strength in ("high", "medium") else "low" if used_fallback else "medium"
            a["score"] = score_article(a, strength)
            a["why_it_matters"] = WHY_TEMPLATES.get(a["category"], "")
            a["sales_angles"] = SALES_ANGLE_TEMPLATES.get(a["category"], [])

        in_window.sort(key=lambda x: x["score"], reverse=True)

        # Format for output
        formatted = []
        for a in in_window[:50]:  # cap at 50 per region
            formatted.append({
                "title": a["title"],
                "url": a["url"],
                "source": a["source"],
                "published": a["published"].isoformat(),
                "country_region": rid,
                "category": a["category"],
                "entities": a["entities"],
                "product_tags": a["product_tags"],
                "summary": a["summary"],
                "why_it_matters": a["why_it_matters"],
                "sales_angles": a["sales_angles"],
                "confidence": a["confidence"],
                "score": a["score"],
            })

        region_news[rid] = formatted

        # Health
        notes = []
        if used_fallback:
            notes.append(f"Expanded to {FALLBACK_DAYS}-day window (only {len([a for a in articles if a['published'] >= CUTOFF])} items in {WINDOW_DAYS}d)")
        if len(formatted) < MIN_ITEMS:
            notes.append(f"Below {MIN_ITEMS}-item threshold: {len(formatted)} items")

        last_date = max((a["published"] for a in in_window), default=None)
        health[rid] = {
            "status": "error" if len(formatted) == 0 else "warning" if len(formatted) < MIN_ITEMS or used_fallback else "ok",
            "items": len(formatted),
            "sources_ok": source_stats["ok"],
            "sources_failed": source_stats["failed"],
            "last_item_date": last_date.isoformat() if last_date else None,
            "notes": notes,
        }

        print(f"    {rid}: {len(formatted)} items | {health[rid]['status']}")

    # ── Step 5: Generate briefings ──
    print("\n  [5/6] GENERATING briefings...")
    briefings = {}

    for rid, articles in region_news.items():
        rname = REGIONS[rid]["name"]

        # Executive summary from top-scored articles
        exec_summary = []
        for a in articles[:5]:
            exec_summary.append({
                "headline": a["title"],
                "detail": a["summary"][:200] if a["summary"] else "",
                "evidence_urls": [a["url"]],
                "confidence": a["confidence"],
            })

        # Key launches
        launches = [a for a in articles if a["category"] == "launch"][:5]
        key_launches = [{
            "title": a["title"],
            "company": ", ".join(a["entities"]["companies"][:2]) or "Unknown",
            "product": ", ".join(a["product_tags"][:2]) or "beverage",
            "angle": a["sales_angles"][0] if a["sales_angles"] else "",
            "evidence_url": a["url"],
            "date": a["published"],
        } for a in launches]

        # Competitor moves
        comp_moves = [a for a in articles if a["category"] == "competitor"][:5]
        competitor_moves = [{
            "title": a["title"],
            "company": ", ".join(a["entities"]["companies"][:2]) or "Unknown",
            "move_type": a["category"],
            "impact": a["why_it_matters"],
            "evidence_url": a["url"],
            "date": a["published"],
        } for a in comp_moves]

        # Regulatory watch
        reg_items = [a for a in articles if a["category"] == "regulatory"][:5]
        regulatory_watch = [{
            "title": a["title"],
            "topic": ", ".join(a["product_tags"][:2]) or "regulation",
            "impact_on_sales": a["why_it_matters"],
            "evidence_url": a["url"],
            "date": a["published"],
        } for a in reg_items]

        # Pricing
        price_items = [a for a in articles if a["category"] == "pricing"][:5]
        pricing_promos = [{
            "title": a["title"],
            "what_changed": a["summary"][:150],
            "sales_risk_or_opportunity": a["why_it_matters"],
            "evidence_url": a["url"],
            "date": a["published"],
        } for a in price_items]

        # Signals: topic frequency analysis
        all_text = " ".join(f"{a['title']} {a['summary']}" for a in articles).lower()
        cat_counter = Counter(a["category"] for a in articles)
        tag_counter = Counter(tag for a in articles for tag in a["product_tags"])

        signals = []
        for tag, count in tag_counter.most_common(5):
            signals.append({
                "signal": f"{tag.replace('_', ' ').title()} trending in {rname}",
                "explanation": f"{count} articles mention {tag.replace('_', ' ')} in the last {WINDOW_DAYS} days",
                "support_count": count,
                "top_keywords": [tag],
                "confidence": "high" if count >= 5 else "medium" if count >= 2 else "low",
            })

        # Talking points
        talking_points = []
        if launches:
            talking_points.append({
                "customer_type": "retail",
                "pitch": f"{len(launches)} new product launches in {rname} — ask about shelf space for new formats",
                "supporting_evidence_urls": [a["url"] for a in launches[:3]],
            })
        if reg_items:
            talking_points.append({
                "customer_type": "key_account",
                "pitch": f"Regulatory changes in {rname} may affect product specs — position as proactive compliance partner",
                "supporting_evidence_urls": [a["url"] for a in reg_items[:3]],
            })
        if tag_counter.get("functional", 0) >= 2:
            talking_points.append({
                "customer_type": "distributor",
                "pitch": f"Functional beverage demand rising in {rname} — expand functional SKU range in next order",
                "supporting_evidence_urls": [a["url"] for a in articles if "functional" in a.get("product_tags", [])][:3],
            })

        # Recommended actions
        actions = []
        if launches:
            actions.append({
                "owner": "sales",
                "action": f"Review {len(launches)} new launches for competitive overlap",
                "why_now": "New products entering market require positioning response",
                "evidence_urls": [a["url"] for a in launches[:3]],
            })
        if reg_items:
            actions.append({
                "owner": "sales",
                "action": "Brief quality team on regulatory developments",
                "why_now": "Compliance deadlines may affect product specifications",
                "evidence_urls": [a["url"] for a in reg_items[:3]],
            })

        briefings[rid] = {
            "executive_summary": exec_summary,
            "key_launches": key_launches,
            "competitor_moves": competitor_moves,
            "regulatory_watch": regulatory_watch,
            "pricing_promotions": pricing_promos,
            "signals": signals,
            "talking_points": talking_points,
            "recommended_actions": actions,
        }

    # ── Step 6: Build market_stats.json ──
    print("\n  [6/6] BUILDING market stats...")
    market_stats = {"generated_at": NOW.isoformat(), "regions": {}}

    # Static market context with full transparency
    MARKET_DATA = {
        "usa": {
            "market_size": {"value": 265, "unit": "USD_B", "year": 2024,
                "method": "manual_estimate",
                "sources": [{"name": "Statista – US Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/united-states"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Order-of-magnitude estimate from Statista public preview. Includes soft drinks, juice, water, energy, RTD coffee/tea."},
            "growth": {"value": 3.0, "unit": "pct", "period": "YoY",
                "method": "manual_estimate",
                "sources": [{"name": "Statista – US Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/united-states"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Approximate YoY growth rate from public market summaries."},
        },
        "germany": {
            "market_size": {"value": 29, "unit": "EUR_B", "year": 2024,
                "method": "manual_estimate",
                "sources": [{"name": "Statista – DE Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/germany"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Order-of-magnitude estimate. DACH region context."},
            "growth": {"value": 2.0, "unit": "pct", "period": "YoY",
                "method": "manual_estimate",
                "sources": [{"name": "Statista DE", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/germany"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Approximate growth rate."},
        },
        "france": {
            "market_size": {"value": 22, "unit": "EUR_B", "year": 2024,
                "method": "manual_estimate",
                "sources": [{"name": "Statista – FR Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/france"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Order-of-magnitude estimate."},
            "growth": {"value": 2.0, "unit": "pct", "period": "YoY",
                "method": "manual_estimate",
                "sources": [{"name": "Statista FR", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/france"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Approximate growth rate."},
        },
        "spain": {
            "market_size": {"value": 12, "unit": "EUR_B", "year": 2024,
                "method": "manual_estimate",
                "sources": [{"name": "Statista – ES Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/spain"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Order-of-magnitude estimate."},
            "growth": {"value": 3.0, "unit": "pct", "period": "YoY",
                "method": "manual_estimate",
                "sources": [{"name": "Statista ES", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/spain"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Approximate growth rate."},
        },
        "italy": {
            "market_size": {"value": 18, "unit": "EUR_B", "year": 2024,
                "method": "manual_estimate",
                "sources": [{"name": "Statista – IT Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/italy"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Order-of-magnitude estimate."},
            "growth": {"value": 3.0, "unit": "pct", "period": "YoY",
                "method": "manual_estimate",
                "sources": [{"name": "Statista IT", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/italy"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Approximate growth rate."},
        },
        "austria": {
            "market_size": {"value": 5, "unit": "EUR_B", "year": 2024,
                "method": "manual_estimate",
                "sources": [{"name": "Statista – AT Non-Alcoholic Beverages", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/austria"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Order-of-magnitude estimate. Red Bull home market."},
            "growth": {"value": 2.0, "unit": "pct", "period": "YoY",
                "method": "manual_estimate",
                "sources": [{"name": "Statista AT", "url": "https://www.statista.com/outlook/cmo/non-alcoholic-drinks/austria"}],
                "last_verified": "2025-01-15", "confidence": "medium",
                "notes": "Approximate growth rate."},
        },
    }

    for rid, mdata in MARKET_DATA.items():
        market_stats["regions"][rid] = {
            "market_context": {
                "currency": REGIONS[rid]["currency"],
                "market_size": mdata["market_size"],
                "growth": mdata["growth"],
            },
            "sales_relevance_notes": [
                f"{len(region_news.get(rid, []))} intelligence items tracked in last {WINDOW_DAYS} days.",
            ],
        }

    # ── SAVE ALL OUTPUTS ──
    print("\n  SAVING outputs...")

    # sales_news.json
    sources_used = {rid: list({a["source"] for a in arts}) for rid, arts in region_news.items()}
    sales_news_out = {
        "generated_at": NOW.isoformat(),
        "window_days": WINDOW_DAYS,
        "regions": region_news,
        "meta": {
            "sources_used": sources_used,
            "errors": errors,
            "counts": {rid: len(arts) for rid, arts in region_news.items()},
        },
    }
    with open(OUT_DIR / "sales_news.json", "w", encoding="utf-8") as f:
        json.dump(sales_news_out, f, ensure_ascii=False, indent=2)
    print(f"  ✓ sales_news.json ({sum(len(a) for a in region_news.values())} items)")

    # sales_briefings.json
    briefings_out = {
        "generated_at": NOW.isoformat(),
        "window_days": WINDOW_DAYS,
        "regions": briefings,
    }
    with open(OUT_DIR / "sales_briefings.json", "w", encoding="utf-8") as f:
        json.dump(briefings_out, f, ensure_ascii=False, indent=2)
    print(f"  ✓ sales_briefings.json")

    # market_stats.json
    with open(OUT_DIR / "market_stats.json", "w", encoding="utf-8") as f:
        json.dump(market_stats, f, ensure_ascii=False, indent=2)
    print(f"  ✓ market_stats.json")

    # data_health.json
    health_out = {
        "generated_at": NOW.isoformat(),
        "regions": health,
        "global": {
            "total_items": sum(len(a) for a in region_news.values()),
            "total_sources_ok": source_stats["ok"],
            "total_sources_failed": source_stats["failed"],
        },
    }
    with open(OUT_DIR / "data_health.json", "w", encoding="utf-8") as f:
        json.dump(health_out, f, ensure_ascii=False, indent=2)
    print(f"  ✓ data_health.json")

    # Also generate briefing.json for backward compatibility with existing sales.html
    # (morning briefing + region signals)
    top_topics = Counter()
    for arts in region_news.values():
        for a in arts:
            for tag in a.get("product_tags", []):
                top_topics[tag] += 1
            top_topics[a["category"]] += 1

    top3 = [t[0] for t in top_topics.most_common(3)]
    theme_phrases = {
        "energy": "energy drinks leading growth",
        "functional": "functional beverages gaining momentum",
        "launch": "new product launches intensifying",
        "regulatory": "regulatory changes reshaping strategies",
        "pricing": "pricing pressures in focus",
        "organic": "organic demand accelerating",
        "sugar_free": "sugar-free reformulation trending",
        "rtd": "RTD formats expanding",
        "alcohol_free": "no/low alcohol segment growing",
        "juice": "juice market under transformation",
        "trend": "consumer trends shifting",
    }

    briefing_sentences = []
    total_items = sum(len(a) for a in region_news.values())
    active_regions = sum(1 for a in region_news.values() if a)
    briefing_sentences.append(f"Sales intelligence tracked {total_items} items across {active_regions} regions in the past {WINDOW_DAYS} days.")
    if top3:
        themes = [theme_phrases.get(t, f"{t} trending") for t in top3]
        briefing_sentences.append(f"Key themes: {'; '.join(themes)}.")
    # Latest headline
    all_sorted = sorted(
        [a for arts in region_news.values() for a in arts],
        key=lambda x: x.get("published", ""), reverse=True,
    )
    if all_sorted:
        briefing_sentences.append(f"Latest: {all_sorted[0]['title'][:100]}.")

    def make_signal(rid):
        arts = region_news.get(rid, [])
        if not arts:
            return "No recent data — monitoring."
        tags = Counter(tag for a in arts for tag in a.get("product_tags", []))
        cats = Counter(a["category"] for a in arts)
        top_tag = tags.most_common(1)[0][0] if tags else None
        top_cat = cats.most_common(1)[0][0] if cats else "market"

        if top_tag:
            return f"{top_tag.replace('_',' ').title()} trending. ({len(arts)} items)"
        return f"{top_cat.title()} activity dominant. ({len(arts)} items)"

    briefing_json = {
        "generated_at": NOW.isoformat(),
        "generated_date": NOW.strftime("%A, %d %B %Y"),
        "briefing": " ".join(briefing_sentences),
        "signals": {rid: make_signal(rid) for rid in REGIONS},
        "meta": {
            "total_articles_analyzed": total_items,
            "method": "rss-analysis",
            "top_topics": dict(top_topics.most_common(10)),
        },
    }
    with open(OUT_DIR / "briefing.json", "w", encoding="utf-8") as f:
        json.dump(briefing_json, f, ensure_ascii=False, indent=2)
    print(f"  ✓ briefing.json (backward compat)")

    # ── Summary ──
    print(f"\n  {'='*50}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Total items: {total_items}")
    print(f"  Sources OK: {source_stats['ok']} / Failed: {source_stats['failed']}")
    for rid in REGIONS:
        h = health[rid]
        print(f"    {rid}: {h['items']} items [{h['status']}]")
    print(f"  {'='*50}\n")


if __name__ == "__main__":
    run_pipeline()
