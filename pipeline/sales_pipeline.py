#!/usr/bin/env python3
"""
pipeline/sales_pipeline.py — Beverage Sales Intelligence Pipeline

Based on the PROVEN news_fetcher.py that successfully fetched 132+ articles.
Same RSS feeds, same filtering logic, reformatted outputs for sales.html.

Produces:
  sales_news.json       — articles per region
  sales_briefings.json  — briefings per region  
  market_stats.json     — market context
  data_health.json      — pipeline health
  briefing.json         — morning briefing + signals
"""

import json
import hashlib
import re
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
OUT_DIR          = Path(__file__).parent.parent  # repo root
ARTICLE_TTL_DAYS = 28
MAX_PER_REGION   = 50
REQUEST_TIMEOUT  = 15
NOW              = datetime.now(timezone.utc)
AGE_CUTOFF       = NOW - timedelta(days=ARTICLE_TTL_DAYS)

REGIONS = {
    "usa":     {"name": "United States", "currency": "USD"},
    "germany": {"name": "Germany",       "currency": "EUR"},
    "france":  {"name": "France",        "currency": "EUR"},
    "spain":   {"name": "Spain",         "currency": "EUR"},
    "italy":   {"name": "Italy",         "currency": "EUR"},
    "austria": {"name": "Austria",       "currency": "EUR"},
}

REGION_KEYWORDS = {
    "usa":     ["usa", "united states", "american", "fda", "us market", "north america"],
    "germany": ["germany", "german", "deutschland", "dach", "bundesrat", "lebensmittel"],
    "france":  ["france", "french", "paris", "francais", "egalim", "leclerc", "carrefour"],
    "spain":   ["spain", "spanish", "espana", "madrid", "barcelona", "mercadona", "horeca spain"],
    "italy":   ["italy", "italian", "italia", "milan", "rome", "aperitivo", "esselunga"],
    "austria": ["austria", "austrian", "wien", "vienna", "spar austria", "hofer", "alnatura"],
}

# ═══════════════════════════════════════════════════════════════
# RSS SOURCES — EXACTLY the ones that worked in news_fetcher.py
# ═══════════════════════════════════════════════════════════════
SALES_SOURCES = [
    # ── GLOBAL industry trade press ──
    # BeverageDaily & FoodNavigator RSS feeds are dead (404).
    # Instead: pull their content via Google News site: queries
    {"name": "GNews: BeverageDaily",
     "url": "https://news.google.com/rss/search?q=site:beveragedaily.com&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "launch"},
    {"name": "GNews: FoodNavigator",
     "url": "https://news.google.com/rss/search?q=site:foodnavigator.com+beverage+OR+drink+OR+juice&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "trend"},
    {"name": "GNews: FoodNavigator-USA",
     "url": "https://news.google.com/rss/search?q=site:foodnavigator-usa.com+beverage+OR+drink&hl=en&gl=US&ceid=US:en",
     "regions": ["usa"], "cat": "trend"},
    {"name": "GNews: Just-Drinks",
     "url": "https://news.google.com/rss/search?q=site:just-drinks.com&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "market"},
    # Working direct RSS feeds
    {"name": "FoodDive",
     "url": "https://www.fooddive.com/feeds/news/",
     "regions": ["usa"], "cat": "trend"},
    {"name": "Drinks Business",
     "url": "https://www.thedrinksbusiness.com/feed/",
     "regions": ["global"], "cat": "market"},

    # ── USA ──
    {"name": "GNews: US Beverage Launches",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(launch+OR+%22new+product%22+OR+%22new+range%22)+USA&hl=en&gl=US&ceid=US:en",
     "regions": ["usa"], "cat": "launch"},
    {"name": "GNews: US Beverage Trends",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(trend+OR+consumer+OR+demand+OR+market)+%22United+States%22&hl=en&gl=US&ceid=US:en",
     "regions": ["usa"], "cat": "trend"},
    {"name": "GNews: FDA Beverage Regulation",
     "url": "https://news.google.com/rss/search?q=FDA+(beverage+OR+drink+OR+juice)+(regulation+OR+labelling+OR+rule+OR+ban)&hl=en&gl=US&ceid=US:en",
     "regions": ["usa"], "cat": "regulation"},

    # ── GERMANY / DACH ──
    {"name": "GNews: FoodNavigator Germany",
     "url": "https://news.google.com/rss/search?q=site:foodnavigator.com+(germany+OR+german+OR+deutschland+OR+DACH+OR+austria)&hl=en&gl=DE&ceid=DE:en",
     "regions": ["germany", "austria"], "cat": "trend"},
    {"name": "GNews: Germany Beverage Market",
     "url": "https://news.google.com/rss/search?q=(Getraenk+OR+Getraenke+OR+Saft+OR+beverage+OR+drink)+(Markt+OR+market+OR+launch+OR+trend)+Deutschland&hl=de&gl=DE&ceid=DE:de",
     "regions": ["germany"], "cat": "market"},
    {"name": "GNews: Germany Juice/Beverage Launches",
     "url": "https://news.google.com/rss/search?q=(juice+OR+beverage+OR+energy+drink+OR+functional+drink)+(launch+OR+new+OR+trend)+(Germany+OR+DACH)&hl=en&gl=DE&ceid=DE:en",
     "regions": ["germany"], "cat": "launch"},
    {"name": "GNews: EU Beverage Regulation",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(regulation+OR+Nutri-Score+OR+PPWR+OR+packaging+OR+sugar+tax)+%22European+Union%22+OR+EU&hl=en&gl=DE&ceid=DE:en",
     "regions": ["germany", "france", "spain", "italy", "austria"], "cat": "regulation"},

    # ── FRANCE ──
    {"name": "GNews: France Beverage Market",
     "url": "https://news.google.com/rss/search?q=(boisson+OR+jus+OR+beverage+OR+drink)+(marche+OR+lancement+OR+tendance+OR+market+OR+launch+OR+trend)+France&hl=fr&gl=FR&ceid=FR:fr",
     "regions": ["france"], "cat": "market"},
    {"name": "GNews: France Food Launches",
     "url": "https://news.google.com/rss/search?q=(juice+OR+beverage+OR+boisson)+(launch+OR+nouveau+OR+new+OR+trend)+France&hl=en&gl=FR&ceid=FR:en",
     "regions": ["france"], "cat": "launch"},
    {"name": "GNews: France EGAlim / Regulation",
     "url": "https://news.google.com/rss/search?q=(EGAlim+OR+Nutri-Score+OR+Eco-Score+OR+%22sugar+tax%22+OR+taxe+sucre)+(boisson+OR+jus+OR+beverage)&hl=fr&gl=FR&ceid=FR:fr",
     "regions": ["france"], "cat": "regulation"},

    # ── SPAIN ──
    {"name": "GNews: Spain Beverage Market",
     "url": "https://news.google.com/rss/search?q=(bebida+OR+zumo+OR+juice+OR+beverage)+(mercado+OR+market+OR+lanzamiento+OR+launch+OR+tendencia)+Spain+OR+Espana&hl=es&gl=ES&ceid=ES:es",
     "regions": ["spain"], "cat": "market"},
    {"name": "GNews: Spain Horeca Drinks",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(Horeca+OR+bar+OR+restaurant+OR+hotel)+Spain&hl=en&gl=ES&ceid=ES:en",
     "regions": ["spain"], "cat": "trend"},
    {"name": "GNews: Spain Food Regulation",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+bebida+OR+zumo)+(impuesto+OR+tax+OR+regulation+OR+etiquetado+OR+labelling)+Spain&hl=en&gl=ES&ceid=ES:en",
     "regions": ["spain"], "cat": "regulation"},

    # ── ITALY ──
    {"name": "GNews: Italy Beverage Market",
     "url": "https://news.google.com/rss/search?q=(bevanda+OR+succo+OR+beverage+OR+drink)+(mercato+OR+market+OR+lancio+OR+launch+OR+tendenza)+Italy+OR+Italia&hl=it&gl=IT&ceid=IT:it",
     "regions": ["italy"], "cat": "market"},
    {"name": "GNews: Italy Aperitivo Drinks",
     "url": "https://news.google.com/rss/search?q=(aperitivo+OR+aperitif+OR+spritz+OR+mixer)+(beverage+OR+drink+OR+juice+OR+succo)+Italy&hl=en&gl=IT&ceid=IT:en",
     "regions": ["italy"], "cat": "trend"},
    {"name": "GNews: Italy Food Launch",
     "url": "https://news.google.com/rss/search?q=(juice+OR+beverage+OR+functional+drink)+(launch+OR+nuovo+OR+new+OR+organic+OR+bio)+Italy&hl=en&gl=IT&ceid=IT:en",
     "regions": ["italy"], "cat": "launch"},

    # ── AUSTRIA ──
    {"name": "GNews: Austria Beverage",
     "url": "https://news.google.com/rss/search?q=(Getraenk+OR+Saft+OR+beverage+OR+drink+OR+juice)+(Markt+OR+market+OR+launch+OR+Bio+OR+organic)+Austria+OR+Oesterreich&hl=de&gl=AT&ceid=AT:de",
     "regions": ["austria"], "cat": "market"},
    {"name": "GNews: Red Bull / Austria Innovation",
     "url": "https://news.google.com/rss/search?q=(%22Red+Bull%22+OR+%22Rauch%22+OR+%22Voelkel%22)+(launch+OR+new+OR+organic+OR+innovation)&hl=en&gl=AT&ceid=AT:en",
     "regions": ["austria"], "cat": "launch"},

    # ── GLOBAL trends & pricing ──
    {"name": "GNews: Global Beverage Trends",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(trend+OR+%22consumer+trend%22+OR+%22market+trend%22+OR+innovation)+(2025+OR+2026)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "trend"},
    {"name": "GNews: Functional Beverages",
     "url": "https://news.google.com/rss/search?q=%22functional+beverage%22+OR+%22functional+drink%22+OR+%22adaptogen+drink%22+(launch+OR+market+OR+trend)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "trend"},
    {"name": "GNews: Energy Drinks Market",
     "url": "https://news.google.com/rss/search?q=%22energy+drink%22+(market+OR+launch+OR+regulation+OR+trend)+(Europe+OR+USA+OR+global)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "market"},
    {"name": "GNews: Sugar Tax Beverage",
     "url": "https://news.google.com/rss/search?q=%22sugar+tax%22+OR+%22sugar+levy%22+(beverage+OR+drink+OR+juice)+(Europe+OR+EU+OR+UK+OR+Germany+OR+France+OR+Spain)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "regulation"},
    {"name": "GNews: Beverage Pricing / Commodities",
     "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(price+OR+pricing+OR+cost+OR+inflation+OR+commodity)+(2025+OR+2026)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "pricing"},
    {"name": "GNews: RTD / Ready-to-Drink",
     "url": "https://news.google.com/rss/search?q=%22ready+to+drink%22+OR+%22RTD%22+(juice+OR+beverage+OR+coffee+OR+tea)+(launch+OR+market+OR+trend)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "launch"},
    {"name": "GNews: No-Low Alcohol Beverages",
     "url": "https://news.google.com/rss/search?q=(%22no-alcohol%22+OR+%22non-alcoholic%22+OR+%22low-alcohol%22+OR+%22alcohol-free%22)+(beverage+OR+drink)+(market+OR+launch+OR+trend)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "trend"},
    {"name": "GNews: EU Food Law / Packaging",
     "url": "https://news.google.com/rss/search?q=(PPWR+OR+%22EU+packaging%22+OR+%22Nutri-Score%22+OR+%22Farm+to+Fork%22)+(beverage+OR+drink+OR+food)&hl=en&gl=US&ceid=US:en",
     "regions": ["global"], "cat": "regulation"},
]

# ═══════════════════════════════════════════════════════════════
# KEYWORDS — EXACTLY from working news_fetcher.py
# ═══════════════════════════════════════════════════════════════
BEVERAGE_KEYWORDS = [
    "beverage", "drink", "juice", "soft drink", "energy drink", "smoothie",
    "water", "tea", "coffee", "rtd", "ready to drink", "functional",
    "boisson", "bebida", "getraenk", "succo", "saft", "jus",
    "carbonat", "sparkling", "still water", "flavour", "flavor",
    "launch", "new product", "innovation", "market", "trend", "consumer",
    "sugar tax", "nutri-score", "packaging", "regulation", "labelling",
    "prix", "price", "pricing", "commodity", "ingredient cost",
]

SALES_EXCLUSIONS = [
    "cryptocurrency", "bitcoin", "stock market", "nasdaq", "nyse",
    "real estate", "mortgage", "auto loan", "car insurance",
    "celebrity", "gossip", "sports score", "football result",
    "recipe", "cooking tip", "how to make", "diy",
]

CATEGORY_KEYWORDS = {
    "launch":     ["launch", "new product", "new range", "introduces", "unveil", "debut", "release"],
    "trend":      ["trend", "consumer", "demand", "growth", "market", "insight", "report", "forecast"],
    "pricing":    ["price", "cost", "inflation", "commodity", "margin", "tariff", "import cost"],
    "regulation": ["regulation", "regulat", "law", "directive", "tax", "ban", "label", "nutri", "ppwr", "egalim"],
    "market":     ["market", "share", "volume", "revenue", "sales", "retail", "channel"],
}

# Product tag detection
TAG_MAP = {
    "energy":       ["energy drink", "energy shot", "caffeine", "taurine", "guarana", "celsius", "monster", "red bull"],
    "functional":   ["functional", "adaptogen", "nootropic", "probiotic", "prebiotic", "gut health", "immunity"],
    "sugar_free":   ["sugar free", "zero sugar", "no sugar", "diet ", "low calorie", "stevia"],
    "juice":        ["juice", "nfc", "cold pressed", "smoothie", "nectar", "concentrate"],
    "rtd":          ["rtd", "ready to drink", "ready-to-drink"],
    "carbonated":   ["carbonated", "sparkling", "soda", "seltzer"],
    "alcohol_free": ["non-alcoholic", "alcohol-free", "zero alcohol", "alcohol free", "mocktail"],
    "organic":      ["organic", " bio "],
    "coffee_tea":   ["coffee", "tea ", "matcha", "cold brew", "iced tea"],
    "water":        ["water", "mineral water", "sparkling water"],
}

# Entity detection
COMPANIES = [
    "Coca-Cola", "PepsiCo", "Nestle", "Danone", "Red Bull", "Monster",
    "Celsius", "Keurig Dr Pepper", "Britvic", "Starbucks", "Oatly",
    "Fever-Tree", "San Pellegrino", "Rauch", "Eckes-Granini", "Tropicana",
    "Aldi", "Lidl", "Carrefour", "Leclerc", "Mercadona", "Walmart",
    "Campari", "Aperol", "Innocent",
]

# Why it matters templates
WHY_TEMPLATES = {
    "launch":     "New product launch signals competitive activity. Assess portfolio overlap and positioning.",
    "trend":      "Consumer trend shift. Consider portfolio alignment and marketing messaging.",
    "pricing":    "Pricing shift affects margins and positioning. Review contract and shelf price impact.",
    "regulation": "Regulatory change may require reformulation or relabeling. Monitor compliance timelines.",
    "market":     "Market development may create opportunities or threats. Brief key accounts.",
}

SALES_ANGLES = {
    "launch":     ["Map against portfolio for overlap", "Brief sales team on positioning"],
    "trend":      ["Include in next customer presentation", "Align marketing messaging"],
    "pricing":    ["Review pricing vs competitors", "Prepare margin impact analysis"],
    "regulation": ["Check compliance timeline", "Brief customers on regulatory impact"],
    "market":     ["Brief key account managers", "Review listing strategy"],
}

# ═══════════════════════════════════════════════════════════════
# RSS FETCHING — EXACTLY from working news_fetcher.py
# ═══════════════════════════════════════════════════════════════
def article_id(url):
    return hashlib.md5(url.encode()).hexdigest()[:12]

def parse_date(date_str):
    if not date_str:
        return NOW
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"]:
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

def fetch_rss(url, source_name):
    headers = {
        "User-Agent": "BeverageSalesIntelligence/1.0 (market research)",
        "Accept": "application/rss+xml, application/xml, text/xml",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        return [], str(e)[:80]

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)
    out = []

    for item in items:
        def g(tag):
            el = item.find(tag)
            if el is None:
                el = item.find(f"atom:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        title = g("title")
        link = g("link") or g("guid")
        if not link:
            le = item.find("atom:link", ns)
            link = le.get("href", "") if le is not None else ""
        summary = clean_html(g("description") or g("summary") or g("content") or "")
        pub = parse_date(g("pubDate") or g("published") or g("updated"))

        if not title or not link:
            continue
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        if pub < AGE_CUTOFF:
            continue
        if len(summary) > 400:
            summary = summary[:397].rsplit(" ", 1)[0] + "..."

        out.append({"title": title, "url": link, "summary": summary, "pub_dt": pub})

    return out, None

# ═══════════════════════════════════════════════════════════════
# FILTERING — EXACTLY from working news_fetcher.py
# ═══════════════════════════════════════════════════════════════
def is_excluded(text):
    t = text.lower()
    return any(kw in t for kw in SALES_EXCLUSIONS)

def is_beverage_relevant(text):
    t = text.lower()
    return any(kw in t for kw in BEVERAGE_KEYWORDS)

def detect_category(text, default_cat):
    t = text.lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            return cat
    return default_cat

def assign_regions(text, source_regions):
    """EXACTLY from working news_fetcher.py:
    - Region-specific source -> always assign to those regions
    - Global source -> check text for region keywords
    - Global with no match -> assign to ALL regions (key fix!)
    """
    if source_regions != ["global"]:
        return source_regions

    t = text.lower()
    matched = []
    for region, keywords in REGION_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            matched.append(region)

    # KEY: if global and no region match, assign to ALL regions
    return matched if matched else list(REGIONS.keys())

def tag_product(text):
    t = text.lower()
    return [tag for tag, kws in TAG_MAP.items() if any(kw in t for kw in kws)]

def extract_entities(text):
    t = text.lower()
    return {
        "companies": [c for c in COMPANIES if c.lower() in t],
        "ingredients": [],
        "packaging": [],
        "channels": [c for c in ["retail", "e-commerce", "online", "horeca", "foodservice"] if c in t],
    }

# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════
def run():
    print("=" * 60)
    print(f"  Sales Intelligence Pipeline — {NOW.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    errors = []
    stats = {"ok": 0, "fail": 0}
    all_articles = []  # flat list, each article has "regions" field

    # ── FETCH ALL FEEDS ──
    print(f"\n  [1/5] FETCHING {len(SALES_SOURCES)} feeds...")
    for src in SALES_SOURCES:
        regions_str = ", ".join(src["regions"])
        items, err = fetch_rss(src["url"], src["name"])
        if err:
            errors.append({"source": src["name"], "error": err, "time": NOW.isoformat()})
            stats["fail"] += 1
            print(f"    x {src['name']} [{regions_str}]: {err[:50]}")
            continue
        stats["ok"] += 1

        filter_kws = [k.lower() for k in src.get("filter_keywords", [])]
        accepted = 0

        for item in items:
            full_text = f"{item['title']} {item['summary']}"

            # Apply source-specific keyword filter
            if filter_kws and not any(kw in full_text.lower() for kw in filter_kws):
                continue

            if is_excluded(full_text):
                continue

            if not is_beverage_relevant(full_text):
                continue

            regions = assign_regions(full_text, src["regions"])
            cat = detect_category(full_text, src.get("cat", "market"))

            all_articles.append({
                "id":        article_id(item["url"]),
                "title":     item["title"],
                "summary":   item["summary"],
                "url":       item["url"],
                "source":    src["name"],
                "regions":   regions,
                "category":  cat,
                "published": item["pub_dt"],
                "product_tags": tag_product(full_text),
                "entities":  extract_entities(full_text),
                "why_it_matters": WHY_TEMPLATES.get(cat, ""),
                "sales_angles": SALES_ANGLES.get(cat, []),
            })
            accepted += 1

        if accepted > 0:
            print(f"    + {src['name']} [{regions_str}]: {accepted} articles")

    print(f"\n  Total articles: {len(all_articles)}")

    # ── DEDUP ──
    print("\n  [2/5] DEDUPLICATING...")
    seen_ids = set()
    unique = []
    for a in all_articles:
        if a["id"] not in seen_ids:
            seen_ids.add(a["id"])
            unique.append(a)
    print(f"  Unique: {len(unique)} (removed {len(all_articles) - len(unique)} dupes)")
    all_articles = unique

    # ── BUCKET INTO REGIONS ──
    print("\n  [3/5] BUCKETING into regions...")
    region_articles = {r: [] for r in REGIONS}

    for a in all_articles:
        for r in a["regions"]:
            if r in region_articles:
                region_articles[r].append(a)

    for r in REGIONS:
        # Sort by date (newest first)
        region_articles[r].sort(key=lambda x: x["published"], reverse=True)
        # Cap
        region_articles[r] = region_articles[r][:MAX_PER_REGION]
        print(f"    {r}: {len(region_articles[r])} articles")

    # ── FORMAT OUTPUTS ──
    print("\n  [4/5] FORMATTING outputs...")

    # 1. sales_news.json
    region_news = {}
    for rid, arts in region_articles.items():
        region_news[rid] = [{
            "id": a["id"],
            "title": a["title"],
            "summary": a["summary"],
            "url": a["url"],
            "source": a["source"],
            "published": a["published"].isoformat(),
            "country_region": rid,
            "category": a["category"],
            "entities": a["entities"],
            "product_tags": a["product_tags"],
            "why_it_matters": a["why_it_matters"],
            "sales_angles": a["sales_angles"],
            "confidence": "high" if rid in a["regions"] and a["regions"] != list(REGIONS.keys()) else "medium",
            "score": 1,
        } for a in arts]

    # 2. data_health.json
    health = {}
    for rid in REGIONS:
        n = len(region_news[rid])
        health[rid] = {
            "status": "ok" if n >= 10 else "warning" if n >= 3 else "error",
            "items": n,
            "sources_ok": stats["ok"],
            "sources_failed": stats["fail"],
            "last_item_date": region_news[rid][0]["published"] if region_news[rid] else None,
            "notes": [] if n >= 10 else [f"Below 10 threshold: {n}"],
        }

    # 3. sales_briefings.json
    briefings = {}
    for rid, arts in region_articles.items():
        rname = REGIONS[rid]["name"]

        exec_summary = [{"headline": a["title"], "detail": a["summary"][:200],
                         "evidence_urls": [a["url"]], "confidence": "medium"}
                        for a in arts[:5]]

        launches = [a for a in arts if a["category"] == "launch"][:5]
        key_launches = [{"title": a["title"],
                         "company": ", ".join(a["entities"]["companies"][:2]) or "-",
                         "product": ", ".join(a["product_tags"][:3]) or "beverage",
                         "angle": a["sales_angles"][0] if a["sales_angles"] else "",
                         "evidence_url": a["url"],
                         "date": a["published"].isoformat()} for a in launches]

        comp = [a for a in arts if a["category"] == "market"][:5]
        comp_moves = [{"title": a["title"],
                       "company": ", ".join(a["entities"]["companies"][:2]) or "-",
                       "move_type": "market", "impact": a["why_it_matters"],
                       "evidence_url": a["url"],
                       "date": a["published"].isoformat()} for a in comp]

        regs = [a for a in arts if a["category"] == "regulation"][:5]
        reg_watch = [{"title": a["title"],
                      "topic": ", ".join(a["product_tags"][:2]) or "regulation",
                      "impact_on_sales": a["why_it_matters"],
                      "evidence_url": a["url"],
                      "date": a["published"].isoformat()} for a in regs]

        price = [a for a in arts if a["category"] == "pricing"][:5]
        pricing = [{"title": a["title"],
                    "what_changed": a["summary"][:150],
                    "sales_risk_or_opportunity": a["why_it_matters"],
                    "evidence_url": a["url"],
                    "date": a["published"].isoformat()} for a in price]

        # Signals from tags
        tc = Counter(tag for a in arts for tag in a["product_tags"])
        cc = Counter(a["category"] for a in arts)
        sigs = [{"signal": f"{t.replace('_', ' ').title()} trending in {rname}",
                 "explanation": f"{c} articles mention {t.replace('_', ' ')}",
                 "support_count": c, "top_keywords": [t],
                 "confidence": "high" if c >= 5 else "medium" if c >= 2 else "low"}
                for t, c in tc.most_common(5)]
        if not sigs:
            sigs = [{"signal": f"{cat.replace('_', ' ').title()} activity in {rname}",
                     "explanation": f"{cnt} articles tracked",
                     "support_count": cnt, "top_keywords": [cat], "confidence": "medium"}
                    for cat, cnt in cc.most_common(3)]

        # Talking points
        tp = []
        if launches:
            tp.append({"customer_type": "retail",
                        "pitch": f"{len(launches)} new launches in {rname} - discuss shelf space",
                        "supporting_evidence_urls": [a["url"] for a in launches[:3]]})
        if regs:
            tp.append({"customer_type": "key_account",
                        "pitch": f"Regulatory changes in {rname} - position as compliance partner",
                        "supporting_evidence_urls": [a["url"] for a in regs[:3]]})
        if tc.get("functional", 0) >= 1:
            tp.append({"customer_type": "distributor",
                        "pitch": f"Functional beverage demand rising in {rname}",
                        "supporting_evidence_urls": [a["url"] for a in arts if "functional" in a.get("product_tags", [])][:3]})
        if not tp and arts:
            tp.append({"customer_type": "key_account",
                        "pitch": f"{len(arts)} developments tracked in {rname}",
                        "supporting_evidence_urls": [a["url"] for a in arts[:3]]})

        # Actions
        act = []
        if launches:
            act.append({"owner": "sales", "action": f"Review {len(launches)} launches for overlap",
                        "why_now": "Competitive response needed",
                        "evidence_urls": [a["url"] for a in launches[:3]]})
        if regs:
            act.append({"owner": "sales", "action": "Brief quality team on regulatory changes",
                        "why_now": "Compliance deadlines approaching",
                        "evidence_urls": [a["url"] for a in regs[:3]]})
        if not act and arts:
            act.append({"owner": "sales", "action": f"Review {len(arts)} items for {rname}",
                        "why_now": "Keep competitive awareness current",
                        "evidence_urls": [a["url"] for a in arts[:3]]})

        briefings[rid] = {
            "executive_summary": exec_summary,
            "key_launches": key_launches,
            "competitor_moves": comp_moves,
            "regulatory_watch": reg_watch,
            "pricing_promotions": pricing,
            "signals": sigs,
            "talking_points": tp,
            "recommended_actions": act,
        }

    # 4. market_stats.json
    MDATA = {
        "usa":     {"sz": 265, "u": "USD_B", "gr": 3.0},
        "germany": {"sz": 29,  "u": "EUR_B", "gr": 2.0},
        "france":  {"sz": 22,  "u": "EUR_B", "gr": 2.0},
        "spain":   {"sz": 12,  "u": "EUR_B", "gr": 3.0},
        "italy":   {"sz": 18,  "u": "EUR_B", "gr": 3.0},
        "austria": {"sz": 5,   "u": "EUR_B", "gr": 2.0},
    }
    market_stats = {"generated_at": NOW.isoformat(), "regions": {}}
    for rid, m in MDATA.items():
        src_url = f"https://www.statista.com/outlook/cmo/non-alcoholic-drinks/{rid.replace('usa', 'united-states')}"
        market_stats["regions"][rid] = {
            "market_context": {
                "currency": REGIONS[rid]["currency"],
                "market_size": {"value": m["sz"], "unit": m["u"], "year": 2024,
                    "method": "manual_estimate",
                    "sources": [{"name": "Statista", "url": src_url}],
                    "last_verified": "2025-01-15", "confidence": "medium",
                    "notes": "Order-of-magnitude estimate."},
                "growth": {"value": m["gr"], "unit": "pct", "period": "YoY",
                    "method": "manual_estimate",
                    "sources": [{"name": "Statista", "url": src_url}],
                    "last_verified": "2025-01-15", "confidence": "medium",
                    "notes": "Approximate growth rate."},
            },
            "sales_relevance_notes": [f"{len(region_news.get(rid, []))} items tracked."],
        }

    # 5. briefing.json
    all_unique = list({a["id"]: a for a in all_articles}.values())
    total = len(all_unique)
    active = sum(1 for r in region_articles.values() if r)

    top_cats = Counter(a["category"] for a in all_unique).most_common(3)
    cat_phrases = {"launch": "product launches", "regulation": "regulatory developments",
                   "pricing": "pricing shifts", "trend": "consumer trends",
                   "market": "market developments"}
    themes = [cat_phrases.get(c, c) for c, _ in top_cats]

    btext = f"Tracking {total} beverage intelligence items across {active} regions."
    if themes:
        btext += f" Top themes: {', '.join(themes)}."
    if all_unique:
        newest = max(all_unique, key=lambda a: a["published"])
        btext += f" Latest: {newest['title'][:100]}."

    def mk_signal(rid):
        arts = region_articles.get(rid, [])
        if not arts:
            return f"Expanding sources for {REGIONS[rid]['name']}."
        tc = Counter(tag for a in arts for tag in a.get("product_tags", []))
        cc = Counter(a["category"] for a in arts)
        n = len(arts)
        if tc:
            top = tc.most_common(1)[0][0].replace("_", " ").title()
            return f"{top} leading. {n} items tracked."
        if cc:
            top = cc.most_common(1)[0][0].replace("_", " ").title()
            return f"{top} activity. {n} items tracked."
        return f"{n} items tracked."

    # ── SAVE ALL ──
    print("\n  [5/5] SAVING...")

    def save(name, data):
        path = OUT_DIR / name
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"    + {name}")

    save("sales_news.json", {
        "generated_at": NOW.isoformat(), "window_days": ARTICLE_TTL_DAYS,
        "regions": region_news,
        "meta": {
            "sources_used": {r: list({a["source"] for a in arts}) for r, arts in region_articles.items()},
            "errors": errors,
            "counts": {r: len(arts) for r, arts in region_news.items()},
        },
    })

    save("sales_briefings.json", {
        "generated_at": NOW.isoformat(), "window_days": ARTICLE_TTL_DAYS,
        "regions": briefings,
    })

    save("market_stats.json", market_stats)

    save("data_health.json", {
        "generated_at": NOW.isoformat(),
        "regions": health,
        "global": {
            "total_items": sum(len(a) for a in region_news.values()),
            "total_sources_ok": stats["ok"],
            "total_sources_failed": stats["fail"],
        },
    })

    save("briefing.json", {
        "generated_at": NOW.isoformat(),
        "generated_date": NOW.strftime("%A, %d %B %Y"),
        "briefing": btext,
        "signals": {r: mk_signal(r) for r in REGIONS},
        "meta": {
            "total_articles_analyzed": total,
            "method": "rss-analysis",
            "top_topics": dict(Counter(
                tag for a in all_unique for tag in a.get("product_tags", [])
            ).most_common(10)),
        },
    })

    # ── SUMMARY ──
    total_items = sum(len(a) for a in region_news.values())
    print(f"\n  {'=' * 50}")
    print(f"  PIPELINE COMPLETE")
    print(f"  Total region items: {total_items}")
    print(f"  Sources: {stats['ok']} OK / {stats['fail']} failed")
    for rid in REGIONS:
        h = health[rid]
        print(f"    {rid}: {h['items']} items [{h['status']}]")
    print(f"  {'=' * 50}\n")


if __name__ == "__main__":
    run()
