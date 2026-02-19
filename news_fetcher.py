# =============================================================
# news_fetcher.py — Red Fruit Crop Monitor · News Fetcher
#
# FILTERING RULES:
#   - Crops (sour cherry, raspberry, etc.): GLOBAL — no geographic
#     filter. Articles from any country are welcome.
#   - Concentrates & juices: GLOBAL — any fruit, any country.
#     If it mentions NFC, juice concentrate, frozen fruit
#     or fruit processing — include it.
#   - No geographic restriction on news whatsoever.
#
# Keeps articles for 14 days. Saves to news.json.
# Run manually:  python news_fetcher.py
# Scheduled:     scheduler.py runs this daily at 07:15
# =============================================================

import json
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    raise SystemExit("Missing package. Run: pip install requests")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("  [INFO] BeautifulSoup not installed — run: pip install beautifulsoup4 lxml")

# =============================================================
# CONFIGURATION
# =============================================================

NEWS_FILE        = Path(__file__).parent / "news.json"
ARTICLE_TTL_DAYS = 14
MAX_ARTICLES     = 60       # increased — more topics now
REQUEST_TIMEOUT  = 15

# =============================================================
# RSS SOURCES
# =============================================================

RSS_SOURCES = [
    # ── Industry trade press ─────────────────────────────────
    { "name": "FreshPlaza",            "url": "https://www.freshplaza.com/rss/" },
    { "name": "FreshPlaza Europe",     "url": "https://www.freshplaza.com/europe/rss/" },
    { "name": "FreshPlaza Latin America","url":"https://www.freshplaza.com/latin-america/rss/" },
    { "name": "Eurofresh Distribution","url": "https://www.eurofresh-distribution.com/feed/" },
    { "name": "FreshFruitPortal",      "url": "https://www.freshfruitportal.com/feed/" },
    { "name": "Produce Business",      "url": "https://www.producebusiness.com/feed/" },
    { "name": "Hortidaily",            "url": "https://www.hortidaily.com/rss/" },
    { "name": "The Packer",            "url": "https://www.thepacker.com/rss.xml" },
    { "name": "Fruit Processing Mag",  "url": "https://www.fruit-processing.com/feed/" },

    # ── Google News: your 8 monitored crops — GLOBAL, no geo filter ──
    # ── Crop-specific queries use EXACT PHRASES to avoid false positives ──
    # "raspberry crop" not just "raspberry" — prevents Raspberry Pi results
    # "strawberry harvest" not just "strawberry" — prevents lifestyle/recipe results
    { "name": "GNews: Sour Cherry",
      "url": "https://news.google.com/rss/search?q=%22sour+cherry%22+OR+%22tart+cherry%22+OR+%22morello+cherry%22+(harvest+OR+crop+OR+production+OR+price+OR+export)&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Black Currant",
      "url": "https://news.google.com/rss/search?q=%22black+currant%22+OR+%22blackcurrant%22+(harvest+OR+crop+OR+production+OR+price+OR+export+OR+concentrate)&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Red Currant",
      "url": "https://news.google.com/rss/search?q=%22red+currant%22+OR+%22redcurrant%22+(harvest+OR+crop+OR+production+OR+price+OR+export)&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Raspberry",
      "url": "https://news.google.com/rss/search?q=(%22raspberry+crop%22+OR+%22raspberry+harvest%22+OR+%22raspberry+production%22+OR+%22raspberry+price%22+OR+%22frozen+raspberry%22+OR+%22raspberry+concentrate%22+OR+%22raspberry+export%22)+-raspberry+pi+-stock+-finance+-banking&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Strawberry",
      "url": "https://news.google.com/rss/search?q=%22strawberry+crop%22+OR+%22strawberry+harvest%22+OR+%22strawberry+production%22+OR+%22strawberry+price%22+OR+%22frozen+strawberry%22+OR+%22strawberry+export%22+OR+%22strawberry+grower%22&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Blueberry",
      "url": "https://news.google.com/rss/search?q=%22blueberry+crop%22+OR+%22blueberry+harvest%22+OR+%22blueberry+production%22+OR+%22blueberry+price%22+OR+%22frozen+blueberry%22+OR+%22blueberry+export%22&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Elderberry",
      "url": "https://news.google.com/rss/search?q=%22elderberry+juice%22+OR+%22elderberry+extract%22+OR+%22elderberry+production%22+OR+%22elderberry+market%22+OR+%22sambucus+nigra%22&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Rhubarb",
      "url": "https://news.google.com/rss/search?q=%22rhubarb+harvest%22+OR+%22rhubarb+production%22+OR+%22rhubarb+crop%22+OR+%22rhubarb+market%22&hl=en&gl=US&ceid=US:en" },

    # ── Google News: concentrates & juices — ANY fruit, GLOBAL ───
    # This is the new category: NFC, juice concentrate, frozen fruit,
    # fruit processing — relevant regardless of fruit type or country
    { "name": "GNews: Fruit Concentrate",
      "url": "https://news.google.com/rss/search?q=%22fruit+concentrate%22+OR+%22juice+concentrate%22+price+OR+market+OR+supply&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: NFC Juice",
      "url": "https://news.google.com/rss/search?q=%22not+from+concentrate%22+OR+%22NFC+juice%22+OR+%22NFC+berry%22+market+OR+price&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Frozen Fruit",
      "url": "https://news.google.com/rss/search?q=%22frozen+fruit%22+OR+%22IQF+fruit%22+market+OR+price+OR+supply&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Fruit Processing",
      "url": "https://news.google.com/rss/search?q=%22fruit+processing%22+OR+%22fruit+juice+industry%22+market+OR+price+OR+supply&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Orange Juice Concentrate",
      "url": "https://news.google.com/rss/search?q=%22orange+juice+concentrate%22+OR+%22FCOJ%22+price+OR+market+OR+supply&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Berry Juice Market",
      "url": "https://news.google.com/rss/search?q=%22berry+juice%22+OR+%22berry+concentrate%22+market+OR+price+OR+supply&hl=en&gl=US&ceid=US:en" },
    { "name": "GNews: Cherry Concentrate",
      "url": "https://news.google.com/rss/search?q=%22cherry+concentrate%22+OR+%22cherry+juice%22+market+OR+price+OR+supply&hl=en&gl=US&ceid=US:en" },
]

# =============================================================
# KEYWORD RULES
# =============================================================

# Your 8 monitored crops — global, any country
CROP_KEYWORDS = {
    # ── Use EXACT / highly specific phrases to avoid false positives ──
    # "raspberry" alone matches "Raspberry Pi" — require agricultural context IN the keyword itself
    "sour cherry":   ["sour cherry", "tart cherry", "sauerkirsche", "vişne", "višnja",
                      "morello cherry", "amarelle", "griotte"],
    "black currant": ["black currant", "blackcurrant", "schwarze johannisbeere", "cassis",
                      "ribis nigrum", "czarna porzeczka", "blackcurrant crop", "blackcurrant harvest"],
    "red currant":   ["red currant", "redcurrant", "rote johannisbeere", "czerwona porzeczka"],
    "raspberry":     ["raspberry crop", "raspberry harvest", "raspberry production", "raspberry grower",
                      "raspberry yield", "raspberry season", "raspberry export", "raspberry import",
                      "raspberry price", "raspberry market", "raspberry supply", "raspberry freeze",
                      "frozen raspberry", "raspberry concentrate", "raspberry juice",
                      "raspberry acreage", "raspberry farm", "raspberry plantation",
                      "himbeere", "framboise", "ahududu", "malina", "maline",
                      "raspberries crop", "raspberries harvest", "raspberries market"],
    "strawberry":    ["strawberry crop", "strawberry harvest", "strawberry production",
                      "strawberry grower", "strawberry yield", "strawberry season",
                      "strawberry export", "strawberry import", "strawberry price",
                      "strawberry market", "strawberry supply", "strawberry plantation",
                      "strawberry acreage", "strawberry farm", "strawberry freeze",
                      "frozen strawberry", "strawberry concentrate", "strawberry juice",
                      "erdbeere", "fraise", "çilek", "truskawka",
                      "strawberries crop", "strawberries harvest", "strawberries market",
                      "huelva strawberr", "egypt strawberr", "turkey strawberr"],
    "blueberry":     ["blueberry crop", "blueberry harvest", "blueberry production",
                      "blueberry grower", "blueberry yield", "blueberry season",
                      "blueberry export", "blueberry import", "blueberry price",
                      "blueberry market", "blueberry supply", "blueberry plantation",
                      "frozen blueberry", "blueberry concentrate", "blueberry juice",
                      "heidelbeere", "myrtille", "borówka",
                      "blueberries crop", "blueberries harvest", "blueberries market"],
    "rhubarb":       ["rhubarb crop", "rhubarb harvest", "rhubarb production",
                      "rhubarb market", "rhubarb season", "rhubarb grower",
                      "rhabarber", "rhubarbe", "rabarbar"],
    "elderberry":    ["elderberry crop", "elderberry harvest", "elderberry production",
                      "elderberry market", "elderberry juice", "elderberry extract",
                      "elderberry concentrate", "elderberry supply", "sambucus nigra",
                      "holunder", "sureau", "czarny bez"],
}

# ── HARD EXCLUSION — if ANY of these appear in title or summary, REJECT the article ──
# Prevents false positives like Raspberry Pi, nursing strawberry, cocktail recipes, etc.
EXCLUSION_KEYWORDS = [
    # Tech false positives
    "raspberry pi", "raspberrypi", "raspberry pi 4", "raspberry pi 5",
    "raspberry pi zero", "raspberry pi pico",
    # Medical / nursing
    "nurse", "nursing", "hospital", "patient", "medical", "clinical", "therapy",
    "diet tip", "smoothie recipe", "cocktail recipe", "dessert recipe",
    "beauty tip", "skin care", "skincare", "weight loss", "superfood",
    # Finance / banking — fruit crop names appearing in financial news
    "stock surge", "stock rally", "stock price", "share price", "market rally",
    "market cap", "ceo purchase", "banking & finance", "global banking",
    "investment", "investor", "hedge fund", "private equity", "ipo",
    "stock market", "nasdaq", "nyse", "cryptocurrency", "bitcoin", "blockchain",
    "quarterly earnings", "annual report", "revenue",
    # Non-food consumer products
    "raspberry ketone",   # weight loss supplement
    "strawberry blonde",  # hair color
    "strawberry moon",    # astronomy
    "strawberry shortcake",  # cartoon/dessert
    "elderflower",        # different product
    # Tech & gadgets
    "raspberry pi",       # repeated explicitly as catch-all
    "software", "firmware", "hardware", "app store", "android", "ios",
]

# Concentrate & juice keywords — any fruit, any country
CONCENTRATE_KEYWORDS = [
    "fruit concentrate", "juice concentrate", "not from concentrate", "NFC juice",
    "NFC berry", "IQF fruit", "frozen fruit market", "frozen berry",
    "fruit processing", "fruit juice industry", "berry concentrate",
    "cherry concentrate", "cherry juice concentrate", "blackcurrant concentrate",
    "currant concentrate", "raspberry concentrate", "strawberry concentrate",
    "blueberry concentrate", "elderberry concentrate", "elderberry juice",
    "orange juice concentrate", "FCOJ", "apple juice concentrate",
    "fruit puree", "aseptic fruit", "drum concentrate",
    "Brix", "degrees brix",
]

# Agricultural context — at least ONE must appear alongside a crop mention
# Much stricter than before — rules out tech, medical, lifestyle articles
CONTEXT_KEYWORDS = [
    "harvest", "crop", "production", "orchard", "plantation", "acreage",
    "grower", "farmer", "producer", "processor", "packhouse",
    "export", "import", "shipment", "trade",
    "frost", "freeze", "cold snap", "drought", "yield loss",
    "supply", "shortage", "surplus", "inventory", "stock",
    "price", "market price", "spot price", "wholesale",
    "season outlook", "crop forecast", "production forecast",
    "berry industry", "fruit industry", "agri",
]

# =============================================================
# ARTICLE CATEGORISATION
# =============================================================

def detect_crops(text: str) -> list[str]:
    """Return monitored crops mentioned anywhere in text."""
    t = text.lower()
    return [crop for crop, kws in CROP_KEYWORDS.items() if any(kw in t for kw in kws)]


def detect_concentrate(text: str) -> bool:
    """Return True if article is about concentrates/juices/frozen fruit."""
    t = text.lower()
    return any(kw.lower() in t for kw in CONCENTRATE_KEYWORDS)


def is_excluded(text: str) -> bool:
    """Return True if article matches any hard exclusion rule — must be rejected."""
    t = text.lower()
    return any(kw.lower() in t for kw in EXCLUSION_KEYWORDS)


def is_relevant(text: str) -> bool:
    """
    Include article ONLY if:
      (a) NOT excluded by hard exclusion list (Raspberry Pi, nursing, recipes etc.)
      AND
      (b1) mentions one of our 8 crops via SPECIFIC agricultural phrases
           AND at least one agricultural context keyword
      OR
      (b2) mentions concentrates/juices/frozen fruit trade keywords
           AND not excluded
    """
    if is_excluded(text):
        return False

    has_crop        = bool(detect_crops(text))
    has_context     = any(kw.lower() in text.lower() for kw in CONTEXT_KEYWORDS)
    has_concentrate = detect_concentrate(text)

    return (has_crop and has_context) or has_concentrate


def article_category(text: str, crops: list[str]) -> str:
    """Primary category for display in dashboard."""
    if detect_concentrate(text) and not crops:
        return "concentrate & juice"
    if crops:
        return crops[0]
    return "general"


def article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


# =============================================================
# RSS PARSING
# =============================================================

def parse_date(date_str: str) -> datetime:
    if not date_str:
        return datetime.now(timezone.utc)
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"]:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    if BS4_AVAILABLE:
        return BeautifulSoup(raw, "lxml").get_text(separator=" ", strip=True)
    import re
    return re.sub(r"<[^>]+>", " ", raw).strip()


def fetch_feed(source: dict) -> list[dict]:
    articles = []
    headers = {
        "User-Agent": "RedFruitCropMonitor/1.0 (procurement intelligence)",
        "Accept":     "application/rss+xml, application/xml, text/xml",
    }
    try:
        resp = requests.get(source["url"], headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        print(f"    ⚠️  {source['name']}: {e}")
        return []

    ns    = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)
    age_cutoff = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)

    for item in items:
        def get(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else ""

        title   = get("title")
        url     = get("link") or get("guid")
        summary = clean_html(get("description") or get("summary") or "")
        pub_dt  = parse_date(get("pubDate") or get("published") or get("updated"))

        if not title or not url:
            continue

        # Age filter — skip anything older than 14 days
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        if pub_dt < age_cutoff:
            continue

        full_text = f"{title} {summary}"

        if not is_relevant(full_text):
            continue

        crops      = detect_crops(full_text)
        is_conc    = detect_concentrate(full_text)
        category   = article_category(full_text, crops)

        # Truncate summary cleanly
        if len(summary) > 320:
            summary = summary[:317].rsplit(" ", 1)[0] + "…"

        articles.append({
            "id":          article_id(url),
            "title":       title,
            "summary":     summary,
            "url":         url,
            "source":      source["name"],
            "region":      source.get("region", "Global"),
            "crops":       crops,
            "crop":        category,          # primary category for filter
            "is_concentrate": is_conc,
            "published":   pub_dt.isoformat(),
            "fetched":     datetime.now(timezone.utc).isoformat(),
        })

    return articles


# =============================================================
# NEWS STORE
# =============================================================

def load_existing() -> list[dict]:
    if not NEWS_FILE.exists():
        return []
    try:
        with open(NEWS_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("articles", [])
    except Exception:
        return []


def remove_expired(articles: list[dict]) -> list[dict]:
    cutoff  = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)
    kept    = []
    expired = 0
    for a in articles:
        try:
            pub = datetime.fromisoformat(a["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            if pub >= cutoff:
                kept.append(a)
            else:
                expired += 1
        except Exception:
            kept.append(a)
    if expired:
        print(f"  🗑  Removed {expired} expired article(s).")
    return kept


def merge(existing: list[dict], new: list[dict]) -> list[dict]:
    existing_ids = {a["id"] for a in existing}
    added = 0
    for article in new:
        if article["id"] not in existing_ids:
            existing.append(article)
            existing_ids.add(article["id"])
            added += 1

    def pub_key(a):
        try:
            dt = datetime.fromisoformat(a["published"])
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    existing.sort(key=pub_key, reverse=True)
    print(f"  ✅ Added {added} new article(s). Total: {len(existing)}.")
    return existing[:MAX_ARTICLES]


def save(articles: list[dict]):
    payload = {
        "last_updated":   datetime.now(timezone.utc).isoformat(),
        "article_count":  len(articles),
        "articles":       articles,
    }
    with open(NEWS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  💾 Saved {len(articles)} articles → {NEWS_FILE}")


# =============================================================
# MAIN
# =============================================================

def run():
    print("=" * 60)
    print(f"  📰 News Fetcher — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Rules: crops = global (no geo filter)")
    print(f"         concentrates/juices = all fruits, global")
    print("=" * 60)

    existing = load_existing()
    print(f"  📂 {len(existing)} existing articles.")
    existing = remove_expired(existing)

    all_new = []
    for source in RSS_SOURCES:
        print(f"\n  🌐 {source['name']}...")
        fetched = fetch_feed(source)
        crops_found    = sum(1 for a in fetched if a["crops"])
        conc_found     = sum(1 for a in fetched if a["is_concentrate"] and not a["crops"])
        print(f"     → {len(fetched)} relevant ({crops_found} crop articles, {conc_found} concentrate/juice articles)")
        all_new.extend(fetched)

    print(f"\n  🔀 Merging...")
    merged = merge(existing, all_new)
    save(merged)

    # Summary
    from collections import Counter
    print("\n  📊 Articles by category:")
    cats = Counter(a["crop"] for a in merged)
    icons = {"sour cherry":"🍒","black currant":"🫐","red currant":"🔴","raspberry":"🫐",
             "strawberry":"🍓","blueberry":"🫐","rhubarb":"🌿","elderberry":"🍇",
             "concentrate & juice":"🥤","general":"📰"}
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"     {icons.get(cat,'📰')} {cat}: {count}")

    conc_total = sum(1 for a in merged if a.get("is_concentrate"))
    print(f"\n  🥤 Concentrate/juice articles: {conc_total} of {len(merged)} total")
    print("\n  Done.\n")
    return merged


if __name__ == "__main__":
    run()
