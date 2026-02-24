# =============================================================
# news_fetcher.py — Red Fruit Crop Monitor + Sales Intelligence
#
# OUTPUTS:
#   news.json       — red fruit / concentrate articles (existing)
#   sales_news.json — beverage industry news per region (new)
#
# FILTERING:
#   Red fruit:  GLOBAL, crop-specific, exclusion list active
#   Sales news: Per-region beverage industry intelligence
#               Launches, trends, pricing, regulatory
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

# =============================================================
# CONFIGURATION
# =============================================================

NEWS_FILE        = Path(__file__).parent / "news.json"
SALES_NEWS_FILE  = Path(__file__).parent / "sales_news.json"
BRIEFING_FILE    = Path(__file__).parent / "briefing.json"
ARTICLE_TTL_DAYS = 14
MAX_ARTICLES     = 60
MAX_SALES_ARTICLES = 120   # more capacity — 6 regions × ~20 each
REQUEST_TIMEOUT  = 15

# =============================================================
# ── SECTION 1: RED FRUIT RSS SOURCES (unchanged) ────────────
# =============================================================

RSS_SOURCES = [
    { "name": "FreshPlaza",              "url": "https://www.freshplaza.com/rss/" },
    { "name": "FreshPlaza Europe",       "url": "https://www.freshplaza.com/europe/rss/" },
    { "name": "FreshPlaza Latin America","url": "https://www.freshplaza.com/latin-america/rss/" },
    { "name": "Eurofresh Distribution",  "url": "https://www.eurofresh-distribution.com/feed/" },
    { "name": "FreshFruitPortal",        "url": "https://www.freshfruitportal.com/feed/" },
    { "name": "Produce Business",        "url": "https://www.producebusiness.com/feed/" },
    { "name": "Hortidaily",              "url": "https://www.hortidaily.com/rss/" },
    { "name": "The Packer",              "url": "https://www.thepacker.com/rss.xml" },
    { "name": "Fruit Processing Mag",    "url": "https://www.fruit-processing.com/feed/" },
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
# FINAL PRO SALES INTELLIGENCE SOURCES
# Ingredient + Beverage Industry Intelligence
# =============================================================

SALES_RSS_SOURCES = [

# ---------- TRADE PRESS (BEST SIGNAL) ----------

{
"name":"BeverageDaily",
"url":"https://www.beveragedaily.com/arc/outboundfeeds/rss/",
"regions":["global"],
"cat":"launch"
},

{
"name":"FoodNavigator",
"url":"https://www.foodnavigator.com/arc/outboundfeeds/rss/",
"regions":["global"],
"cat":"trend"
},

{
"name":"FoodBev",
"url":"https://www.foodbev.com/feed/",
"regions":["global"],
"cat":"launch"
},

{
"name":"Drinks Business",
"url":"https://www.thedrinksbusiness.com/feed/",
"regions":["global"],
"cat":"market"
},

# ---------- USA ----------

{
"name":"FoodDive",
"url":"https://www.fooddive.com/feeds/news/",
"regions":["usa"],
"cat":"trend"
},

{
"name":"Food Business News",
"url":"https://www.foodbusinessnews.net/rss",
"regions":["usa"],
"cat":"market"
},

# ---------- GOOGLE NEWS SUPPORT ----------

{
"name":"Global Beverage Launches",
"url":"https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(launch+OR+innovation)&hl=en&gl=US&ceid=US:en",
"regions":["global"],
"cat":"launch"
},

{
"name":"Ingredient Innovation",
"url":"https://news.google.com/rss/search?q=ingredient+innovation+OR+flavour+innovation&hl=en&gl=US&ceid=US:en",
"regions":["global"],
"cat":"innovation"
},

{
"name":"FDA Beverage",
"url":"https://news.google.com/rss/search?q=FDA+beverage+regulation&hl=en&gl=US&ceid=US:en",
"regions":["usa"],
"cat":"regulation"
},

{
"name":"EU Regulation",
"url":"https://news.google.com/rss/search?q=EFSA+food+regulation+additives&hl=en&gl=DE&ceid=DE:en",
"regions":["germany","france","spain","italy","austria"],
"cat":"regulation"
},

]

# =============================================================
# ── SECTION 3: SALES NEWS KEYWORD FILTERS ───────────────────
# =============================================================

# Region-specific keyword boosts for relevance scoring
REGION_KEYWORDS = {
    "usa":     ["usa", "united states", "american", "fda", "us market", "north america"],
    "germany": ["germany", "german", "deutschland", "dach", "bundesrat", "lebensmittel"],
    "france":  ["france", "french", "paris", "francais", "egalim", "leclerc", "carrefour"],
    "spain":   ["spain", "spanish", "espana", "madrid", "barcelona", "mercadona", "horeca spain"],
    "italy":   ["italy", "italian", "italia", "milan", "rome", "aperitivo", "esselunga"],
    "austria": ["austria", "austrian", "wien", "vienna", "spar austria", "hofer", "alnatura"],
}

# Beverage relevance — article must contain at least one of these
BEVERAGE_KEYWORDS = [
    "beverage", "drink", "juice", "soft drink", "energy drink", "smoothie",
    "water", "tea", "coffee", "rtd", "ready to drink", "functional",
    "boisson", "bebida", "getraenk", "succo", "saft", "jus",
    "carbonat", "sparkling", "still water", "flavour", "flavor",
    "launch", "new product", "innovation", "market", "trend", "consumer",
    "sugar tax", "nutri-score", "packaging", "regulation", "labelling",
    "prix", "price", "pricing", "commodity", "ingredient cost",
    "ingredient", "ingredient",
"flavour",
"flavor",
"aroma",
"extract",
"colour",
"color",
"additive",
"stabilizer",
"emulsifier",
"natural extract",
"botanical",
"fortified",
"protein drink",

]

# Hard exclusions for sales news (less strict than fruit — just filter obvious junk)
SALES_EXCLUSIONS = [
    "cryptocurrency", "bitcoin", "stock market", "nasdaq", "nyse",
    "real estate", "mortgage", "auto loan", "car insurance",
    "celebrity", "gossip", "sports score", "football result",
    "recipe", "cooking tip", "how to make", "diy",
]

# Categories — used for filtering in the UI
SALES_CATEGORIES = {
    "launch":     ["launch", "new product", "new range", "introduces", "unveil", "debut", "release"],
    "trend":      ["trend", "consumer", "demand", "growth", "market", "insight", "report", "forecast"],
    "pricing":    ["price", "cost", "inflation", "commodity", "margin", "tariff", "import cost"],
    "regulation": ["regulation", "regulat", "law", "directive", "tax", "ban", "label", "nutri", "ppwr", "egalim"],
    "market":     ["market", "share", "volume", "revenue", "sales", "retail", "channel"],
}

# =============================================================
# SHARED UTILITIES
# =============================================================

def article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]

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

def fetch_rss(url: str, source_name: str) -> list[dict]:

    headers = {
        "User-Agent":
        "Mozilla/5.0 (MarketIntelBot)"
    }

    try:

        r = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

        r.raise_for_status()

        root = ET.fromstring(r.content)

    except Exception as e:

        print("FAIL", source_name, e)
        return []

    ns = {
        "atom": "http://www.w3.org/2005/Atom"
    }

    # RSS items
    items = root.findall(".//item")

    # ATOM fallback (BeverageDaily etc)
    if not items:

        items = root.findall(".//atom:entry", ns)

    print(source_name, "RAW:", len(items))

    cutoff = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)

    out = []

    for item in items:

        def get(tag):

            el = item.find(tag)

            return el.text.strip() if el is not None and el.text else ""

        title = get("title")

        link = get("link") or get("id")

        summary = (
            get("description")
            or get("summary")
            or get("{http://www.w3.org/2005/Atom}summary")
        )

        pub = parse_date(
            get("pubDate")
            or get("published")
            or get("updated")
        )

        if not title or not link:
            continue

        if pub < cutoff:
            continue

        out.append({

            "title": title,
            "url": link,
            "summary": summary[:400],
            "pub_dt": pub

        })

    print(source_name, "KEPT:", len(out))

    return out

# =============================================================
# ── RED FRUIT LOGIC (unchanged from original) ───────────────
# =============================================================

CROP_KEYWORDS = {
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
                      "erdbeere", "fraise", "truskawka",
                      "strawberries crop", "strawberries harvest", "strawberries market"],
    "blueberry":     ["blueberry crop", "blueberry harvest", "blueberry production",
                      "blueberry grower", "blueberry yield", "blueberry season",
                      "blueberry export", "blueberry import", "blueberry price",
                      "blueberry market", "blueberry supply", "blueberry plantation",
                      "frozen blueberry", "blueberry concentrate", "blueberry juice",
                      "heidelbeere", "myrtille", "borowka",
                      "blueberries crop", "blueberries harvest", "blueberries market"],
    "rhubarb":       ["rhubarb crop", "rhubarb harvest", "rhubarb production",
                      "rhubarb market", "rhubarb season", "rhubarb grower",
                      "rhabarber", "rhubarbe", "rabarbar"],
    "elderberry":    ["elderberry crop", "elderberry harvest", "elderberry production",
                      "elderberry market", "elderberry juice", "elderberry extract",
                      "elderberry concentrate", "elderberry supply", "sambucus nigra",
                      "holunder", "sureau", "czarny bez"],
}

EXCLUSION_KEYWORDS = [
    "raspberry pi", "raspberrypi", "raspberry pi 4", "raspberry pi 5",
    "raspberry pi zero", "raspberry pi pico",
    "nurse", "nursing", "hospital", "patient", "medical", "clinical", "therapy",
    "diet tip", "smoothie recipe", "cocktail recipe", "dessert recipe",
    "beauty tip", "skin care", "skincare", "weight loss", "superfood",
    "stock surge", "stock rally", "stock price", "share price", "market rally",
    "market cap", "ceo purchase", "banking & finance", "global banking",
    "investment", "investor", "hedge fund", "private equity", "ipo",
    "stock market", "nasdaq", "nyse", "cryptocurrency", "bitcoin", "blockchain",
    "quarterly earnings", "annual report", "revenue",
    "raspberry ketone", "strawberry blonde", "strawberry moon",
    "strawberry shortcake", "elderflower",
    "software", "firmware", "hardware", "app store", "android", "ios",
]

CONCENTRATE_KEYWORDS = [
    "fruit concentrate", "juice concentrate", "not from concentrate", "NFC juice",
    "NFC berry", "IQF fruit", "frozen fruit market", "frozen berry",
    "fruit processing", "fruit juice industry", "berry concentrate",
    "cherry concentrate", "cherry juice concentrate", "blackcurrant concentrate",
    "currant concentrate", "raspberry concentrate", "strawberry concentrate",
    "blueberry concentrate", "elderberry concentrate", "elderberry juice",
    "orange juice concentrate", "FCOJ", "apple juice concentrate",
    "fruit puree", "aseptic fruit", "drum concentrate", "Brix",
]

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

def detect_crops(text):
    t = text.lower()
    return [crop for crop, kws in CROP_KEYWORDS.items() if any(kw in t for kw in kws)]

def detect_concentrate(text):
    t = text.lower()
    return any(kw.lower() in t for kw in CONCENTRATE_KEYWORDS)

def is_excluded(text):
    t = text.lower()
    return any(kw.lower() in t for kw in EXCLUSION_KEYWORDS)

def is_fruit_relevant(text):
    if is_excluded(text):
        return False
    has_crop        = bool(detect_crops(text))
    has_context     = any(kw.lower() in text.lower() for kw in CONTEXT_KEYWORDS)
    has_concentrate = detect_concentrate(text)
    return (has_crop and has_context) or has_concentrate

def article_category(text, crops):
    if detect_concentrate(text) and not crops:
        return "concentrate & juice"
    if crops:
        return crops[0]
    return "general"

# =============================================================
# ── SALES NEWS LOGIC ─────────────────────────────────────────
# =============================================================

def is_sales_excluded(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in SALES_EXCLUSIONS)

def is_beverage_relevant(text: str):

    t=text.lower()

    KEYWORDS=[
    "beverage",
    "drink",
    "juice",
    "launch",
    "innovation",
    "ingredient",
    "flavour",
    "flavor",
    "functional",
    "market"
    ]

    return any(k in t for k in KEYWORDS)

def detect_sales_category(text: str, default_cat: str) -> str:
    t = text.lower()
    for cat, keywords in SALES_CATEGORIES.items():
        if any(kw in t for kw in keywords):
            return cat
    return default_cat

def assign_regions(text: str, source_regions: list) -> list:
    """
    Assign article to regions:
    - If source is global, check text for region keywords
    - If source is region-specific, always assign to those regions
    - If global and no region keywords found, assign to all regions
    """
    if source_regions != ["global"]:
        return source_regions

    t = text.lower()
    matched = []
    for region, keywords in REGION_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            matched.append(region)

    return matched if matched else ["global"]

def fetch_sales_feed(source: dict) -> list[dict]:
    raw_items = fetch_rss(source["url"], source["name"])
    articles = []
    filter_kws = [k.lower() for k in source.get("filter_keywords", [])]

    for item in raw_items:
        full_text = f"{item['title']} {item['summary']}"

        # Apply source-specific keyword filter if set
        if filter_kws:
            if not any(kw in full_text.lower() for kw in filter_kws):
                continue

        if is_sales_excluded(full_text):
            continue



        regions = assign_regions(full_text, source["regions"])
        cat     = detect_sales_category(full_text, source.get("cat", "market"))

        articles.append({
            "id":        article_id(item["url"]),
            "title":     item["title"],
            "summary":   item["summary"],
            "url":       item["url"],
            "source":    source["name"],
            "regions":   regions,
            "cat":       cat,
            "published": item["pub_dt"].isoformat(),
            "fetched":   datetime.now(timezone.utc).isoformat(),
        })

    return articles

# =============================================================
# STORE OPERATIONS
# =============================================================

def load_json(path: Path) -> list:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("articles", [])
    except Exception:
        return []

def remove_expired(articles: list) -> list:
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)
    kept, expired = [], 0
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
        print(f"  Removed {expired} expired articles.")
    return kept

def merge_articles(existing: list, new: list, max_count: int) -> tuple[list, int]:
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
    return existing[:max_count], added

def save_json(articles: list, path: Path, label: str):
    payload = {
        "last_updated":  datetime.now(timezone.utc).isoformat(),
        "article_count": len(articles),
        "articles":      articles,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  Saved {len(articles)} {label} articles -> {path.name}")

# =============================================================
# BRIEFING GENERATION (from fetched RSS articles — no API needed)
# =============================================================

# Topic keywords for detecting what each article is about
TOPIC_DETECTORS = {
    "energy drinks":    ["energy drink", "red bull", "monster", "celsius", "rockstar", "caffeine"],
    "functional":       ["functional", "probiotic", "prebiotic", "adaptogen", "nootropic", "gut health", "immunity"],
    "juice decline":    ["juice decline", "juice falling", "juice down", "juice drop", "nfc decline"],
    "juice growth":     ["premium juice", "nfc juice", "cold pressed", "juice growth", "juice up"],
    "sugar tax":        ["sugar tax", "sugar levy", "soda tax", "hfss", "sweetened beverage tax"],
    "packaging":        ["ppwr", "packaging", "deposit", "pfand", "recycl", "pcr content", "pet bottle"],
    "organic":          ["organic", "bio ", "biologique", "ökologisch", "bio-"],
    "launches":         ["launch", "debut", "unveil", "introduces", "new product", "new range", "rolls out"],
    "pricing":          ["price", "inflation", "cost", "commodity", "margin", "tariff"],
    "regulation":       ["regulation", "fda", "efsa", "nutri-score", "labelling", "ban", "directive"],
    "rtd":              ["rtd", "ready to drink", "ready-to-drink", "canned cocktail"],
    "no-low alcohol":   ["non-alcoholic", "no-alcohol", "alcohol-free", "low-alcohol", "0%", "zero alcohol"],
    "sparkling water":  ["sparkling water", "mineral water", "seltzer", "carbonated water"],
    "m&a":              ["acquisition", "acquire", "merger", "m&a", "takeover", "buyout"],
}

def detect_topics(text: str) -> list[str]:
    """Detect which topics an article covers."""
    t = text.lower()
    return [topic for topic, keywords in TOPIC_DETECTORS.items()
            if any(kw in t for kw in keywords)]


def generate_briefing(sales_articles: list, fruit_articles: list):
    """
    Generate morning briefing + region signals from fetched RSS articles.
    No API needed — pure keyword/frequency analysis of recent articles.
    """
    today = datetime.now(timezone.utc).strftime("%A, %d %B %Y")
    now_iso = datetime.now(timezone.utc).isoformat()

    # Combine all articles for analysis
    all_articles = sales_articles + fruit_articles

    if not all_articles:
        print("  ⚠ No articles available — cannot generate briefing.")
        return

    # ── Analyze global topics ──
    topic_counts = {}
    region_topics = {r: {} for r in REGION_KEYWORDS}
    region_articles = {r: [] for r in REGION_KEYWORDS}
    global_articles = []

    for a in all_articles:
        text = f"{a.get('title', '')} {a.get('summary', '')}"
        topics = detect_topics(text)
        regions = a.get("regions", ["global"])

        for topic in topics:
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

        if isinstance(regions, list):
            for r in regions:
                if r in region_topics:
                    region_articles[r].append(a)
                    for topic in topics:
                        region_topics[r][topic] = region_topics[r].get(topic, 0) + 1
                elif r == "global":
                    global_articles.append(a)
        else:
            global_articles.append(a)

    # ── Build briefing from top topics ──
    sorted_topics = sorted(topic_counts.items(), key=lambda x: -x[1])
    top_topics = [t[0] for t in sorted_topics[:5]]

    # Count articles by category
    cat_counts = {}
    for a in sales_articles:
        cat = a.get("cat", "market")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    # Get most recent headlines for context
    recent = sorted(all_articles, key=lambda a: a.get("published", ""), reverse=True)[:5]
    recent_titles = [a.get("title", "")[:80] for a in recent]

    # Build briefing sentences
    sentences = []

    # Sentence 1: Overall market activity
    total = len(sales_articles)
    launch_count = cat_counts.get("launch", 0)
    reg_count = cat_counts.get("regulation", 0)
    trend_count = cat_counts.get("trend", 0)
    if total > 0:
        parts = []
        if launch_count > 0:
            parts.append(f"{launch_count} product launches")
        if reg_count > 0:
            parts.append(f"{reg_count} regulatory updates")
        if trend_count > 0:
            parts.append(f"{trend_count} market trends")
        activity = ", ".join(parts[:3]) if parts else f"{total} articles"
        sentences.append(
            f"Beverage market intelligence tracked {activity} across {len([r for r in region_articles if region_articles[r]])} active regions in the past 2 weeks."
        )

    # Sentence 2: Dominant themes
    if len(top_topics) >= 2:
        theme_map = {
            "energy drinks": "energy drinks continue to dominate headlines",
            "functional": "functional and wellness beverages gaining momentum",
            "sugar tax": "sugar tax developments reshaping pricing strategies",
            "packaging": "EU packaging regulation (PPWR) driving compliance activity",
            "organic": "organic and clean-label demand accelerating",
            "launches": "new product launches intensifying across markets",
            "pricing": "pricing pressures and commodity costs in focus",
            "regulation": "regulatory changes impacting product strategies",
            "no-low alcohol": "no/low alcohol category expanding rapidly",
            "juice decline": "traditional juice volumes under pressure",
            "juice growth": "premium NFC and cold-pressed juice segments growing",
            "rtd": "RTD formats gaining share across categories",
            "sparkling water": "sparkling and functional water demand rising",
            "m&a": "M&A activity consolidating the beverage landscape",
        }
        themes = [theme_map.get(t, t.replace("_", " ") + " trending") for t in top_topics[:3]]
        sentences.append(f"Key themes: {'; '.join(themes)}.")

    # Sentence 3: Most notable recent headline
    if recent_titles:
        sentences.append(f"Latest: {recent_titles[0]}.")

    briefing_text = " ".join(sentences) if sentences else "Market intelligence is being collected. Check back after the next scheduled update."

    # ── Build region signals ──
    def make_signal(region_id: str) -> str:
        """Generate a short signal for a region based on its articles."""
        rt = region_topics.get(region_id, {})
        ra = region_articles.get(region_id, [])

        if not rt and not ra:
            # No region-specific articles — use a sensible default based on known market characteristics
            defaults = {
                "usa":     "Energy drinks & functional RTD surging.",
                "germany": "Functional water growing. Juice declining.",
                "france":  "Premium juice & organic sparkling growing.",
                "spain":   "Energy drinks and Horeca recovery strong.",
                "italy":   "Aperitivo culture driving premium mixers.",
                "austria": "Red Bull home market. Organic above EU avg.",
            }
            return defaults.get(region_id, "Monitoring — limited recent data.")

        # Pick top topic for this region
        top = sorted(rt.items(), key=lambda x: -x[1])
        top_topic = top[0][0] if top else "market"

        signal_map = {
            "energy drinks":    "Energy drink segment leading growth.",
            "functional":       "Functional beverage demand accelerating.",
            "sugar tax":        "Sugar tax impacting pricing strategy.",
            "packaging":        "Packaging regulation compliance in focus.",
            "organic":          "Organic & clean-label demand rising.",
            "launches":         f"{len(ra)} new launches tracked recently.",
            "pricing":          "Commodity costs pressuring margins.",
            "regulation":       "Regulatory changes reshaping market.",
            "no-low alcohol":   "No/low alcohol segment expanding fast.",
            "juice decline":    "Traditional juice volumes declining.",
            "juice growth":     "Premium juice segment outperforming.",
            "rtd":              "RTD formats gaining shelf space.",
            "sparkling water":  "Sparkling water demand accelerating.",
            "m&a":              "M&A activity consolidating players.",
        }

        signal = signal_map.get(top_topic, f"{top_topic.title()} trending.")

        # Add article count context if we have enough
        if len(ra) >= 3:
            signal = f"{signal} ({len(ra)} articles)"

        return signal

    signals = {region: make_signal(region) for region in REGION_KEYWORDS}

    # ── Save briefing.json ──
    briefing_data = {
        "generated_at": now_iso,
        "generated_date": today,
        "briefing": briefing_text,
        "signals": signals,
        "meta": {
            "total_articles_analyzed": len(all_articles),
            "sales_articles": len(sales_articles),
            "fruit_articles": len(fruit_articles),
            "top_topics": dict(sorted_topics[:8]),
            "method": "rss-analysis",
        }
    }
    with open(BRIEFING_FILE, "w", encoding="utf-8") as f:
        json.dump(briefing_data, f, ensure_ascii=False, indent=2)

    print(f"  ✓ Briefing generated from {len(all_articles)} articles → {BRIEFING_FILE.name}")
    print(f"    Briefing: {briefing_text[:150]}...")
    for region, signal in signals.items():
        print(f"    {region}: {signal}")


# =============================================================
# MAIN
# =============================================================

def run():
    print("=" * 60)
    print(f"  News Fetcher — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # ── Part 1: Red Fruit News ───────────────────────────────
    print("\n  [1/2] RED FRUIT NEWS")
    existing_fruit = remove_expired(load_json(NEWS_FILE))
    all_fruit_new  = []

    for source in RSS_SOURCES:
        print(f"    {source['name']}...")
        raw = fetch_rss(source["url"], source["name"])
        age_cutoff = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)
        for item in raw:
            full_text = f"{item['title']} {item['summary']}"
            if not is_fruit_relevant(full_text):
                continue
            crops    = detect_crops(full_text)
            is_conc  = detect_concentrate(full_text)
            category = article_category(full_text, crops)
            if len(item["summary"]) > 320:
                item["summary"] = item["summary"][:317].rsplit(" ", 1)[0] + "..."
            all_fruit_new.append({
                "id":           article_id(item["url"]),
                "title":        item["title"],
                "summary":      item["summary"],
                "url":          item["url"],
                "source":       source["name"],
                "region":       source.get("region", "Global"),
                "crops":        crops,
                "crop":         category,
                "is_concentrate": is_conc,
                "published":    item["pub_dt"].isoformat(),
                "fetched":      datetime.now(timezone.utc).isoformat(),
            })

    merged_fruit, added_fruit = merge_articles(existing_fruit, all_fruit_new, MAX_ARTICLES)
    save_json(merged_fruit, NEWS_FILE, "red fruit")
    print(f"  Added {added_fruit} new red fruit articles. Total: {len(merged_fruit)}")

    # ── Part 2: Sales Intelligence News ─────────────────────
    print("\n  [2/2] SALES INTELLIGENCE NEWS")
    existing_sales = remove_expired(load_json(SALES_NEWS_FILE))
    all_sales_new  = []

    for source in SALES_RSS_SOURCES:
        regions_str = ", ".join(source["regions"])
        print(f"    {source['name']} [{regions_str}]...")
        fetched = fetch_sales_feed(source)
        print(f"      -> {len(fetched)} relevant articles")
        all_sales_new.extend(fetched)

    merged_sales, added_sales = merge_articles(existing_sales, all_sales_new, MAX_SALES_ARTICLES)
    save_json(merged_sales, SALES_NEWS_FILE, "sales intelligence")
    print(f"  Added {added_sales} new sales articles. Total: {len(merged_sales)}")

    # ── Part 3: Morning Briefing + Region Signals (from RSS data) ──
    print("\n  [3/3] MORNING BRIEFING (from RSS analysis — no API needed)")
    generate_briefing(merged_sales, merged_fruit)

    # ── Summary ──────────────────────────────────────────────
    print("\n  SUMMARY:")
    print(f"    Red fruit:         {len(merged_fruit)} articles")
    print(f"    Sales intelligence:{len(merged_sales)} articles")

    from collections import Counter
    region_counts = Counter()
    for a in merged_sales:
        for r in a.get("regions", ["global"]):
            region_counts[r] += 1
    print("    Sales by region:")
    for region, count in sorted(region_counts.items(), key=lambda x: -x[1]):
        print(f"      {region}: {count}")

    print("\n  Done.\n")

if __name__ == "__main__":
    run()
