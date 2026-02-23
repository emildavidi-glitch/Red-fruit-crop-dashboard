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
# ── SECTION 2: SALES INTELLIGENCE RSS SOURCES ───────────────
# =============================================================
# Each source has:
#   regions: list of region IDs this source applies to
#             (or ["global"] for all regions)
#   cat:     category tag — launch / trend / pricing / regulation / market

SALES_RSS_SOURCES = [

    # ── GLOBAL industry trade press (all regions) ────────────
    {
        "name": "FoodNavigator",
        "url": "https://www.foodnavigator.com/rss/editorial.rss",
        "regions": ["global"],
        "cat": "trend",
    },
    {
        "name": "BeverageDaily",
        "url": "https://www.beveragedaily.com/rss/editorial.rss",
        "regions": ["global"],
        "cat": "launch",
    },
    {
        "name": "Just-Drinks",
        "url": "https://www.just-drinks.com/feed/",
        "regions": ["global"],
        "cat": "market",
    },
    {
        "name": "Drinks Business",
        "url": "https://www.thedrinksbusiness.com/feed/",
        "regions": ["global"],
        "cat": "market",
    },
    {
        "name": "Food & Drink Technology",
        "url": "https://www.foodanddrink-technology.com/feed/",
        "regions": ["global"],
        "cat": "launch",
    },

    # ── USA ─────────────────────────────────────────────────
    {
        "name": "FoodNavigator-USA",
        "url": "https://www.foodnavigator-usa.com/rss/editorial.rss",
        "regions": ["usa"],
        "cat": "trend",
    },
    {
        "name": "Beverage Industry Magazine",
        "url": "https://www.bevindustry.com/rss/all",
        "regions": ["usa"],
        "cat": "market",
    },
    {
        "name": "GNews: US Beverage Launches",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(launch+OR+%22new+product%22+OR+%22new+range%22)+USA&hl=en&gl=US&ceid=US:en",
        "regions": ["usa"],
        "cat": "launch",
    },
    {
        "name": "GNews: US Beverage Trends",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(trend+OR+consumer+OR+demand+OR+market)+%22United+States%22&hl=en&gl=US&ceid=US:en",
        "regions": ["usa"],
        "cat": "trend",
    },
    {
        "name": "GNews: FDA Beverage Regulation",
        "url": "https://news.google.com/rss/search?q=FDA+(beverage+OR+drink+OR+juice)+(regulation+OR+labelling+OR+rule+OR+ban)&hl=en&gl=US&ceid=US:en",
        "regions": ["usa"],
        "cat": "regulation",
    },

    # ── GERMANY / DACH ───────────────────────────────────────
    {
        "name": "FoodNavigator (Germany)",
        "url": "https://www.foodnavigator.com/rss/editorial.rss",
        "regions": ["germany", "austria"],
        "cat": "trend",
        "filter_keywords": ["germany", "german", "deutschland", "dach", "austria", "swiss"],
    },
    {
        "name": "GNews: Germany Beverage Market",
        "url": "https://news.google.com/rss/search?q=(Getraenk+OR+Getraenke+OR+Saft+OR+beverage+OR+drink)+(Markt+OR+market+OR+launch+OR+trend)+Deutschland&hl=de&gl=DE&ceid=DE:de",
        "regions": ["germany"],
        "cat": "market",
    },
    {
        "name": "GNews: Germany Juice/Beverage Launches",
        "url": "https://news.google.com/rss/search?q=(juice+OR+beverage+OR+energy+drink+OR+functional+drink)+(launch+OR+new+OR+trend)+(Germany+OR+DACH)&hl=en&gl=DE&ceid=DE:en",
        "regions": ["germany"],
        "cat": "launch",
    },
    {
        "name": "GNews: EU Beverage Regulation",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(regulation+OR+Nutri-Score+OR+PPWR+OR+packaging+OR+sugar+tax)+%22European+Union%22+OR+EU&hl=en&gl=DE&ceid=DE:en",
        "regions": ["germany", "france", "spain", "italy", "austria"],
        "cat": "regulation",
    },

    # ── FRANCE ───────────────────────────────────────────────
    {
        "name": "GNews: France Beverage Market",
        "url": "https://news.google.com/rss/search?q=(boisson+OR+jus+OR+beverage+OR+drink)+(marche+OR+lancement+OR+tendance+OR+market+OR+launch+OR+trend)+France&hl=fr&gl=FR&ceid=FR:fr",
        "regions": ["france"],
        "cat": "market",
    },
    {
        "name": "GNews: France Food Launches",
        "url": "https://news.google.com/rss/search?q=(juice+OR+beverage+OR+boisson)+(launch+OR+nouveau+OR+new+OR+trend)+France&hl=en&gl=FR&ceid=FR:en",
        "regions": ["france"],
        "cat": "launch",
    },
    {
        "name": "GNews: France EGAlim / Regulation",
        "url": "https://news.google.com/rss/search?q=(EGAlim+OR+Nutri-Score+OR+Eco-Score+OR+%22sugar+tax%22+OR+taxe+sucre)+(boisson+OR+jus+OR+beverage)&hl=fr&gl=FR&ceid=FR:fr",
        "regions": ["france"],
        "cat": "regulation",
    },

    # ── SPAIN ────────────────────────────────────────────────
    {
        "name": "GNews: Spain Beverage Market",
        "url": "https://news.google.com/rss/search?q=(bebida+OR+zumo+OR+juice+OR+beverage)+(mercado+OR+market+OR+lanzamiento+OR+launch+OR+tendencia)+Spain+OR+Espana&hl=es&gl=ES&ceid=ES:es",
        "regions": ["spain"],
        "cat": "market",
    },
    {
        "name": "GNews: Spain Horeca Drinks",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(Horeca+OR+bar+OR+restaurant+OR+hotel)+Spain&hl=en&gl=ES&ceid=ES:en",
        "regions": ["spain"],
        "cat": "trend",
    },
    {
        "name": "GNews: Spain Food Regulation",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+bebida+OR+zumo)+(impuesto+OR+tax+OR+regulation+OR+etiquetado+OR+labelling)+Spain&hl=en&gl=ES&ceid=ES:en",
        "regions": ["spain"],
        "cat": "regulation",
    },

    # ── ITALY ────────────────────────────────────────────────
    {
        "name": "GNews: Italy Beverage Market",
        "url": "https://news.google.com/rss/search?q=(bevanda+OR+succo+OR+beverage+OR+drink)+(mercato+OR+market+OR+lancio+OR+launch+OR+tendenza)+Italy+OR+Italia&hl=it&gl=IT&ceid=IT:it",
        "regions": ["italy"],
        "cat": "market",
    },
    {
        "name": "GNews: Italy Aperitivo Drinks",
        "url": "https://news.google.com/rss/search?q=(aperitivo+OR+aperitif+OR+spritz+OR+mixer)+(beverage+OR+drink+OR+juice+OR+succo)+Italy&hl=en&gl=IT&ceid=IT:en",
        "regions": ["italy"],
        "cat": "trend",
    },
    {
        "name": "GNews: Italy Food Launch",
        "url": "https://news.google.com/rss/search?q=(juice+OR+beverage+OR+functional+drink)+(launch+OR+nuovo+OR+new+OR+organic+OR+bio)+Italy&hl=en&gl=IT&ceid=IT:en",
        "regions": ["italy"],
        "cat": "launch",
    },

    # ── AUSTRIA ─────────────────────────────────────────────
    {
        "name": "GNews: Austria Beverage",
        "url": "https://news.google.com/rss/search?q=(Getraenk+OR+Saft+OR+beverage+OR+drink+OR+juice)+(Markt+OR+market+OR+launch+OR+Bio+OR+organic)+Austria+OR+Oesterreich&hl=de&gl=AT&ceid=AT:de",
        "regions": ["austria"],
        "cat": "market",
    },
    {
        "name": "GNews: Red Bull / Austria Innovation",
        "url": "https://news.google.com/rss/search?q=(%22Red+Bull%22+OR+%22Rauch%22+OR+%22Voelkel%22)+(launch+OR+new+OR+organic+OR+innovation)&hl=en&gl=AT&ceid=AT:en",
        "regions": ["austria"],
        "cat": "launch",
    },

    # ── GLOBAL beverage trends & pricing ────────────────────
    {
        "name": "GNews: Global Beverage Trends",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(trend+OR+%22consumer+trend%22+OR+%22market+trend%22+OR+innovation)+(2025+OR+2026)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "trend",
    },
    {
        "name": "GNews: Functional Beverages",
        "url": "https://news.google.com/rss/search?q=%22functional+beverage%22+OR+%22functional+drink%22+OR+%22adaptogen+drink%22+(launch+OR+market+OR+trend)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "trend",
    },
    {
        "name": "GNews: Energy Drinks Market",
        "url": "https://news.google.com/rss/search?q=%22energy+drink%22+(market+OR+launch+OR+regulation+OR+trend)+(Europe+OR+USA+OR+global)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "market",
    },
    {
        "name": "GNews: Sugar Tax Beverage",
        "url": "https://news.google.com/rss/search?q=%22sugar+tax%22+OR+%22sugar+levy%22+(beverage+OR+drink+OR+juice)+(Europe+OR+EU+OR+UK+OR+Germany+OR+France+OR+Spain)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "regulation",
    },
    {
        "name": "GNews: Beverage Pricing / Commodities",
        "url": "https://news.google.com/rss/search?q=(beverage+OR+drink+OR+juice)+(price+OR+pricing+OR+cost+OR+inflation+OR+commodity)+(2025+OR+2026)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "pricing",
    },
    {
        "name": "GNews: RTD / Ready-to-Drink",
        "url": "https://news.google.com/rss/search?q=%22ready+to+drink%22+OR+%22RTD%22+(juice+OR+beverage+OR+coffee+OR+tea)+(launch+OR+market+OR+trend)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "launch",
    },
    {
        "name": "GNews: No-Low Alcohol Beverages",
        "url": "https://news.google.com/rss/search?q=(%22no-alcohol%22+OR+%22non-alcoholic%22+OR+%22low-alcohol%22+OR+%22alcohol-free%22)+(beverage+OR+drink)+(market+OR+launch+OR+trend)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "trend",
    },
    {
        "name": "GNews: EU Food Law / Packaging",
        "url": "https://news.google.com/rss/search?q=(PPWR+OR+%22EU+packaging%22+OR+%22Nutri-Score%22+OR+%22Farm+to+Fork%22)+(beverage+OR+drink+OR+food)&hl=en&gl=US&ceid=US:en",
        "regions": ["global"],
        "cat": "regulation",
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
    """Fetch and parse an RSS feed, return raw items."""
    headers = {
        "User-Agent": "BeverageSalesIntelligence/1.0 (market research)",
        "Accept": "application/rss+xml, application/xml, text/xml",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        print(f"    WARNING {source_name}: {e}")
        return []

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)
    age_cutoff = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)
    raw_items = []

    for item in items:
        def get(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else ""

        title   = get("title")
        url_    = get("link") or get("guid")
        summary = clean_html(get("description") or get("summary") or "")
        pub_dt  = parse_date(get("pubDate") or get("published") or get("updated"))

        if not title or not url_:
            continue
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        if pub_dt < age_cutoff:
            continue
        if len(summary) > 400:
            summary = summary[:397].rsplit(" ", 1)[0] + "..."

        raw_items.append({
            "title": title, "url": url_, "summary": summary, "pub_dt": pub_dt,
        })

    return raw_items

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

def is_beverage_relevant(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in BEVERAGE_KEYWORDS)

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

        if not is_beverage_relevant(full_text):
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
