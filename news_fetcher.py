# =============================================================
# news_fetcher.py — Red Fruit Crop Monitor · Automatic News Fetcher
#
# Fetches RSS feeds from FreshPlaza, Eurofresh, FreshFruitPortal
# and other industry sources every 24 hours.
# Keeps articles for 7 days, saves to news.json for the dashboard.
#
# Run manually:  python news_fetcher.py
# Scheduled:     see scheduler.py (runs daily at 07:00)
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
    print("  [INFO] BeautifulSoup not installed — summaries will use RSS description only.")
    print("         For better summaries run: pip install beautifulsoup4 lxml")

# =============================================================
# CONFIGURATION
# =============================================================

NEWS_FILE = Path(__file__).parent / "news.json"
ARTICLE_TTL_DAYS = 7       # Articles older than this are removed
MAX_ARTICLES = 40          # Maximum total articles to keep
REQUEST_TIMEOUT = 15       # Seconds before giving up on a feed

# =============================================================
# RSS SOURCES
# All free, public RSS feeds from industry-standard websites
# =============================================================

RSS_SOURCES = [
    # ── FreshPlaza (world's leading fresh produce news) ──────
    {
        "name": "FreshPlaza",
        "url":  "https://www.freshplaza.com/rss/",
        "region": "Global"
    },
    {
        "name": "FreshPlaza Europe",
        "url":  "https://www.freshplaza.com/europe/rss/",
        "region": "Europe"
    },
    # ── Eurofresh Distribution ───────────────────────────────
    {
        "name": "Eurofresh Distribution",
        "url":  "https://www.eurofresh-distribution.com/feed/",
        "region": "Europe"
    },
    # ── Fresh Fruit Portal ───────────────────────────────────
    {
        "name": "FreshFruitPortal",
        "url":  "https://www.freshfruitportal.com/feed/",
        "region": "Global"
    },
    # ── Produce Business ─────────────────────────────────────
    {
        "name": "Produce Business",
        "url":  "https://www.producebusiness.com/feed/",
        "region": "Global"
    },
    # ── Hortidaily ───────────────────────────────────────────
    {
        "name": "Hortidaily",
        "url":  "https://www.hortidaily.com/rss/",
        "region": "Global"
    },
    # ── The Packer ───────────────────────────────────────────
    {
        "name": "The Packer",
        "url":  "https://www.thepacker.com/rss.xml",
        "region": "North America"
    },
    # ── Google News RSS — targeted crop searches ─────────────
    # These search Google News in real time for your exact crops
    {
        "name": "Google News: Sour Cherry",
        "url":  "https://news.google.com/rss/search?q=sour+cherry+crop+harvest&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
    {
        "name": "Google News: Black Currant",
        "url":  "https://news.google.com/rss/search?q=black+currant+blackcurrant+crop&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
    {
        "name": "Google News: Raspberry",
        "url":  "https://news.google.com/rss/search?q=raspberry+crop+harvest+Serbia+Poland&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
    {
        "name": "Google News: Strawberry",
        "url":  "https://news.google.com/rss/search?q=strawberry+crop+Huelva+Egypt+Turkey&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
    {
        "name": "Google News: Blueberry",
        "url":  "https://news.google.com/rss/search?q=blueberry+crop+Chile+Canada+export&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
    {
        "name": "Google News: Elderberry",
        "url":  "https://news.google.com/rss/search?q=elderberry+market+harvest+Austria+Hungary&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
    {
        "name": "Google News: Rhubarb",
        "url":  "https://news.google.com/rss/search?q=rhubarb+harvest+Germany+market&hl=en&gl=US&ceid=US:en",
        "region": "Global"
    },
]

# =============================================================
# KEYWORDS — article must match at least ONE crop keyword
# AND at least ONE context keyword to be included
# =============================================================

CROP_KEYWORDS = {
    "sour cherry":   ["sour cherry", "sauerkirsche", "vişne", "cerise acide", "tart cherry"],
    "black currant": ["black currant", "blackcurrant", "schwarze johannisbeere", "cassis"],
    "red currant":   ["red currant", "redcurrant", "rote johannisbeere"],
    "raspberry":     ["raspberry", "raspberries", "himbeere", "framboise", "ahududu"],
    "strawberry":    ["strawberry", "strawberries", "erdbeere", "fraise", "çilek"],
    "blueberry":     ["blueberry", "blueberries", "heidelbeere", "myrtille"],
    "rhubarb":       ["rhubarb", "rhabarber", "rhubarbe"],
    "elderberry":    ["elderberry", "elderberries", "holunder", "sureau", "mürver"],
}

CONTEXT_KEYWORDS = [
    "harvest", "crop", "production", "season", "export", "import",
    "price", "market", "supply", "demand", "frost", "yield", "grower",
    "processor", "concentrate", "frozen", "fresh", "berry", "fruit",
    "poland", "turkey", "serbia", "spain", "huelva", "egypt", "chile",
    "canada", "ukraine", "germany", "hungary", "austria",
    "ernte", "saison", "markt", "récolte", "saison",
]

# =============================================================
# CROP DETECTION
# =============================================================

def detect_crops(text: str) -> list[str]:
    """Return list of crops mentioned in text."""
    text_lower = text.lower()
    found = []
    for crop, keywords in CROP_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(crop)
    return found


def is_relevant(text: str) -> bool:
    """Check if article has both a crop keyword and a context keyword."""
    text_lower = text.lower()
    has_crop    = any(kw in text_lower for kws in CROP_KEYWORDS.values() for kw in kws)
    has_context = any(kw in text_lower for kw in CONTEXT_KEYWORDS)
    return has_crop and has_context


def article_id(url: str) -> str:
    """Stable unique ID from URL."""
    return hashlib.md5(url.encode()).hexdigest()[:12]


# =============================================================
# RSS PARSING
# =============================================================

def parse_date(date_str: str) -> datetime:
    """Parse RSS date string to UTC datetime."""
    if not date_str:
        return datetime.now(timezone.utc)

    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return datetime.now(timezone.utc)


def clean_html(raw: str) -> str:
    """Strip HTML tags from summary text."""
    if not raw:
        return ""
    if BS4_AVAILABLE:
        return BeautifulSoup(raw, "lxml").get_text(separator=" ", strip=True)
    # Fallback: basic tag stripping
    import re
    return re.sub(r"<[^>]+>", " ", raw).strip()


def fetch_feed(source: dict) -> list[dict]:
    """Fetch and parse one RSS feed. Returns list of article dicts."""
    articles = []
    headers = {
        "User-Agent": "RedFruitCropMonitor/1.0 (crop risk intelligence; contact@example.com)",
        "Accept": "application/rss+xml, application/xml, text/xml",
    }

    try:
        resp = requests.get(source["url"], headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        print(f"    ⚠️  Could not fetch {source['name']}: {e}")
        return []

    # Handle both RSS and Atom namespaces
    ns = {"atom": "http://www.w3.org/2005/Atom",
          "media": "http://search.yahoo.com/mrss/"}

    items = root.findall(".//item") or root.findall(".//atom:entry", ns)

    for item in items:
        def get(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else ""

        title   = get("title")
        url     = get("link") or get("guid")
        summary = clean_html(get("description") or get("summary") or "")
        pub_str = get("pubDate") or get("published") or get("updated")
        pub_dt  = parse_date(pub_str)

        full_text = f"{title} {summary}"

        if not title or not url:
            continue
        if not is_relevant(full_text):
            continue

        crops = detect_crops(full_text)
        if not crops:
            continue

        # Truncate summary to ~300 chars cleanly
        if len(summary) > 300:
            summary = summary[:297].rsplit(" ", 1)[0] + "…"

        articles.append({
            "id":        article_id(url),
            "title":     title,
            "summary":   summary,
            "url":       url,
            "source":    source["name"],
            "region":    source["region"],
            "crops":     crops,
            "crop":      crops[0],          # primary crop (for filter)
            "published": pub_dt.isoformat(),
            "fetched":   datetime.now(timezone.utc).isoformat(),
        })

    return articles


# =============================================================
# NEWS STORE — load, merge, expire, save
# =============================================================

def load_existing() -> list[dict]:
    if not NEWS_FILE.exists():
        return []
    try:
        with open(NEWS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("articles", [])
    except Exception:
        return []


def remove_expired(articles: list[dict]) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARTICLE_TTL_DAYS)
    kept = []
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
            kept.append(a)  # keep if date unreadable
    if expired:
        print(f"  🗑  Removed {expired} expired article(s) older than {ARTICLE_TTL_DAYS} days.")
    return kept


def merge(existing: list[dict], new: list[dict]) -> list[dict]:
    existing_ids = {a["id"] for a in existing}
    added = 0
    for article in new:
        if article["id"] not in existing_ids:
            existing.append(article)
            existing_ids.add(article["id"])
            added += 1

    # Sort newest first
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
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "article_count": len(articles),
        "articles": articles,
    }
    with open(NEWS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  💾 Saved {len(articles)} articles to {NEWS_FILE}")


# =============================================================
# MAIN
# =============================================================

def run():
    print("=" * 60)
    print(f"  📰 News Fetcher — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # 1. Load existing articles
    existing = load_existing()
    print(f"  📂 Loaded {len(existing)} existing articles.")

    # 2. Remove expired
    existing = remove_expired(existing)

    # 3. Fetch all RSS feeds
    all_new = []
    for source in RSS_SOURCES:
        print(f"\n  🌐 Fetching: {source['name']}...")
        fetched = fetch_feed(source)
        print(f"     → {len(fetched)} relevant article(s) found.")
        all_new.extend(fetched)

    # 4. Merge and save
    print(f"\n  🔀 Merging {len(all_new)} fetched with {len(existing)} existing...")
    merged = merge(existing, all_new)
    save(merged)

    # 5. Summary by crop
    print("\n  📊 Articles by crop:")
    from collections import Counter
    crop_counts = Counter(crop for a in merged for crop in a["crops"])
    for crop, count in sorted(crop_counts.items(), key=lambda x: -x[1]):
        icon = {"sour cherry":"🍒","black currant":"🫐","red currant":"🔴",
                "raspberry":"🫐","strawberry":"🍓","blueberry":"🫐",
                "rhubarb":"🌿","elderberry":"🍇"}.get(crop,"🌱")
        print(f"     {icon} {crop}: {count} article(s)")

    print("\n  Done.\n")
    return merged


if __name__ == "__main__":
    run()
