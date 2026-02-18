# =============================================================
# scheduler.py — Red Fruit Crop Monitor · Unified Scheduler
#
# Runs two jobs daily:
#   07:00 — Weather check + email alert (monitor.py)
#   07:15 — News fetch from RSS feeds (news_fetcher.py)
# =============================================================

import schedule
import time
from datetime import datetime

from monitor      import run as run_monitor
from news_fetcher import run as run_news

WEATHER_TIME = "07:00"
NEWS_TIME    = "07:15"


def weather_job():
    print(f"\n⏰ [{datetime.now().strftime('%H:%M')}] Running weather monitor & email...")
    try:
        run_monitor()
    except Exception as e:
        print(f"  ❌ Weather job failed: {e}")


def news_job():
    print(f"\n⏰ [{datetime.now().strftime('%H:%M')}] Running news fetcher...")
    try:
        run_news()
    except Exception as e:
        print(f"  ❌ News job failed: {e}")


schedule.every().day.at(WEATHER_TIME).do(weather_job)
schedule.every().day.at(NEWS_TIME).do(news_job)

print("🍒 Red Fruit Crop Monitor — Scheduler started")
print(f"   Weather + Email: daily at {WEATHER_TIME}")
print(f"   News Fetch:      daily at {NEWS_TIME}")
print(f"   Press Ctrl+C to stop.\n")

print("▶ Running both jobs now on startup...\n")
weather_job()
news_job()

while True:
    schedule.run_pending()
    time.sleep(60)
