# =============================================================
# monitor.py — Red Fruit Crop Monitor · Main Script
#
# Run manually:     python monitor.py
# Schedule daily:   see scheduler.py
# =============================================================

import sys
from datetime import datetime

from config   import REGIONS
from weather  import fetch_weather
from risk     import assess_region, sort_by_risk
from emailer  import send_email


def run():
    print("=" * 60)
    print(f"  🍒 Red Fruit Crop Monitor — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    results = []

    for region in REGIONS:
        print(f"\n  📍 Fetching: {region['flag']} {region['name']}, {region['country']}...")
        weather = fetch_weather(region)

        if weather is None:
            print(f"     ⚠️  Skipped — no weather data.")
            continue

        result = assess_region(region, weather)
        results.append(result)

        level = result["risk_level"].upper()
        print(f"     Temp now: {weather['current_temp']}°C  |  "
              f"Min 7d: {weather['min_7d']}°C  |  Risk: {level}")

        for alert in result["alerts"]:
            print(f"     → [{alert['level'].upper()}] {alert['message'][:80]}...")

    # Sort by risk severity
    results = sort_by_risk(results)

    # Summary
    print("\n" + "=" * 60)
    counts = {"critical": 0, "risk": 0, "watch": 0, "safe": 0}
    for r in results:
        counts[r["risk_level"]] += 1
    print(f"  Summary: 🚨 {counts['critical']} Critical  "
          f"⚠️  {counts['risk']} Risk  "
          f"👁  {counts['watch']} Watch  "
          f"✅ {counts['safe']} Safe")
    print("=" * 60)

    # Send email
    print("\n  📧 Sending daily report email...")
    send_email(results)

    print("\n  Done.\n")
    return results


if __name__ == "__main__":
    run()
