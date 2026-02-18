# =============================================================
# scheduler.py — Run monitor.py every morning at 07:00
#
# Option A: Run this script on your PC/Mac (keeps running)
# Option B: Use Windows Task Scheduler or Mac launchd instead
# Option C: Deploy to PythonAnywhere.com (free cloud hosting)
# =============================================================

import schedule
import time
from monitor import run

# ── Schedule: every day at 07:00 local time ──────────────────
RUN_TIME = "07:00"

def job():
    print(f"\n⏰ Scheduled run triggered at {RUN_TIME}")
    run()

schedule.every().day.at(RUN_TIME).do(job)

print(f"🍒 Red Fruit Crop Monitor — Scheduler started")
print(f"   Will run every day at {RUN_TIME} local time.")
print(f"   Press Ctrl+C to stop.\n")

# Run once immediately on startup so you see it working
print("▶ Running immediately on startup...")
run()

# Then keep running on schedule
while True:
    schedule.run_pending()
    time.sleep(60)


# =============================================================
# ALTERNATIVE: Windows Task Scheduler (no Python script needed)
# =============================================================
# 1. Open Task Scheduler → Create Basic Task
# 2. Trigger: Daily at 07:00
# 3. Action: Start a program
#    Program: C:\Python312\python.exe   (your Python path)
#    Arguments: monitor.py
#    Start in: C:\path\to\your\project\
# Done — runs silently every morning even if script is closed.


# =============================================================
# ALTERNATIVE: PythonAnywhere.com (free cloud, always-on)
# =============================================================
# 1. Sign up at pythonanywhere.com (free tier works)
# 2. Upload all 5 .py files via their file manager
# 3. Go to Tasks → Add a scheduled task
# 4. Command: python /home/yourusername/monitor.py
# 5. Set time to 07:00 UTC (adjust for your timezone)
# That's it — runs every day from the cloud, no PC needed.
