# 🍒 Red Fruit Crop Monitor — Python Setup Guide

## Files in this project

| File | Purpose |
|------|---------|
| `config.py` | All settings: regions, crops, email credentials |
| `weather.py` | Fetches live data from Open-Meteo API |
| `risk.py` | Calculates frost risk per crop and region |
| `emailer.py` | Builds and sends the HTML alert email |
| `monitor.py` | Main script — run this manually or on schedule |
| `scheduler.py` | Keeps monitor running daily at 07:00 |
| `requirements.txt` | Python packages needed |

---

## Step 1 — Install Python

Download from https://www.python.org (version 3.10 or newer).
During install, tick **"Add Python to PATH"**.

---

## Step 2 — Install dependencies

Open a terminal / command prompt in the project folder and run:

```bash
pip install -r requirements.txt
```

---

## Step 3 — Configure your email (Outlook / Office 365)

Open `config.py` and fill in:

```python
EMAIL_CONFIG = {
    "sender_email":    "your_email@yourdomain.com",
    "sender_password": "your_app_password_here",
    "recipients":      ["you@yourdomain.com"],
    ...
}
```

### Getting an App Password for Office 365

If your organisation uses standard Outlook/O365:
1. Go to https://account.microsoft.com/security
2. Select **Advanced security options**
3. Under **App passwords**, create a new one
4. Paste it as `sender_password` in config.py

> ⚠️ If your company uses **Modern Authentication / MFA only**, ask your IT department to allow SMTP AUTH for your account, or use a shared mailbox with SMTP enabled.

---

## Step 4 — Test it manually

```bash
python monitor.py
```

You should see weather data printed for all 16 regions and receive an email.

---

## Step 5 — Run automatically every morning

**Option A — Keep it running on your PC:**
```bash
python scheduler.py
```
Runs at 07:00 every day as long as your PC is on.

**Option B — Windows Task Scheduler (recommended for reliability):**
1. Open Task Scheduler → Create Basic Task
2. Trigger: Daily at 07:00
3. Program: `python.exe`  |  Arguments: `monitor.py`  |  Start in: your project folder
4. Runs silently in background every morning.

**Option C — PythonAnywhere.com (free cloud, always-on):**
1. Sign up free at https://www.pythonanywhere.com
2. Upload all `.py` files
3. Go to **Tasks** → add daily task at 07:00
4. Command: `python /home/yourusername/monitor.py`

---

## How risk levels work

| Level | When it triggers |
|-------|-----------------|
| 🚨 **Critical** | Frost ≤ threshold°C forecast **during** flowering months |
| ⚠️ **Risk** | Near-frost temps forecast **during** flowering months |
| 👁 **Watch** | Within **14 days** of flowering window + cold temps in forecast |
| ✅ **Safe** | Outside watch window — no email detail, just listed as safe |

> Watch only triggers close to the critical window — not year-round noise.

---

## Adding or changing regions

Open `config.py` and edit the `REGIONS` list. For each region you need:
- `name`, `country`, `flag`
- `lat` / `lon` — find at https://www.latlong.net
- `crops` — list from: `sour cherry`, `black currant`, `strawberry`, `raspberry`, `blueberry`

Southern Hemisphere regions (negative latitude) automatically use reversed season logic.

---

## Questions?

Contact your setup team or re-open Claude and paste any error messages.
