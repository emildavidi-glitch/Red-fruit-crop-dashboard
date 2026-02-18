# =============================================================
# config.py — Red Fruit Crop Monitor Configuration
# Edit this file to change regions, crops, thresholds, and email
# =============================================================

# -------------------------------------------------------------
# EMAIL SETTINGS (Outlook / Office 365)
# -------------------------------------------------------------
EMAIL_CONFIG = {
    "smtp_server": "smtp.office365.com",
    "smtp_port": 587,
    "sender_email": "your_email@yourdomain.com",      # ← Change this
    "sender_password": "your_app_password_here",       # ← Change this (use App Password)
    "recipients": [
        "you@yourdomain.com",                          # ← Add all recipients here
        # "colleague@yourdomain.com",
    ],
    "subject_prefix": "🍒 Crop Monitor Alert"
}

# -------------------------------------------------------------
# WATCH WINDOW — days before critical period to start watching
# -------------------------------------------------------------
WATCH_DAYS_BEFORE_CRITICAL = 14   # "Watch" triggers 14 days before flowering starts

# -------------------------------------------------------------
# REGIONS
# Add or remove regions here. Find lat/lon at latlong.net
# -------------------------------------------------------------
REGIONS = [
    # POLAND
    dict(id="masovia",        name="Masovia",           country="Poland",         flag="🇵🇱", lat=51.9,  lon=21.0,  crops=["sour cherry", "black currant", "raspberry"]),
    dict(id="lubelskie",      name="Lubelskie",         country="Poland",         flag="🇵🇱", lat=51.2,  lon=22.5,  crops=["sour cherry", "black currant", "strawberry"]),
    dict(id="podkarpacie",    name="Podkarpacie",       country="Poland",         flag="🇵🇱", lat=50.0,  lon=22.0,  crops=["sour cherry", "raspberry", "strawberry"]),
    # UKRAINE
    dict(id="ukraine-west",   name="Western Ukraine",   country="Ukraine",        flag="🇺🇦", lat=49.5,  lon=25.5,  crops=["sour cherry", "black currant", "raspberry"]),
    # SERBIA
    dict(id="serbia",         name="Šumadija",          country="Serbia",         flag="🇷🇸", lat=44.0,  lon=20.9,  crops=["sour cherry", "raspberry", "strawberry"]),
    # MOLDOVA
    dict(id="moldova",        name="Central Moldova",   country="Moldova",        flag="🇲🇩", lat=47.0,  lon=28.8,  crops=["sour cherry", "black currant"]),
    # CZECHIA
    dict(id="moravia",        name="South Moravia",     country="Czechia",        flag="🇨🇿", lat=48.9,  lon=16.9,  crops=["sour cherry", "black currant", "raspberry"]),
    # HUNGARY
    dict(id="hungary",        name="Bács-Kiskun",       country="Hungary",        flag="🇭🇺", lat=46.6,  lon=19.4,  crops=["sour cherry", "raspberry", "strawberry"]),
    # ROMANIA
    dict(id="romania",        name="Olt Valley",        country="Romania",        flag="🇷🇴", lat=44.5,  lon=24.5,  crops=["sour cherry", "black currant", "strawberry"]),
    # GERMANY
    dict(id="germany",        name="Sachsen-Anhalt",    country="Germany",        flag="🇩🇪", lat=51.8,  lon=11.7,  crops=["black currant", "raspberry", "strawberry"]),
    # TURKEY
    dict(id="turkey-aegean",  name="Aegean Region",     country="Turkey",         flag="🇹🇷", lat=38.4,  lon=27.1,  crops=["sour cherry", "black currant", "raspberry", "strawberry", "blueberry"]),
    dict(id="turkey-marmara", name="Marmara Region",    country="Turkey",         flag="🇹🇷", lat=40.5,  lon=29.5,  crops=["sour cherry", "raspberry", "strawberry", "blueberry"]),
    # CANADA
    dict(id="canada-bc",      name="British Columbia",  country="Canada",         flag="🇨🇦", lat=49.1,  lon=-122.3, crops=["blueberry", "raspberry", "strawberry"]),
    dict(id="canada-ontario", name="Ontario",           country="Canada",         flag="🇨🇦", lat=44.3,  lon=-79.5,  crops=["blueberry", "raspberry", "strawberry", "black currant"]),
    # CHILE (Southern Hemisphere — reversed seasons)
    dict(id="chile-ohiggins", name="O'Higgins Region",  country="Chile",          flag="🇨🇱", lat=-34.5, lon=-71.0,  crops=["blueberry", "raspberry", "strawberry"]),
    dict(id="chile-biobio",   name="Biobío Region",     country="Chile",          flag="🇨🇱", lat=-37.5, lon=-72.5,  crops=["blueberry", "raspberry"]),
]

# -------------------------------------------------------------
# CROP RISK DEFINITIONS
# criticalMonths: months where frost causes direct crop damage
# criticalMonthsSouth: for Southern Hemisphere regions (Chile)
# frostThreshold: temp (°C) below which crop damage occurs
# watchThreshold: temp (°C) that triggers Watch alert
# -------------------------------------------------------------
CROP_RISKS = {
    "sour cherry": {
        "criticalMonths":      [4, 5],
        "criticalMonthsSouth": [10, 11, 12],
        "frostThreshold": -1.0,
        "watchThreshold":  2.0,
        "icon": "🍒",
    },
    "black currant": {
        "criticalMonths":      [4, 5],
        "criticalMonthsSouth": [10, 11, 12],
        "frostThreshold": -1.0,
        "watchThreshold":  2.0,
        "icon": "🫐",
    },
    "strawberry": {
        "criticalMonths":      [4, 5, 6],
        "criticalMonthsSouth": [10, 11, 12],
        "frostThreshold": -0.5,
        "watchThreshold":  3.0,
        "icon": "🍓",
    },
    "raspberry": {
        "criticalMonths":      [4, 5, 6],
        "criticalMonthsSouth": [10, 11, 12],
        "frostThreshold": -1.0,
        "watchThreshold":  3.0,
        "icon": "🫐",
    },
    "blueberry": {
        "criticalMonths":      [4, 5, 6],
        "criticalMonthsSouth": [10, 11, 12],
        "frostThreshold": -1.0,
        "watchThreshold":  2.0,
        "icon": "🫐",
    },
}

# -------------------------------------------------------------
# RISK LEVELS (do not change order — used for sorting)
# -------------------------------------------------------------
RISK_ORDER = ["critical", "risk", "watch", "safe"]
