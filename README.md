# 🍒 Red Fruit Crop Monitor

A live weather risk intelligence dashboard for red fruit crop monitoring across key European growing regions.

**Built for:** Processing factory managing directors who need to monitor weather risk for sour cherry, black currant, strawberry, and raspberry crops.

---

## 🚀 How to Use

1. Download `index.html`
2. Open it in any modern web browser — **no installation required**
3. It automatically fetches live weather data for all 10 regions
4. Refreshes every 30 minutes automatically, or press the ↻ Refresh button

> **Free to use** — powered by [Open-Meteo](https://open-meteo.com/), a free weather API that requires no API key.

---

## 🌍 Monitored Regions

| Region | Country | Key Crops |
|--------|---------|-----------|
| Masovia | Poland 🇵🇱 | Sour cherry, Black currant, Raspberry |
| Lubelskie | Poland 🇵🇱 | Sour cherry, Black currant, Strawberry |
| Podkarpacie | Poland 🇵🇱 | Sour cherry, Raspberry, Strawberry |
| Western Ukraine | Ukraine 🇺🇦 | Sour cherry, Black currant, Raspberry |
| Šumadija | Serbia 🇷🇸 | Sour cherry, Raspberry, Strawberry |
| Central Moldova | Moldova 🇲🇩 | Sour cherry, Black currant |
| South Moravia | Czechia 🇨🇿 | Sour cherry, Black currant, Raspberry |
| Bács-Kiskun | Hungary 🇭🇺 | Sour cherry, Raspberry, Strawberry |
| Olt Valley | Romania 🇷🇴 | Sour cherry, Black currant, Strawberry |
| Sachsen-Anhalt | Germany 🇩🇪 | Black currant, Raspberry, Strawberry |
| Aegean Region | Turkey 🇹🇷 | Sour cherry, Black currant, Raspberry, Strawberry, Blueberry |
| Marmara Region | Turkey 🇹🇷 | Sour cherry, Raspberry, Strawberry, Blueberry |
| British Columbia | Canada 🇨🇦 | Blueberry, Raspberry, Strawberry |
| Ontario | Canada 🇨🇦 | Blueberry, Raspberry, Strawberry, Black currant |
| O'Higgins Region | Chile 🇨🇱 | Blueberry, Raspberry, Strawberry |
| Biobío Region | Chile 🇨🇱 | Blueberry, Raspberry |

---

## 🌡 Risk Level Logic

| Level | Meaning |
|-------|---------|
| ✅ **Safe** | No frost risk forecast, outside critical window |
| 👁 **Watch** | Temperatures approaching threshold, or cold outside critical window |
| ⚠️ **Risk** | Near-frost temperatures during flowering season |
| 🚨 **Critical** | Frost below 0°C forecast during critical flowering months (April–May) |

---

## 🍒 Crop-Specific Frost Thresholds

| Crop | Critical Months | Frost Alert Below | Watch Below |
|------|---------------|-------------------|-------------|
| Sour Cherry | April, May | -1°C | +2°C |
| Black Currant | April, May | -1°C | +2°C |
| Strawberry | April, May, June | -0.5°C | +3°C |
| Raspberry | April, May, June | -1°C | +3°C |
| Blueberry (N. Hemisphere) | April, May, June | -1°C | +2°C |
| Blueberry (S. Hemisphere — Chile) | October, November, December | -1°C | +2°C |

---

## ✏️ How to Customize

Open `index.html` in a text editor and find the `REGIONS` array (line ~180). You can:

- **Add a new region:** Add a new object with name, country, latitude, longitude, and crops
- **Remove a region:** Delete its entry
- **Change crops per region:** Edit the `crops` array

Find latitude/longitude for any location at [latlong.net](https://www.latlong.net/).

---

## 📅 Season Phases

The dashboard automatically adjusts its guidance based on the time of year:

- **March–mid April:** Pre-flowering / Bud Break
- **mid April–late May:** 🚨 CRITICAL Flowering Season
- **May–June:** Fruit Set
- **July–September:** Harvest Season
- **Oct–Feb:** Dormancy

---

## 🛠 Tech Stack

- Pure HTML + CSS + JavaScript (no framework needed)
- [Open-Meteo API](https://open-meteo.com/) — free, no API key required
- [Google Fonts](https://fonts.google.com/) — Playfair Display + IBM Plex

---

*Developed for red fruit processing factory operations in Germany.*
