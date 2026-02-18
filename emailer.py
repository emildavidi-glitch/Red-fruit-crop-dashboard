# =============================================================
# emailer.py — Send formatted HTML alert email via Outlook/O365
# =============================================================

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from config import EMAIL_CONFIG, CROP_RISKS


# ── Risk colours ──────────────────────────────────────────────
RISK_COLORS = {
    "critical": {"bg": "#7B1A12", "text": "#FFFFFF", "label": "🚨 CRITICAL"},
    "risk":     {"bg": "#E74C3C", "text": "#FFFFFF", "label": "⚠️  RISK"},
    "watch":    {"bg": "#F39C12", "text": "#FFFFFF", "label": "👁  WATCH"},
    "safe":     {"bg": "#27AE60", "text": "#FFFFFF", "label": "✅ SAFE"},
}


def _crop_tags_html(crops: list[str], affected: list[str]) -> str:
    tags = []
    for crop in crops:
        icon = CROP_RISKS.get(crop, {}).get("icon", "")
        color = "#C0392B" if crop in affected else "#888"
        bg    = "#FFF0EE" if crop in affected else "#F5F5F5"
        tags.append(
            f'<span style="background:{bg};color:{color};border:1px solid '
            f'{"#FFCCCC" if crop in affected else "#DDD"};'
            f'border-radius:10px;padding:2px 10px;font-size:12px;'
            f'font-family:monospace;margin:2px;display:inline-block;">'
            f'{icon} {crop}</span>'
        )
    return " ".join(tags)


def _alert_rows_html(alerts: list[dict]) -> str:
    if not alerts:
        return ""
    rows = []
    for a in alerts:
        c = RISK_COLORS[a["level"]]
        rows.append(
            f'<tr>'
            f'<td style="padding:8px 12px;background:{c["bg"]};color:{c["text"]};'
            f'font-size:12px;font-weight:bold;border-radius:4px;white-space:nowrap;">'
            f'{c["label"]}</td>'
            f'<td style="padding:8px 14px;font-size:13px;color:#333;">{a["message"]}</td>'
            f'</tr>'
        )
    return (
        '<table style="width:100%;border-collapse:collapse;margin-top:10px;">'
        + "".join(rows)
        + "</table>"
    )


def _region_card_html(result: dict) -> str:
    region  = result["region"]
    weather = result["weather"]
    risk    = result["risk_level"]
    rc      = RISK_COLORS[risk]

    # 7-day forecast mini strip
    forecast_cells = ""
    for i, (mn, mx, dt) in enumerate(zip(
        weather["daily_min"], weather["daily_max"], weather["daily_dates"]
    )):
        is_frost = mn <= 0
        day_name = datetime.strptime(dt, "%Y-%m-%d").strftime("%a")
        cell_bg  = "#FFF0EE" if is_frost else "#F8F8F8"
        temp_col = "#C0392B" if is_frost else "#333"
        forecast_cells += (
            f'<td style="text-align:center;padding:6px 8px;background:{cell_bg};'
            f'border-radius:5px;min-width:44px;">'
            f'<div style="font-size:10px;color:#999;font-family:monospace;">{day_name}</div>'
            f'<div style="font-size:12px;color:{temp_col};font-family:monospace;'
            f'font-weight:{"bold" if is_frost else "normal"};">'
            f'{mx}°/{mn}°</div>'
            f'</td>'
        )

    alerts_html = _alert_rows_html(result["alerts"])
    crops_html  = _crop_tags_html(region["crops"], result["affected_crops"])

    return f"""
    <div style="background:#fff;border:1px solid #E8DDD5;border-radius:10px;
                margin-bottom:18px;overflow:hidden;
                box-shadow:0 2px 10px rgba(0,0,0,0.06);">
      <!-- Card header -->
      <div style="display:flex;justify-content:space-between;align-items:center;
                  padding:14px 18px;border-bottom:1px solid #EEE;">
        <div>
          <span style="font-family:'Georgia',serif;font-size:16px;font-weight:bold;
                       color:#1C1C1C;">{region['flag']} {region['name']}</span>
          <span style="font-size:11px;color:#999;font-family:monospace;
                       margin-left:10px;text-transform:uppercase;
                       letter-spacing:0.08em;">{region['country']}</span>
        </div>
        <span style="background:{rc['bg']};color:{rc['text']};padding:4px 14px;
                     border-radius:12px;font-size:11px;font-family:monospace;
                     font-weight:bold;letter-spacing:0.08em;">
          {rc['label']}
        </span>
      </div>
      <!-- Weather stats -->
      <div style="padding:14px 18px;">
        <table style="width:100%;border-collapse:collapse;margin-bottom:12px;">
          <tr>
            <td style="text-align:center;background:#FAF6F0;border-radius:6px;
                       padding:10px;width:25%;">
              <div style="font-size:18px;font-family:monospace;font-weight:bold;">
                {weather['current_temp']}°C</div>
              <div style="font-size:10px;color:#999;text-transform:uppercase;">Now</div>
            </td>
            <td style="width:8px;"></td>
            <td style="text-align:center;background:#FAF6F0;border-radius:6px;
                       padding:10px;width:25%;">
              <div style="font-size:18px;font-family:monospace;font-weight:bold;
                          color:{'#C0392B' if weather['min_7d'] <= 0 else '#1C1C1C'};">
                {weather['min_7d']}°C</div>
              <div style="font-size:10px;color:#999;text-transform:uppercase;">Min 7d</div>
            </td>
            <td style="width:8px;"></td>
            <td style="text-align:center;background:#FAF6F0;border-radius:6px;
                       padding:10px;width:25%;">
              <div style="font-size:18px;font-family:monospace;font-weight:bold;">
                {weather['wind_kmh']}</div>
              <div style="font-size:10px;color:#999;text-transform:uppercase;">km/h Wind</div>
            </td>
            <td style="width:8px;"></td>
            <td style="text-align:center;background:#FAF6F0;border-radius:6px;
                       padding:10px;width:25%;">
              <div style="font-size:18px;font-family:monospace;font-weight:bold;">
                {weather['precip_mm']}</div>
              <div style="font-size:10px;color:#999;text-transform:uppercase;">mm Rain</div>
            </td>
          </tr>
        </table>
        <!-- Alerts -->
        {alerts_html}
        <!-- Crops -->
        <div style="margin-top:12px;">{crops_html}</div>
        <!-- 7-day forecast -->
        <table style="width:100%;border-collapse:separate;border-spacing:4px;margin-top:12px;
                      border-top:1px solid #EEE;padding-top:12px;">
          <tr>{forecast_cells}</tr>
        </table>
      </div>
    </div>"""


def build_email_html(results: list[dict]) -> str:
    """Build the full HTML email body."""
    now = datetime.now().strftime("%A, %d %B %Y — %H:%M")

    # Count by level
    counts = {"critical": 0, "risk": 0, "watch": 0, "safe": 0}
    for r in results:
        counts[r["risk_level"]] += 1

    # Only include non-safe regions in detail cards
    actionable = [r for r in results if r["risk_level"] != "safe"]
    safe_list  = [r for r in results if r["risk_level"] == "safe"]

    cards_html = "".join(_region_card_html(r) for r in actionable)

    safe_names = ", ".join(
        f"{r['region']['flag']} {r['region']['name']} ({r['region']['country']})"
        for r in safe_list
    ) or "None"

    summary_row = "".join(
        f'<td style="text-align:center;padding:16px 20px;background:{RISK_COLORS[lvl]["bg"]};'
        f'color:#fff;border-radius:8px;">'
        f'<div style="font-size:28px;font-weight:bold;">{counts[lvl]}</div>'
        f'<div style="font-size:11px;margin-top:4px;opacity:0.85;">'
        f'{RISK_COLORS[lvl]["label"]}</div></td>'
        f'<td style="width:10px;"></td>'
        for lvl in ["critical", "risk", "watch", "safe"]
    )

    no_alert_msg = ""
    if not actionable:
        no_alert_msg = """
        <div style="background:#F0FFF4;border:1px solid #A8E6C0;border-radius:8px;
                    padding:20px;text-align:center;color:#27AE60;font-size:15px;
                    margin-bottom:24px;">
          ✅ All regions are currently safe — no crop risk detected today.
        </div>"""

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#FAF6F0;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#FAF6F0;">
<tr><td align="center" style="padding:30px 20px;">
<table width="680" cellpadding="0" cellspacing="0"
       style="max-width:680px;width:100%;">

  <!-- HEADER -->
  <tr><td style="background:#1C1C1C;border-bottom:3px solid #C0392B;
                  border-radius:10px 10px 0 0;padding:22px 28px;">
    <table width="100%"><tr>
      <td>
        <div style="font-family:'Georgia',serif;font-size:20px;color:#FAF6F0;
                    font-weight:bold;">🍒 Red Fruit Crop Monitor</div>
        <div style="font-family:monospace;font-size:11px;color:#888;
                    margin-top:4px;letter-spacing:0.1em;text-transform:uppercase;">
          Daily Weather Risk Report</div>
      </td>
      <td align="right">
        <div style="font-family:monospace;font-size:11px;color:#888;">{now}</div>
      </td>
    </tr></table>
  </td></tr>

  <!-- SUMMARY ROW -->
  <tr><td style="background:#fff;padding:20px 28px;border-bottom:1px solid #EEE;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>{summary_row}</tr>
    </table>
  </td></tr>

  <!-- MAIN CONTENT -->
  <tr><td style="padding:24px 28px;">
    {no_alert_msg}
    {cards_html}

    <!-- SAFE REGIONS (collapsed) -->
    <div style="background:#F0FFF4;border:1px solid #A8E6C0;border-radius:8px;
                padding:14px 18px;margin-top:8px;">
      <div style="font-size:12px;color:#27AE60;font-weight:bold;margin-bottom:6px;">
        ✅ Safe Regions (no action needed)</div>
      <div style="font-size:12px;color:#555;">{safe_names}</div>
    </div>
  </td></tr>

  <!-- FOOTER -->
  <tr><td style="background:#1C1C1C;border-radius:0 0 10px 10px;
                  padding:16px 28px;text-align:center;">
    <div style="font-family:monospace;font-size:10px;color:#666;letter-spacing:0.05em;">
      Weather data: Open-Meteo API · Red Fruit Crop Monitor · Germany 🇩🇪<br>
      This report is generated automatically each morning.
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def send_email(results: list[dict]) -> bool:
    """Send the daily alert email via Outlook SMTP."""
    cfg = EMAIL_CONFIG

    # Determine subject based on highest risk
    levels = [r["risk_level"] for r in results]
    if "critical" in levels:
        subject = f"{cfg['subject_prefix']} — 🚨 CRITICAL FROST ALERT"
    elif "risk" in levels:
        subject = f"{cfg['subject_prefix']} — ⚠️ Crop Risk Detected"
    elif "watch" in levels:
        subject = f"{cfg['subject_prefix']} — 👁 Watch: Approaching Critical Window"
    else:
        subject = f"{cfg['subject_prefix']} — ✅ All Regions Safe"

    html_body = build_email_html(results)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["sender_email"]
    msg["To"]      = ", ".join(cfg["recipients"])
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"]) as server:
            server.ehlo()
            server.starttls()
            server.login(cfg["sender_email"], cfg["sender_password"])
            server.sendmail(cfg["sender_email"], cfg["recipients"], msg.as_string())
        print(f"  ✅ Email sent to: {', '.join(cfg['recipients'])}")
        return True
    except Exception as e:
        print(f"  ❌ Failed to send email: {e}")
        return False
