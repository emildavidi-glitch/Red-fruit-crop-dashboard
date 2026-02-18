# =============================================================
# risk.py — Smart crop risk assessment engine
#
# Risk logic:
#   CRITICAL — frost below frostThreshold during critical months
#   RISK     — near-frost temps (below watchThreshold) during critical months
#   WATCH    — within WATCH_DAYS_BEFORE_CRITICAL days of critical window
#              AND forecast shows low temps worth monitoring
#   SAFE     — no concern (outside window, no frost risk)
# =============================================================

from datetime import date, timedelta
from config import CROP_RISKS, WATCH_DAYS_BEFORE_CRITICAL


def _critical_months(crop_def: dict, is_southern: bool) -> list[int]:
    """Return the critical flowering months for this crop, adjusted for hemisphere."""
    if is_southern and "criticalMonthsSouth" in crop_def:
        return crop_def["criticalMonthsSouth"]
    return crop_def["criticalMonths"]


def _days_until_critical_window(crop_def: dict, is_southern: bool) -> int:
    """
    Returns how many days until the next critical window starts.
    Returns 0 if we are currently inside the window.
    Returns a large number if the window is far away.
    """
    today = date.today()
    critical_months = _critical_months(crop_def, is_southern)
    current_month = today.month

    # Already inside critical window
    if current_month in critical_months:
        return 0

    # Search forward up to 365 days to find when next critical window starts
    for delta in range(1, 366):
        future = today + timedelta(days=delta)
        if future.month in critical_months:
            return delta

    return 999  # Should never happen


def assess_region(region: dict, weather: dict) -> dict:
    """
    Assess frost risk for all crops in a region.
    Returns a risk summary dict.
    """
    is_southern = region["lat"] < 0
    current_month = date.today().month

    min_temps   = weather["daily_min"]
    highest_risk = "safe"
    alerts       = []
    affected_crops = []

    for crop in region["crops"]:
        crop_def = CROP_RISKS.get(crop)
        if not crop_def:
            continue

        crit_months   = _critical_months(crop_def, is_southern)
        frost_thresh  = crop_def["frostThreshold"]
        watch_thresh  = crop_def["watchThreshold"]
        days_to_crit  = _days_until_critical_window(crop_def, is_southern)
        in_window     = current_month in crit_months
        near_window   = 0 < days_to_crit <= WATCH_DAYS_BEFORE_CRITICAL

        frost_days = [t for t in min_temps if t <= frost_thresh]
        cold_days  = [t for t in min_temps if frost_thresh < t <= watch_thresh]

        if in_window and frost_days:
            # CRITICAL: frost during flowering
            highest_risk = "critical"
            affected_crops.append(crop)
            alerts.append({
                "level": "critical",
                "crop": crop,
                "message": (
                    f"FROST ALERT — {len(frost_days)} day(s) with min temp "
                    f"≤ {frost_thresh}°C forecast (lowest: {min(frost_days)}°C). "
                    f"{crop.title()} in full flowering — crop damage highly likely."
                )
            })

        elif in_window and cold_days:
            # RISK: near-frost during flowering
            if highest_risk not in ("critical",):
                highest_risk = "risk"
            affected_crops.append(crop)
            alerts.append({
                "level": "risk",
                "crop": crop,
                "message": (
                    f"Near-frost temperatures forecast — {len(cold_days)} day(s) "
                    f"below {watch_thresh}°C (lowest: {min(cold_days)}°C). "
                    f"{crop.title()} flowering at risk."
                )
            })

        elif near_window and (frost_days or cold_days):
            # WATCH: approaching critical window with concerning temps
            if highest_risk not in ("critical", "risk"):
                highest_risk = "watch"
            alerts.append({
                "level": "watch",
                "crop": crop,
                "message": (
                    f"Critical flowering window starts in ~{days_to_crit} day(s). "
                    f"Cold temperatures already in forecast (min: {min(min_temps)}°C). "
                    f"Monitor {crop} closely."
                )
            })

        # else: SAFE — outside window, no imminent concern

    return {
        "region":         region,
        "weather":        weather,
        "risk_level":     highest_risk,
        "alerts":         alerts,
        "affected_crops": list(set(affected_crops)),
    }


def sort_by_risk(results: list[dict]) -> list[dict]:
    """Sort regions from most to least critical."""
    order = {"critical": 0, "risk": 1, "watch": 2, "safe": 3}
    return sorted(results, key=lambda r: order.get(r["risk_level"], 9))
