from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

DATA_DIR = Path("/app/data")
CENSUS_CACHE_DIR = DATA_DIR / "census_cache"
CENSUS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

CENSUS_STATE_FIPS = os.getenv("CENSUS_STATE_FIPS", "16").strip() or "16"  # Idaho = 16
CENSUS_ACS_YEAR = os.getenv("CENSUS_ACS_YEAR", "2023").strip() or "2023"  # stable default
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "").strip()  # optional
CENSUS_CACHE_TTL_SECONDS = int(os.getenv("CENSUS_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))  # 7 days

BASE_URL = "https://api.census.gov/data/{year}/acs/acs5/profile"

# Variables (Profiles)
# NOTE: Some years expose the median HH income as DP03_0062E and/or DP03_0062PE.
# We request both and use whichever parses cleanly.
VARS = [
    "NAME",
    "DP05_0001E",     # Total population (common across ACS profile)
    "DP03_0062E",     # Median household income (dollars) (may exist)
    "DP03_0062PE",    # Sometimes used for median HH income in profile metadata
    "DP03_0128PE",    # Percent below poverty level (all people)
    "DP05_0024PE",    # Percent age 65+ (total pop)
    "DP02_0068PE",    # Percent BA+ (age 25+)
    "DP04_0046PE",    # Owner-occupied (%)
    "DP04_0047PE",    # Renter-occupied (%)
]


def _cache_path(key: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in key)
    return CENSUS_CACHE_DIR / f"{safe}.json"


def _read_cache(key: str) -> Optional[Dict[str, Any]]:
    p = _cache_path(key)
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        if time.time() - float(d.get("_cached_at", 0)) > CENSUS_CACHE_TTL_SECONDS:
            return None
        return d.get("payload")
    except Exception:
        return None


def _write_cache(key: str, payload: Dict[str, Any]) -> None:
    p = _cache_path(key)
    try:
        p.write_text(json.dumps({"_cached_at": time.time(), "payload": payload}, indent=2), encoding="utf-8")
    except Exception:
        pass


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() == "null":
            return None
        return float(s)
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() == "null":
            return None
        return int(float(s))
    except Exception:
        return None


def _fetch(params: Dict[str, str]) -> Dict[str, Any]:
    url = BASE_URL.format(year=CENSUS_ACS_YEAR)
    q = {"get": ",".join(VARS)}
    q.update(params)
    if CENSUS_API_KEY:
        q["key"] = CENSUS_API_KEY

    r = requests.get(url, params=q, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) < 2:
        raise RuntimeError("Unexpected Census API response shape")

    headers = data[0]
    values = data[1]
    out = {headers[i]: values[i] for i in range(min(len(headers), len(values)))}
    return out


def get_state_snapshot() -> Optional[Dict[str, Any]]:
    cache_key = f"acs{CENSUS_ACS_YEAR}_state_{CENSUS_STATE_FIPS}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    try:
        row = _fetch({"for": f"state:{CENSUS_STATE_FIPS}"})
        snap = _normalize_snapshot(row)
        _write_cache(cache_key, snap)
        return snap
    except Exception:
        return None


def get_sld_snapshot(chamber: str, district_num: str) -> Optional[Dict[str, Any]]:
    chamber_l = (chamber or "").strip().lower()
    d = str(district_num or "").strip()
    if not d:
        return None

    # Districts in Census API are typically 3-digit strings
    try:
        d3 = str(int(d)).zfill(3)
    except Exception:
        d3 = d.zfill(3)

    if chamber_l == "senate":
        geo = "state legislative district (upper chamber)"
    elif chamber_l == "house":
        geo = "state legislative district (lower chamber)"
    else:
        return None

    cache_key = f"acs{CENSUS_ACS_YEAR}_sld_{geo.replace(' ', '_')}_{CENSUS_STATE_FIPS}_{d3}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    try:
        row = _fetch({"for": f"{geo}:{d3}", "in": f"state:{CENSUS_STATE_FIPS}"})
        snap = _normalize_snapshot(row)
        _write_cache(cache_key, snap)
        return snap
    except Exception:
        return None


def _normalize_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    # Median HH income: prefer DP03_0062E, otherwise DP03_0062PE if numeric
    inc = _to_int(row.get("DP03_0062E"))
    if inc is None:
        inc = _to_int(row.get("DP03_0062PE"))

    return {
        "name": row.get("NAME"),
        "population": _to_int(row.get("DP05_0001E")),
        "median_household_income": inc,
        "poverty_rate_pct": _to_float(row.get("DP03_0128PE")),
        "age_65_plus_pct": _to_float(row.get("DP05_0024PE")),
        "ba_plus_pct": _to_float(row.get("DP02_0068PE")),
        "owner_occupied_pct": _to_float(row.get("DP04_0046PE")),
        "renter_occupied_pct": _to_float(row.get("DP04_0047PE")),
        "acs_year": CENSUS_ACS_YEAR,
    }


def format_snapshot_section(state: Optional[Dict[str, Any]], district: Optional[Dict[str, Any]], chamber: str, dist_num: str) -> str:
    if not state and not district:
        return ""

    def fmt_int(x: Optional[int]) -> str:
        return "—" if x is None else f"{x:,}"

    def fmt_money(x: Optional[int]) -> str:
        return "—" if x is None else f"${x:,}"

    def fmt_pct(x: Optional[float]) -> str:
        return "—" if x is None else f"{x:.1f}%"

    def block(title: str, s: Dict[str, Any]) -> str:
        return (
            f"{title}\n"
            f"- Population: {fmt_int(s.get('population'))}\n"
            f"- Median household income: {fmt_money(s.get('median_household_income'))}\n"
            f"- Poverty rate: {fmt_pct(s.get('poverty_rate_pct'))}\n"
            f"- Age 65+: {fmt_pct(s.get('age_65_plus_pct'))}\n"
            f"- BA+ (age 25+): {fmt_pct(s.get('ba_plus_pct'))}\n"
            f"- Housing tenure: Owner {fmt_pct(s.get('owner_occupied_pct'))} / Renter {fmt_pct(s.get('renter_occupied_pct'))}\n"
        )

    # Comparison highlights (simple, readable)
    highlights = []
    if state and district:
        def diff_pp(a: Optional[float], b: Optional[float]) -> Optional[float]:
            if a is None or b is None:
                return None
            return a - b

        def add_pp(label: str, dpp: Optional[float], threshold: float = 2.0):
            if dpp is None:
                return
            if abs(dpp) >= threshold:
                direction = "higher" if dpp > 0 else "lower"
                highlights.append(f"- District is {abs(dpp):.1f} percentage points {direction} than Idaho for {label}.")

        add_pp("poverty", diff_pp(district.get("poverty_rate_pct"), state.get("poverty_rate_pct")))
        add_pp("age 65+", diff_pp(district.get("age_65_plus_pct"), state.get("age_65_plus_pct")))
        add_pp("BA+ attainment", diff_pp(district.get("ba_plus_pct"), state.get("ba_plus_pct")))
        add_pp("renters", diff_pp(district.get("renter_occupied_pct"), state.get("renter_occupied_pct")))

        inc_d = district.get("median_household_income")
        inc_s = state.get("median_household_income")
        if isinstance(inc_d, int) and isinstance(inc_s, int):
            delta = inc_d - inc_s
            if abs(delta) >= 5000:
                direction = "higher" if delta > 0 else "lower"
                highlights.append(f"- Median household income is {fmt_money(abs(delta))} {direction} than Idaho.")

    header = f"District & State Snapshot (ACS 5-year, {state.get('acs_year') if state else (district.get('acs_year') if district else '—')})"
    out = [header, ""]

    if state:
        out.append(block("Idaho statewide:", state).rstrip())

    if district:
        label = "Senate" if (chamber or "").strip().lower() == "senate" else ("House" if (chamber or "").strip().lower() == "house" else "District")
        out.append("")
        out.append(block(f"{label} District {dist_num}:", district).rstrip())

    if highlights:
        out.append("")
        out.append("District vs State highlights:")
        out.extend(highlights)

    out.append("")
    return "\n".join(out)
