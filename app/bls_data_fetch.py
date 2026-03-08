"""
bls_data_fetch.py
Fetch and cache Bureau of Labor Statistics data for Idaho.

Caches:
- State-level unemployment/employment stats (LAUS)
- County-level unemployment rates (LAUS)
- Industry employment and wages (QCEW via CSV)
- Consumer Price Index - West Region (CPI)
- Job Openings and Labor Turnover - Idaho (JOLTS)
- Occupational Employment and Wage Statistics - Idaho (OEWS)

Run monthly via cron to keep data fresh.
"""
import json
import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Configuration
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
CACHE_FILE = Path("/app/data/bls_idaho_cache.json")

# Idaho FIPS code
IDAHO_FIPS = "16"

# LAUS Series ID format: LASST{state_fips}0000000000000{measure}
# Measures: 3=unemployment rate, 4=unemployment, 5=employment, 6=labor force
LAUS_STATE_SERIES = {
    "unemployment_rate": f"LASST{IDAHO_FIPS}0000000000003",
    "unemployment_count": f"LASST{IDAHO_FIPS}0000000000004",
    "employment_count": f"LASST{IDAHO_FIPS}0000000000005",
    "labor_force": f"LASST{IDAHO_FIPS}0000000000006",
}

# Idaho counties with FIPS codes
IDAHO_COUNTIES = {
    "Ada": "001", "Adams": "003", "Bannock": "005", "Bear Lake": "007",
    "Benewah": "009", "Bingham": "011", "Blaine": "013", "Boise": "015",
    "Bonner": "017", "Bonneville": "019", "Boundary": "021", "Butte": "023",
    "Camas": "025", "Canyon": "027", "Caribou": "029", "Cassia": "031",
    "Clark": "033", "Clearwater": "035", "Custer": "037", "Elmore": "039",
    "Franklin": "041", "Fremont": "043", "Gem": "045", "Gooding": "047",
    "Idaho": "049", "Jefferson": "051", "Jerome": "053", "Kootenai": "055",
    "Latah": "057", "Lemhi": "059", "Lewis": "061", "Lincoln": "063",
    "Madison": "065", "Minidoka": "067", "Nez Perce": "069", "Oneida": "071",
    "Owyhee": "073", "Payette": "075", "Power": "077", "Shoshone": "079",
    "Teton": "081", "Twin Falls": "083", "Valley": "085", "Washington": "087",
}

# NAICS industry sectors for QCEW (2-digit sector codes)
# Note: QCEW uses agglvl_code 54 for 2-digit private sector data
NAICS_SECTORS = {
    "agriculture": "11",
    "mining": "21",
    "utilities": "22",
    "construction": "23",
    "manufacturing": "31-33",
    "wholesale_trade": "42",
    "retail_trade": "44-45",
    "transportation": "48-49",
    "information": "51",
    "finance_insurance": "52",
    "real_estate": "53",
    "professional_services": "54",
    "management": "55",
    "admin_support": "56",
    "education": "61",
    "healthcare": "62",
    "arts_entertainment": "71",
    "accommodation_food": "72",
    "other_services": "81",
    "public_admin": "92",
}

# Reverse mapping for CSV parsing (handles multi-digit codes)
NAICS_CODE_TO_SECTOR = {v: k for k, v in NAICS_SECTORS.items()}

# CPI Series IDs - West Region (Idaho doesn't have state-specific CPI)
# Format: CUUR0400SA{item_code}
# 0400 = West Region area code
CPI_WEST_SERIES = {
    "all_items": "CUUR0400SA0",
    "food": "CUUR0400SAF1",
    "housing": "CUUR0400SAH",
    "medical_care": "CUUR0400SAM",
    "transportation": "CUUR0400SAT",
    "energy": "CUUR0400SAE",
}

# JOLTS Series IDs - Idaho state level
# Format: JTS + industry(6) + state(2) + area(5) + sizeclass(2) + element(2) + rate/level(1)
# 000000 = total nonfarm, 16 = Idaho, 00000 = statewide, 00 = all sizes
# Elements: JO=job openings, QU=quits, LD=layoffs/discharges, HI=hires
# Rate/Level: L=level (thousands), R=rate (percent)
JOLTS_STATE_SERIES = {
    "job_openings": f"JTS000000{IDAHO_FIPS}0000000JOL",
    "job_openings_rate": f"JTS000000{IDAHO_FIPS}0000000JOR",
    "hires": f"JTS000000{IDAHO_FIPS}0000000HIL",
    "hires_rate": f"JTS000000{IDAHO_FIPS}0000000HIR",
    "quits": f"JTS000000{IDAHO_FIPS}0000000QUL",
    "quits_rate": f"JTS000000{IDAHO_FIPS}0000000QUR",
    "layoffs": f"JTS000000{IDAHO_FIPS}0000000LDL",
    "layoffs_rate": f"JTS000000{IDAHO_FIPS}0000000LDR",
}

# Key occupations to track from OEWS (SOC codes)
# Focus on occupations relevant to state government and key industries
KEY_OCCUPATIONS = {
    "all_occupations": "00-0000",
    "management": "11-0000",
    "healthcare_practitioners": "29-0000",
    "healthcare_support": "31-0000",
    "protective_service": "33-0000",
    "education_training": "25-0000",
    "construction_trades": "47-0000",
    "production": "51-0000",
    "transportation": "53-0000",
    "office_admin": "43-0000",
    "sales": "41-0000",
    "food_prep_serving": "35-0000",
    "farming_fishing": "45-0000",
}


def fetch_bls_series(series_ids: List[str], start_year: int, end_year: int) -> Dict[str, Any]:
    """
    Fetch data for one or more BLS series.

    Args:
        series_ids: List of BLS series IDs
        start_year: Start year for data
        end_year: End year for data

    Returns:
        API response as dict
    """
    if not BLS_API_KEY:
        print("WARNING: BLS_API_KEY not set, using unregistered API (lower limits)")

    headers = {"Content-type": "application/json"}
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY,
        "calculations": True,
        "annualaverage": True,
    }

    try:
        response = requests.post(BLS_API_URL, data=json.dumps(payload), headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"BLS API error: {e}")
        return {"status": "ERROR", "message": str(e)}


def get_latest_value(series_data: List[Dict]) -> Optional[Dict[str, Any]]:
    """Extract the most recent data point from a series."""
    if not series_data:
        return None

    # Sort by year and period (M01-M12 for monthly, A01 for annual)
    sorted_data = sorted(
        series_data,
        key=lambda x: (x.get("year", ""), x.get("period", "")),
        reverse=True
    )

    # Get most recent non-annual value (monthly preferred)
    for item in sorted_data:
        if item.get("period", "").startswith("M"):
            return {
                "value": float(item.get("value", 0)),
                "year": item.get("year"),
                "period": item.get("period"),
                "period_name": item.get("periodName"),
            }

    # Fallback to annual
    for item in sorted_data:
        if item.get("period", "").startswith("A"):
            return {
                "value": float(item.get("value", 0)),
                "year": item.get("year"),
                "period": "Annual",
                "period_name": "Annual",
            }

    return None


def fetch_state_laus() -> Dict[str, Any]:
    """Fetch Idaho state-level LAUS data."""
    print("Fetching state LAUS data...")

    current_year = datetime.now().year
    series_ids = list(LAUS_STATE_SERIES.values())

    result = fetch_bls_series(series_ids, current_year - 2, current_year)

    if result.get("status") != "REQUEST_SUCCEEDED":
        print(f"State LAUS fetch failed: {result.get('message', 'Unknown error')}")
        return {}

    state_data = {}
    for series in result.get("Results", {}).get("series", []):
        series_id = series.get("seriesID", "")
        data = series.get("data", [])

        # Map series ID back to field name
        for field_name, sid in LAUS_STATE_SERIES.items():
            if series_id == sid:
                latest = get_latest_value(data)
                if latest:
                    state_data[field_name] = latest["value"]
                    state_data[f"{field_name}_period"] = f"{latest['period_name']} {latest['year']}"
                break

    return state_data


def fetch_county_laus() -> Dict[str, Dict[str, Any]]:
    """Fetch county-level unemployment rates."""
    print("Fetching county LAUS data...")

    current_year = datetime.now().year
    county_data = {}

    # Build series IDs for all counties (unemployment rate only)
    # County LAUS format: LAUCN{state}{county}0000000003
    series_ids = []
    county_map = {}

    for county_name, county_fips in IDAHO_COUNTIES.items():
        series_id = f"LAUCN{IDAHO_FIPS}{county_fips}0000000003"
        series_ids.append(series_id)
        county_map[series_id] = county_name

    # Fetch in batches of 50 (API limit)
    for i in range(0, len(series_ids), 50):
        batch = series_ids[i:i+50]
        result = fetch_bls_series(batch, current_year - 1, current_year)

        if result.get("status") != "REQUEST_SUCCEEDED":
            print(f"County LAUS batch {i//50 + 1} failed")
            continue

        for series in result.get("Results", {}).get("series", []):
            series_id = series.get("seriesID", "")
            data = series.get("data", [])

            if series_id in county_map:
                county_name = county_map[series_id]
                latest = get_latest_value(data)
                if latest:
                    county_data[county_name] = {
                        "fips": f"{IDAHO_FIPS}{IDAHO_COUNTIES[county_name]}",
                        "unemployment_rate": latest["value"],
                        "period": f"{latest['period_name']} {latest['year']}",
                    }

    return county_data


def fetch_qcew_industry_data() -> Dict[str, Dict[str, Any]]:
    """
    Fetch QCEW industry data for Idaho via CSV endpoint.
    QCEW data is accessed differently - via direct CSV files.
    """
    print("Fetching QCEW industry data...")

    industry_data = {}
    current_year = datetime.now().year

    # Try current year, fall back to previous years
    for year in [current_year, current_year - 1, current_year - 2]:
        # QCEW CSV endpoint format
        url = f"https://data.bls.gov/cew/data/api/{year}/a/area/{IDAHO_FIPS}000.csv"

        try:
            response = requests.get(url, timeout=60)
            if response.status_code != 200:
                print(f"  QCEW {year}: HTTP {response.status_code}")
                continue

            # Parse CSV using csv module for proper handling
            import csv
            from io import StringIO

            reader = csv.DictReader(StringIO(response.text))

            for row in reader:
                # Filter: private sector (own_code=5), 2-digit NAICS level (agglvl_code=54)
                own_code = row.get("own_code", "").strip('"')
                agglvl_code = row.get("agglvl_code", "").strip('"')

                if own_code != "5" or agglvl_code != "54":
                    continue

                industry_code = row.get("industry_code", "").strip('"')

                # Map industry code to sector name
                if industry_code in NAICS_CODE_TO_SECTOR:
                    sector_name = NAICS_CODE_TO_SECTOR[industry_code]
                    try:
                        emp = row.get("annual_avg_emplvl", "0").strip('"')
                        wage = row.get("annual_avg_wkly_wage", "0").strip('"')
                        industry_data[sector_name] = {
                            "naics": industry_code,
                            "employment": int(emp) if emp else 0,
                            "avg_weekly_wage": int(wage) if wage else 0,
                            "year": year,
                        }
                    except (ValueError, TypeError) as e:
                        print(f"  Parse error for {sector_name}: {e}")

            if industry_data:
                print(f"  Loaded {len(industry_data)} industries from QCEW {year}")
                break

        except Exception as e:
            print(f"  QCEW fetch error for {year}: {e}")

    return industry_data


def fetch_cpi_west() -> Dict[str, Any]:
    """
    Fetch CPI data for West Region.
    Idaho doesn't have state-specific CPI, so we use West Region as proxy.
    """
    print("Fetching CPI West Region data...")

    current_year = datetime.now().year
    series_ids = list(CPI_WEST_SERIES.values())

    result = fetch_bls_series(series_ids, current_year - 2, current_year)

    if result.get("status") != "REQUEST_SUCCEEDED":
        print(f"CPI fetch failed: {result.get('message', 'Unknown error')}")
        return {}

    cpi_data = {}
    for series in result.get("Results", {}).get("series", []):
        series_id = series.get("seriesID", "")
        data = series.get("data", [])

        # Map series ID back to field name
        for field_name, sid in CPI_WEST_SERIES.items():
            if series_id == sid:
                latest = get_latest_value(data)
                if latest:
                    cpi_data[field_name] = latest["value"]
                    cpi_data[f"{field_name}_period"] = f"{latest['period_name']} {latest['year']}"

                    # Calculate year-over-year change if we have enough data
                    if len(data) >= 13:  # Need at least 13 months for YoY
                        sorted_data = sorted(
                            data,
                            key=lambda x: (x.get("year", ""), x.get("period", "")),
                            reverse=True
                        )
                        # Find same month last year
                        current = sorted_data[0]
                        current_period = current.get("period")
                        current_year_val = current.get("year")
                        for item in sorted_data:
                            if item.get("period") == current_period and item.get("year") == str(int(current_year_val) - 1):
                                try:
                                    old_val = float(item.get("value", 0))
                                    new_val = float(current.get("value", 0))
                                    if old_val > 0:
                                        yoy_change = round(((new_val - old_val) / old_val) * 100, 1)
                                        cpi_data[f"{field_name}_yoy_change"] = yoy_change
                                except (ValueError, ZeroDivisionError):
                                    pass
                                break
                break

    print(f"  Loaded {len([k for k in cpi_data.keys() if not k.endswith('_period') and not k.endswith('_change')])} CPI categories")
    return cpi_data


def fetch_jolts_state() -> Dict[str, Any]:
    """
    Fetch JOLTS (Job Openings and Labor Turnover) data for Idaho.
    """
    print("Fetching JOLTS Idaho data...")

    current_year = datetime.now().year
    series_ids = list(JOLTS_STATE_SERIES.values())

    result = fetch_bls_series(series_ids, current_year - 2, current_year)

    if result.get("status") != "REQUEST_SUCCEEDED":
        print(f"JOLTS fetch failed: {result.get('message', 'Unknown error')}")
        return {}

    jolts_data = {}
    for series in result.get("Results", {}).get("series", []):
        series_id = series.get("seriesID", "")
        data = series.get("data", [])

        # Map series ID back to field name
        for field_name, sid in JOLTS_STATE_SERIES.items():
            if series_id == sid:
                latest = get_latest_value(data)
                if latest:
                    jolts_data[field_name] = latest["value"]
                    jolts_data[f"{field_name}_period"] = f"{latest['period_name']} {latest['year']}"
                break

    # Calculate derived metrics
    if "job_openings" in jolts_data and "unemployment_count" in load_cache().get("state", {}):
        # This will be calculated after we have both pieces of data
        pass

    print(f"  Loaded {len([k for k in jolts_data.keys() if not k.endswith('_period')])} JOLTS metrics")
    return jolts_data


def fetch_oews_data() -> Dict[str, Dict[str, Any]]:
    """
    Fetch OEWS (Occupational Employment and Wage Statistics) for Idaho.

    Note: OEWS data is not easily accessible via API. BLS provides downloadable files
    but access is often restricted. This function attempts multiple approaches and
    returns empty gracefully if data cannot be fetched.

    For manual updates, visit: https://www.bls.gov/oes/current/oes_id.htm
    """
    print("Fetching OEWS Idaho data...")

    oews_data = {}
    current_year = datetime.now().year

    # Try CSV download first (most reliable when accessible)
    for year in [current_year - 1, current_year - 2]:
        url = f"https://www.bls.gov/oes/special-requests/oesm{str(year)[2:]}st.zip"

        try:
            import zipfile
            from io import BytesIO

            response = requests.get(url, timeout=120)
            if response.status_code == 403:
                print(f"  OEWS {year}: Access denied (403) - BLS blocking automated downloads")
                continue
            if response.status_code != 200:
                print(f"  OEWS {year}: HTTP {response.status_code}")
                continue

            # Extract and parse the CSV from the zip
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                for filename in z.namelist():
                    if 'state' in filename.lower() and filename.endswith('.csv'):
                        with z.open(filename) as f:
                            import csv
                            from io import TextIOWrapper

                            reader = csv.DictReader(TextIOWrapper(f, 'utf-8'))
                            for row in reader:
                                state = row.get("PRIM_STATE") or row.get("ST") or row.get("AREA")
                                if state not in ["ID", "16", "1600000"]:
                                    continue

                                occ_code = row.get("OCC_CODE", "")
                                occ_title = row.get("OCC_TITLE", "")

                                if not occ_code.endswith("0000"):
                                    continue

                                for occ_name, soc_code in KEY_OCCUPATIONS.items():
                                    if occ_code == soc_code:
                                        try:
                                            emp = row.get("TOT_EMP", "").replace(",", "")
                                            wage = row.get("H_MEAN", "").replace(",", "")
                                            annual = row.get("A_MEAN", "").replace(",", "")

                                            oews_data[occ_name] = {
                                                "soc_code": soc_code,
                                                "occupation_title": occ_title,
                                                "employment": int(float(emp)) if emp and emp != "*" else None,
                                                "hourly_wage": float(wage) if wage and wage != "*" else None,
                                                "annual_wage": int(float(annual)) if annual and annual != "*" else None,
                                                "year": year,
                                            }
                                        except (ValueError, TypeError):
                                            pass
                                        break

            if oews_data:
                print(f"  Loaded {len(oews_data)} occupations from OEWS {year}")
                break

        except Exception as e:
            print(f"  OEWS fetch error for {year}: {e}")

    if not oews_data:
        print("  OEWS data unavailable - BLS restricts automated access")
        print("  Manual data can be added from: https://www.bls.gov/oes/current/oes_id.htm")

    return oews_data


def build_cache() -> Dict[str, Any]:
    """Build the complete BLS cache."""
    print(f"Building BLS cache at {datetime.now(timezone.utc).isoformat()}")

    # Fetch all data sources
    state_laus = fetch_state_laus()
    county_laus = fetch_county_laus()
    industries = fetch_qcew_industry_data()
    cpi_data = fetch_cpi_west()
    jolts_data = fetch_jolts_state()
    oews_data = fetch_oews_data()

    cache = {
        "metadata": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "source": "BLS API v2 / QCEW / OEWS",
            "api_version": "2.0",
        },
        "state": state_laus,
        "counties": county_laus,
        "industries": industries,
        "cpi": cpi_data,
        "jolts": jolts_data,
        "occupations": oews_data,
    }

    # Calculate derived metrics
    if jolts_data.get("job_openings") and state_laus.get("unemployment_count"):
        try:
            # Unemployed per job opening ratio (lower = tighter labor market)
            job_openings_k = jolts_data["job_openings"]  # in thousands
            unemployed_k = state_laus["unemployment_count"] / 1000  # convert to thousands
            cache["state"]["unemployed_per_opening"] = round(unemployed_k / job_openings_k, 2)
        except (ZeroDivisionError, TypeError):
            pass

    # Summary stats
    cache["metadata"]["state_fields"] = len(cache["state"])
    cache["metadata"]["counties_count"] = len(cache["counties"])
    cache["metadata"]["industries_count"] = len(cache["industries"])
    cache["metadata"]["cpi_categories"] = len([k for k in cpi_data.keys() if not k.endswith('_period') and not k.endswith('_change')])
    cache["metadata"]["jolts_metrics"] = len([k for k in jolts_data.keys() if not k.endswith('_period')])
    cache["metadata"]["occupations_count"] = len(oews_data)

    return cache


def save_cache(cache: Dict[str, Any]) -> None:
    """Save cache to disk."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)
    print(f"Cache saved to {CACHE_FILE}")


def load_cache() -> Optional[Dict[str, Any]]:
    """Load cache from disk."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return None


def get_state_bls_data() -> Dict[str, Any]:
    """Get cached state-level BLS data."""
    cache = load_cache()
    if cache:
        return cache.get("state", {})
    return {}








def refresh_cache() -> bool:
    """Refresh the BLS cache. Called by cron job."""
    try:
        cache = build_cache()
        save_cache(cache)

        # Also generate district-level estimates
        try:
            from district_county_mapping import convert_bls_to_districts
            district_data = convert_bls_to_districts(cache)
            district_cache = {
                "metadata": {
                    "source": "BLS LAUS via county-to-district mapping",
                    "methodology": "Population-weighted average of county unemployment rates",
                    "last_updated": cache.get("metadata", {}).get("last_updated"),
                },
                "districts": district_data,
            }
            district_cache_file = CACHE_FILE.parent / "bls_district_cache.json"
            with open(district_cache_file, "w") as f:
                json.dump(district_cache, f, indent=2)
            print(f"District cache saved to {district_cache_file}")
        except ImportError:
            print("Warning: district_county_mapping not available, skipping district cache")
        except Exception as e:
            print(f"Warning: District cache generation failed: {e}")

        print("BLS cache refresh complete!")
        return True
    except Exception as e:
        print(f"Cache refresh failed: {e}")
        return False


def get_district_bls_data(ld_code: str) -> Optional[Dict[str, Any]]:
    """
    Get BLS data for a specific legislative district.

    Args:
        ld_code: District code (e.g., "LD12" or "12")

    Returns:
        Dict with unemployment_rate, primary_county, counties, period
    """
    # Normalize district code
    if not ld_code.upper().startswith("LD"):
        ld_code = f"LD{int(ld_code):02d}"
    else:
        ld_code = ld_code.upper()

    district_cache_file = CACHE_FILE.parent / "bls_district_cache.json"
    if district_cache_file.exists():
        with open(district_cache_file, "r") as f:
            cache = json.load(f)
        return cache.get("districts", {}).get(ld_code)

    # Fallback: calculate on-the-fly if district cache missing
    county_cache = load_cache()
    if county_cache:
        try:
            from district_county_mapping import get_district_bls_estimate, get_primary_county, get_district_counties
            county_data = county_cache.get("counties", {})
            return {
                "unemployment_rate": get_district_bls_estimate(ld_code, county_data, "unemployment_rate"),
                "primary_county": get_primary_county(ld_code),
                "counties": get_district_counties(ld_code),
            }
        except ImportError:
            pass

    return None


# CLI entry point
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Quick test of API connectivity
        print("Testing BLS API connection...")
        result = fetch_bls_series(["LASST160000000000003"], 2024, 2025)
        print(f"Status: {result.get('status')}")
        if result.get("status") == "REQUEST_SUCCEEDED":
            series = result.get("Results", {}).get("series", [])
            if series:
                print(f"Got {len(series[0].get('data', []))} data points")
        else:
            print(f"Error: {result}")
    else:
        # Full cache refresh
        refresh_cache()
