"""
district_county_mapping.py
Maps Idaho legislative districts to counties with population weights.

Used to convert county-level BLS data to district-level estimates.

Methodology:
- For districts entirely within one county: weight = 1.0
- For districts spanning multiple counties: weights based on population distribution
- For split counties: population allocated proportionally to districts

Data sources:
- Idaho Commission for Reapportionment 2021
- US Census 2020 redistricting data
- Idaho Legislature district maps
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional

# District to county mapping with population weights
# Format: {district: [(county, weight), ...]}
# Weights sum to 1.0 for each district

DISTRICT_COUNTY_MAPPING = {
    # Northern Idaho
    "LD01": [("Boundary", 0.70), ("Bonner", 0.30)],
    "LD02": [("Shoshone", 0.25), ("Clearwater", 0.15), ("Benewah", 0.20), ("Bonner", 0.25), ("Kootenai", 0.15)],
    "LD03": [("Kootenai", 1.0)],
    "LD04": [("Kootenai", 1.0)],
    "LD05": [("Kootenai", 1.0)],
    "LD06": [("Latah", 0.70), ("Lewis", 0.10), ("Nez Perce", 0.20)],
    "LD07": [("Idaho", 0.35), ("Clearwater", 0.15), ("Lewis", 0.10), ("Nez Perce", 0.40)],
    "LD08": [("Boise", 0.15), ("Custer", 0.10), ("Gem", 0.25), ("Lemhi", 0.15), ("Valley", 0.35)],

    # Southwest Idaho - Canyon County area
    "LD09": [("Payette", 0.45), ("Washington", 0.40), ("Canyon", 0.15)],
    "LD10": [("Canyon", 0.85), ("Ada", 0.15)],
    "LD11": [("Canyon", 1.0)],
    "LD12": [("Canyon", 1.0)],
    "LD13": [("Canyon", 1.0)],

    # Ada County districts (Boise metro)
    "LD14": [("Gem", 0.10), ("Ada", 0.90)],
    "LD15": [("Ada", 1.0)],
    "LD16": [("Ada", 1.0)],
    "LD17": [("Ada", 1.0)],
    "LD18": [("Ada", 1.0)],
    "LD19": [("Ada", 1.0)],
    "LD20": [("Ada", 1.0)],
    "LD21": [("Ada", 1.0)],
    "LD22": [("Ada", 1.0)],
    "LD23": [("Owyhee", 0.20), ("Ada", 0.35), ("Canyon", 0.30), ("Elmore", 0.15)],

    # South Central Idaho - Magic Valley
    "LD24": [("Gooding", 0.35), ("Twin Falls", 0.50), ("Camas", 0.15)],
    "LD25": [("Jerome", 0.65), ("Twin Falls", 0.35)],
    "LD26": [("Blaine", 0.50), ("Lincoln", 0.20), ("Gooding", 0.15), ("Camas", 0.15)],
    "LD27": [("Cassia", 0.55), ("Minidoka", 0.45)],

    # Southeast Idaho
    "LD28": [("Power", 0.30), ("Bannock", 0.70)],
    "LD29": [("Bannock", 1.0)],
    "LD30": [("Bonneville", 1.0)],
    "LD31": [("Bingham", 1.0)],
    "LD32": [("Bear Lake", 0.12), ("Caribou", 0.15), ("Franklin", 0.25), ("Oneida", 0.08), ("Teton", 0.25), ("Bonneville", 0.15)],
    "LD33": [("Bonneville", 1.0)],
    "LD34": [("Madison", 0.75), ("Bonneville", 0.25)],
    "LD35": [("Butte", 0.05), ("Clark", 0.02), ("Fremont", 0.30), ("Jefferson", 0.63)],
}

# Reverse mapping: county to districts with weights
def build_county_to_district_mapping() -> Dict[str, List[tuple]]:
    """Build reverse mapping from counties to districts."""
    county_to_districts = {}
    for district, counties in DISTRICT_COUNTY_MAPPING.items():
        for county, weight in counties:
            if county not in county_to_districts:
                county_to_districts[county] = []
            county_to_districts[county].append((district, weight))
    return county_to_districts

COUNTY_TO_DISTRICTS = build_county_to_district_mapping()


def get_district_bls_estimate(
    district: str,
    county_data: Dict[str, Dict[str, Any]],
    field: str = "unemployment_rate"
) -> Optional[float]:
    """
    Calculate weighted BLS estimate for a district based on county data.

    Args:
        district: District code (e.g., "LD01")
        county_data: Dict of county BLS data {county_name: {field: value, ...}}
        field: Which field to calculate (e.g., "unemployment_rate")

    Returns:
        Weighted average value, or None if insufficient data
    """
    if district not in DISTRICT_COUNTY_MAPPING:
        return None

    total_weight = 0.0
    weighted_sum = 0.0

    for county, weight in DISTRICT_COUNTY_MAPPING[district]:
        if county in county_data and field in county_data[county]:
            value = county_data[county][field]
            if value is not None:
                weighted_sum += value * weight
                total_weight += weight

    if total_weight == 0:
        return None

    return round(weighted_sum / total_weight, 2)


def get_primary_county(district: str) -> Optional[str]:
    """Get the primary (highest weight) county for a district."""
    if district not in DISTRICT_COUNTY_MAPPING:
        return None

    counties = DISTRICT_COUNTY_MAPPING[district]
    if not counties:
        return None

    # Return county with highest weight
    return max(counties, key=lambda x: x[1])[0]


def get_district_counties(district: str) -> List[str]:
    """Get list of counties in a district."""
    if district not in DISTRICT_COUNTY_MAPPING:
        return []
    return [county for county, _ in DISTRICT_COUNTY_MAPPING[district]]


def convert_bls_to_districts(bls_cache: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Convert county-level BLS data to district-level estimates.

    Args:
        bls_cache: Full BLS cache with 'counties' key

    Returns:
        Dict of district BLS estimates {district: {field: value, ...}}
    """
    county_data = bls_cache.get("counties", {})
    district_estimates = {}

    for district in DISTRICT_COUNTY_MAPPING.keys():
        estimate = {
            "unemployment_rate": get_district_bls_estimate(district, county_data, "unemployment_rate"),
            "primary_county": get_primary_county(district),
            "counties": get_district_counties(district),
        }

        # Add the period from the first available county
        for county in estimate["counties"]:
            if county in county_data and "period" in county_data[county]:
                estimate["period"] = county_data[county]["period"]
                break

        district_estimates[district] = estimate

    return district_estimates


    import sys

    # Test with sample data
    sample_counties = {
        "Ada": {"unemployment_rate": 3.1, "period": "November 2025"},
        "Canyon": {"unemployment_rate": 4.2, "period": "November 2025"},
        "Kootenai": {"unemployment_rate": 3.8, "period": "November 2025"},
        "Bonneville": {"unemployment_rate": 2.9, "period": "November 2025"},
    }

    print("=== District-County Mapping Test ===\n")

    # Test a few districts
    for district in ["LD03", "LD10", "LD23", "LD30"]:
        rate = get_district_bls_estimate(district, sample_counties, "unemployment_rate")
        primary = get_primary_county(district)
        counties = get_district_counties(district)
        print(f"{district}:")
        print(f"  Primary county: {primary}")
        print(f"  All counties: {', '.join(counties)}")
        print(f"  Est. unemployment: {rate}%")
        print()

    # If BLS cache exists, test with real data
    cache_path = Path("/app/data/bls_idaho_cache.json")
    if cache_path.exists():
        print("=== Real BLS Data Conversion ===\n")
        with open(cache_path) as f:
            bls_cache = json.load(f)

        district_data = convert_bls_to_districts(bls_cache)
        for district in ["LD01", "LD15", "LD30", "LD35"]:
            data = district_data.get(district, {})
            print(f"{district}: {data.get('unemployment_rate')}% (primary: {data.get('primary_county')})")
