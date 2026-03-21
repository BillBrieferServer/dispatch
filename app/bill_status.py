"""
bill_status.py

Status and committee resolution for bill dashboard display.

Committee: derived from first "Referred to" event (originating committee).
Status: derived from bills.committee (current location) with cross-chamber awareness.
"""

# (label, background_color)
STATUS_GROUPS = {
    "introduced":         ("Introduced",         "#4A90D9"),
    "in_committee":       ("In Committee",       "#E8A020"),
    "amending_order":     ("Amending Order",     "#E07020"),
    "second_reading":     ("2nd Reading",        "#5B9BD5"),
    "floor_vote":         ("Floor Vote",         "#6B8ED6"),
    "passed_chamber":     ("Passed Chamber",     "#2E8B2E"),
    "crossed_to_senate":  ("Crossed to Senate",  "#7B52AB"),
    "crossed_to_house":   ("Crossed to House",   "#7B52AB"),
    "senate_committee":   ("Senate Committee",   "#9B6FC0"),
    "house_committee":    ("House Committee",    "#9B6FC0"),
    "senate_floor":       ("Senate Floor",       "#8B6ED6"),
    "house_floor":        ("House Floor",        "#8B6ED6"),
    "with_governor":      ("With Governor",      "#D4A017"),
    "signed_law":         ("Signed into Law",    "#1A5C1A"),
    "held_failed":        ("Held / Failed",      "#CC3333"),
    "vetoed":             ("Vetoed",             "#8B1A1A"),
}

# Procedural stage patterns found in bills.committee field
_STAGE_PATTERNS = [
    # Terminal
    ("LAW",         "signed_law"),
    ("ADOPTED",     "signed_law"),
    ("Vetoed",      "vetoed"),
    # Governor
    ("To Gov",      "with_governor"),
    ("To enrol",    "with_governor"),
    ("Sp signed",   "with_governor"),
    ("Pres signed", "with_governor"),
    ("To Sec",      "with_governor"),
    # Failed/held
    ("FAILED",      "held_failed"),
    ("Filed",       "held_failed"),
    ("Held",        "held_failed"),
    # Chamber passage
    ("PASSED",      "passed_chamber"),
    # Floor stages
    ("3rd Rdg",     "floor_vote"),
    ("Gen Ord",     "floor_vote"),
    ("2nd Rdg",     "second_reading"),
    ("14th Ord",    "amending_order"),
    ("10th Ord",    "amending_order"),
    # Pre-floor
    ("printing",    "introduced"),
]


def classify_status(committee_location, last_action=None, bill_number=None):
    """
    Classify a bill's status from its committee field (current location).
    Uses bill_number to determine originating chamber for cross-chamber labels.
    Returns (label, color) tuple.
    """
    if not committee_location:
        if last_action:
            text = last_action.lower()
            if "signed by governor" in text or "became law" in text:
                return STATUS_GROUPS["signed_law"]
            if "vetoed" in text:
                return STATUS_GROUPS["vetoed"]
            if "transmitted to governor" in text or "delivered to governor" in text or "enrolled" in text:
                return STATUS_GROUPS["with_governor"]
        return STATUS_GROUPS["introduced"]

    loc = committee_location.strip()

    # Determine originating chamber from bill number
    origin_chamber = None
    if bill_number:
        origin_chamber = 'H' if bill_number.startswith('H') else 'S'

    # Check procedural stage patterns first
    for pattern, group_key in _STAGE_PATTERNS:
        if pattern in loc:
            # For floor/committee stages, add cross-chamber awareness
            if origin_chamber and group_key in ('in_committee', 'floor_vote', 'second_reading', 'amending_order'):
                loc_chamber = 'H' if loc.startswith('H ') or loc.startswith('H') else 'S' if loc.startswith('S ') or loc.startswith('S') else None
                if loc_chamber and loc_chamber != origin_chamber:
                    # Bill is in the OTHER chamber
                    if group_key == 'floor_vote' or group_key == 'second_reading':
                        return STATUS_GROUPS[f"{'senate' if loc_chamber == 'S' else 'house'}_floor"]
                    elif group_key == 'amending_order':
                        return STATUS_GROUPS[f"{'senate' if loc_chamber == 'S' else 'house'}_floor"]
            return STATUS_GROUPS[group_key]

    # Not a procedural stage — it's a committee name
    # Check if bill is in the other chamber's committee
    if origin_chamber:
        loc_chamber = 'H' if loc.startswith('H ') else 'S' if loc.startswith('S ') else None
        if loc_chamber and loc_chamber != origin_chamber:
            return STATUS_GROUPS[f"{'senate' if loc_chamber == 'S' else 'house'}_committee"]

    return STATUS_GROUPS["in_committee"]


def is_procedural_stage(committee_location):
    """Return True if the committee field contains a procedural stage, not a committee name."""
    if not committee_location:
        return False
    loc = committee_location.strip()
    for pattern, _ in _STAGE_PATTERNS:
        if pattern in loc:
            return True
    return False


# Short code -> full committee name
_COMMITTEE_NAMES = {
    'H Agric Aff':   'Agricultural Affairs',
    'H Approp':      'Appropriations',
    'H Bus':         'Business',
    'H Com/HuRes':   'Commerce & Human Resources',
    'H Educ':        'Education',
    'H Env':         'Environment, Energy & Technology',
    'H Health/Wel':  'Health & Welfare',
    'H Jud':         'Judiciary, Rules & Administration',
    'H Loc Gov':     'Local Government',
    'H Res/Con':     'Resources & Conservation',
    'H Rev/Tax':     'Revenue & Taxation',
    'H St Aff':      'State Affairs',
    'H Transp':      'Transportation & Defense',
    'H Way/Means':   'Ways & Means',
    'H W/M':         'Ways & Means',
    'S Agric Aff':   'Agricultural Affairs',
    'S Com/HuRes':   'Commerce & Human Resources',
    'S Educ':        'Education',
    'S Fin':         'Finance',
    'S Health/Wel':  'Health & Welfare',
    'S Jud':         'Judiciary & Rules',
    'S Loc Gov':     'Local Government & Taxation',
    'S Res/Env':     'Resources & Environment',
    'S St Aff':      'State Affairs',
    'S Transp':      'Transportation',
}

_FULL_NAME_CLEANUP = {
    'Health And Welfare Committee': 'Health & Welfare',
    'Judiciary And Rules Committee': 'Judiciary & Rules',
    'Ways And Means Committee': 'Ways & Means',
    'State Affairs Committee': 'State Affairs',
    'Finance Committee': 'Finance',
    'Appropriations Committee': 'Appropriations',
    'Judiciary and Rules': 'Judiciary & Rules',
    'Ways and Means': 'Ways & Means',
    'Commerce and Human Resources': 'Commerce & Human Resources',
    'Environment, Energy and Technology': 'Environment, Energy & Technology',
    'Resources and Conservation': 'Resources & Conservation',
    'Resources and Environment': 'Resources & Environment',
    'Local Government and Taxation': 'Local Government & Taxation',
}

_REFERRAL_CLEANUP = {
    'Agricultural Affairs': 'Agricultural Affairs',
    'Appropriations': 'Appropriations',
    'Business': 'Business',
    'Commerce & Human Resources': 'Commerce & Human Resources',
    'Commerce and Human Resources': 'Commerce & Human Resources',
    'Education': 'Education',
    'Environment, Energy & Technology': 'Environment, Energy & Technology',
    'Environment, Energy and Technology': 'Environment, Energy & Technology',
    'Finance': 'Finance',
    'Health & Welfare': 'Health & Welfare',
    'Health and Welfare': 'Health & Welfare',
    'Judiciary & Rules': 'Judiciary & Rules',
    'Judiciary and Rules': 'Judiciary & Rules',
    'Judiciary, Rules & Administration': 'Judiciary, Rules & Administration',
    'Judiciary, Rules and Administration': 'Judiciary, Rules & Administration',
    'Local Government': 'Local Government',
    'Local Government & Taxation': 'Local Government & Taxation',
    'Local Government and Taxation': 'Local Government & Taxation',
    'Resources & Conservation': 'Resources & Conservation',
    'Resources and Conservation': 'Resources & Conservation',
    'Resources & Environment': 'Resources & Environment',
    'Resources and Environment': 'Resources & Environment',
    'Revenue & Taxation': 'Revenue & Taxation',
    'Revenue and Taxation': 'Revenue & Taxation',
    'State Affairs': 'State Affairs',
    'Transportation': 'Transportation',
    'Transportation & Defense': 'Transportation & Defense',
    'Transportation and Defense': 'Transportation & Defense',
    'Ways & Means': 'Ways & Means',
    'Ways and Means': 'Ways & Means',
}


def normalize_committee_name(raw_name):
    """Convert any committee name format to a clean display name."""
    if not raw_name:
        return ''
    name = raw_name.strip()
    if name in _COMMITTEE_NAMES:
        return _COMMITTEE_NAMES[name]
    if name in _FULL_NAME_CLEANUP:
        return _FULL_NAME_CLEANUP[name]
    if name in _REFERRAL_CLEANUP:
        return _REFERRAL_CLEANUP[name]
    if name.endswith(' Committee'):
        name = name[:-10].strip()
    return name
