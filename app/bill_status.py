"""
bill_status.py

Status mapping for bill dashboard display.
Derives status from bills.committee (the bill's current location in the process).
"""

# (label, background_color)
STATUS_GROUPS = {
    "introduced":       ("Introduced",       "#4A90D9"),
    "in_committee":     ("In Committee",     "#E8A020"),
    "committee_action": ("Amending Order",   "#E07020"),
    "floor_vote":       ("3rd Reading",      "#6B8ED6"),
    "passed_chamber":   ("Passed Chamber",   "#2E8B2E"),
    "held_failed":      ("Held / Failed",    "#CC3333"),
    "second_chamber":   ("Second Chamber",   "#7B52AB"),
    "with_governor":    ("With Governor",    "#888888"),
    "signed_law":       ("Signed into Law",  "#1A5C1A"),
    "vetoed":           ("Vetoed",           "#8B1A1A"),
}

# Procedural stage patterns found in bills.committee field
# These indicate the bill has moved beyond a committee
_STAGE_PATTERNS = [
    ("PASSED",    "passed_chamber"),
    ("3rd Rdg",   "floor_vote"),
    ("14th Ord",  "committee_action"),
    ("FAILED",    "held_failed"),
]


def classify_status(committee_location, last_action=None):
    """
    Classify a bill\'s status from its committee field (current location).
    Falls back to last_action text if committee is empty.
    Returns (label, color) tuple.
    """
    if not committee_location:
        # No location info — check last_action as fallback
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

    # Check procedural stage patterns
    for pattern, group_key in _STAGE_PATTERNS:
        if pattern in loc:
            return STATUS_GROUPS[group_key]

    # If it\'s a committee name (short code or full name), bill is in committee
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
# Used by dashboard to display clean committee names
_COMMITTEE_NAMES = {
    # House
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
    # Senate
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

# Also normalize full names that have inconsistent formatting
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

# Referral event text patterns -> clean names
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
    # Try short code first
    if name in _COMMITTEE_NAMES:
        return _COMMITTEE_NAMES[name]
    # Try full name cleanup
    if name in _FULL_NAME_CLEANUP:
        return _FULL_NAME_CLEANUP[name]
    # Try referral cleanup
    if name in _REFERRAL_CLEANUP:
        return _REFERRAL_CLEANUP[name]
    # Strip trailing "Committee" if present
    if name.endswith(' Committee'):
        name = name[:-10].strip()
    return name
