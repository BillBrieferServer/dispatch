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
