"""
bill_status.py

Status mapping for bill dashboard display.
Maps free-text last_action strings to display groups via keyword pattern matching.
Single source of truth for status labels and colors.
"""

# (label, background_color)
STATUS_GROUPS = {
    "introduced":       ("Introduced",       "#4A90D9"),
    "in_committee":     ("In Committee",     "#E8A020"),
    "committee_action": ("Committee Action", "#E07020"),
    "passed_chamber":   ("Passed Chamber",   "#2E8B2E"),
    "held_failed":      ("Held / Failed",    "#CC3333"),
    "second_chamber":   ("Second Chamber",   "#7B52AB"),
    "with_governor":    ("With Governor",    "#888888"),
    "signed_law":       ("Signed into Law",  "#1A5C1A"),
    "vetoed":           ("Vetoed",           "#8B1A1A"),
}

# Ordered list of (keywords_to_match, status_group_key).
# First match wins. All matching is case-insensitive.
_PATTERNS = [
    # Most specific first
    ("vetoed by governor",                           "vetoed"),
    ("signed by governor",                           "signed_law"),
    ("became law",                                   "signed_law"),
    ("session law chapter",                          "signed_law"),
    ("delivered to secretary of state",              "signed_law"),
    ("transmitted to governor",                      "with_governor"),
    ("delivered to governor",                        "with_governor"),
    ("ordered delivered to governor",                "with_governor"),
    ("enrolled",                                     "with_governor"),
    ("signed by president",                          "with_governor"),
    ("signed by the speaker",                        "with_governor"),
    ("returned signed by the president",             "with_governor"),
    ("received from house",                          "second_chamber"),
    ("received from senate",                         "second_chamber"),
    ("to senate",                                    "second_chamber"),
    ("to house",                                     "second_chamber"),
    ("returned to house",                            "second_chamber"),
    ("returned to senate",                           "second_chamber"),
    ("passed",                                       "passed_chamber"),
    ("read third time",                              "passed_chamber"),
    ("third reading",                                "passed_chamber"),
    ("filed for third reading",                      "passed_chamber"),
    ("held in committee",                            "held_failed"),
    ("failed",                                       "held_failed"),
    ("do not pass",                                  "committee_action"),
    ("do pass",                                      "committee_action"),
    ("reported out of committee",                    "committee_action"),
    ("14th order",                                   "committee_action"),
    ("general orders",                               "committee_action"),
    ("reported printed; referred to",                "in_committee"),
    ("reported printed and referred to",             "in_committee"),
    ("referred to",                                  "in_committee"),
    ("reported printed",                             "in_committee"),
    ("read first time",                              "in_committee"),
    ("retained on calendar",                         "in_committee"),
    ("u.c. to be returned to",                       "in_committee"),
    ("u.c. to hold place",                           "in_committee"),
    ("u.c. to be placed",                            "in_committee"),
    ("second reading",                               "in_committee"),
    ("read second time",                             "in_committee"),
    ("introduced",                                   "introduced"),
    ("filed in office",                              "introduced"),
]


def classify_status(last_action):
    """
    Classify a bill's last_action text into a status group.
    Returns (label, color) tuple. Falls back to Introduced (blue) if unrecognized.
    """
    if not last_action:
        return STATUS_GROUPS["introduced"]

    text = last_action.lower()
    for pattern, group_key in _PATTERNS:
        if pattern in text:
            return STATUS_GROUPS[group_key]

    # Catch-all: anything unrecognized shows as Introduced (blue)
    return STATUS_GROUPS["introduced"]
