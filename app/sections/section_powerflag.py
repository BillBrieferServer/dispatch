"""
section_powerflag.py

Dispatch Module B — Power Flag (Section 4).
Independent AI call, separate from the main briefing.
"""
import json
import logging
import os
from typing import Dict, Optional, Tuple

from app.services.anthropic_client import (
    get_client,
    _call_with_retry,
    ANTHROPIC_MODEL,
)

logger = logging.getLogger(__name__)

PROMPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")
SECTION_MAX_TOKENS = 1500
MAX_RETRIES = 2

VALID_FLAG_LEVELS = {"none", "low", "medium", "high"}
VALID_DIRECTIONS = {"executive", "federal", "judicial", "automatic", "none"}


def _load_prompt() -> str:
    """Load the power flag prompt."""
    path = os.path.join(PROMPTS_DIR, "base", "section_powerflag.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    # Inline fallback
    return """You are a legislative analyst for Idaho's House and Senate leadership.

Does this bill transfer decision-making power away from the legislature?

Flag if the bill:
- Delegates rulemaking or implementation authority to executive agencies without guardrails
- Creates federal funding dependency that could alter Idaho law without legislative action
- Uses ambiguous terms that invite courts to define the law's scope
- Creates programs or authority that continue without legislative reauthorization

Do NOT flag: legislature mandating policy on local governments (that is exercising power, not losing it).

flag_level: "none" / "low" / "medium" / "high"
direction: "executive" / "federal" / "judicial" / "automatic" / "none"
explanation: 2-4 sentences citing specific provisions.

Output JSON: {"power_flag": {"flag_level": "string", "direction": "string", "explanation": "string"}}"""


def _validate(data: dict) -> str | None:
    """Validate power flag output. Returns error message or None."""
    pf = data.get("power_flag")
    if not isinstance(pf, dict):
        return "power_flag must be an object"
    fl = pf.get("flag_level", "")
    if fl not in VALID_FLAG_LEVELS:
        return f"flag_level '{fl}' not in {VALID_FLAG_LEVELS}"
    dr = pf.get("direction", "")
    if dr not in VALID_DIRECTIONS:
        return f"direction '{dr}' not in {VALID_DIRECTIONS}"
    explanation = pf.get("explanation", "")
    if not isinstance(explanation, str):
        return "explanation must be a string"
    if fl == "none" and len(explanation) < 40:
        return f"explanation too short for flag_level=none ({len(explanation)} chars, min 40)"
    if fl != "none" and len(explanation) < 120:
        return f"explanation too short for flag_level={fl} ({len(explanation)} chars, min 120)"
    return None


def generate_power_flag(
    *,
    bill_number: str,
    bill_title: str,
    bill_text: str,
    bill_summary: str = "",
) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """
    Generate Section 4 (Power Flag) via independent AI call.

    Returns: (result_dict, error_message, token_usage)
    """
    client = get_client()
    if not client:
        return None, "Anthropic API key not configured", {}

    system_prompt = _load_prompt()

    user_message = f"""BILL: {bill_number} — {bill_title}

BILL SUMMARY: {bill_summary}

BILL TEXT:
{bill_text[:15000]}

Analyze for authority shifts and generate the power_flag JSON."""

    for attempt in range(MAX_RETRIES + 1):
        try:
            message = _call_with_retry(
                client=client,
                model=ANTHROPIC_MODEL,
                max_tokens=SECTION_MAX_TOKENS,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            response_text = message.content[0].text
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                json_lines = []
                in_block = False
                for line in lines:
                    if line.startswith("```") and not in_block:
                        in_block = True
                        continue
                    elif line.startswith("```") and in_block:
                        break
                    elif in_block:
                        json_lines.append(line)
                response_text = "\n".join(json_lines)

            parsed = json.loads(response_text)

            # Validate
            error = _validate(parsed)
            if error:
                if attempt < MAX_RETRIES:
                    logger.warning(f"[{bill_number}] S4 validation failed (attempt {attempt+1}): {error}")
                    user_message += f"\n\nYour previous response failed validation: {error}. Try again."
                    continue
                else:
                    return None, f"Validation failed after {MAX_RETRIES+1} attempts: {error}", {}

            token_usage = {}
            if hasattr(message, "usage") and message.usage:
                token_usage = {
                    "input_tokens": message.usage.input_tokens,
                    "output_tokens": message.usage.output_tokens,
                }

            logger.info(f"[{bill_number}] S4 generated: {parsed['power_flag']['flag_level']} / {parsed['power_flag']['direction']}")
            return parsed, None, token_usage

        except json.JSONDecodeError as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"[{bill_number}] S4 JSON parse failed (attempt {attempt+1}): {e}")
                continue
            return None, f"JSON parse failed: {e}", {}

        except Exception as e:
            return None, f"S4 error: {e}", {}

    return None, "S4 generation exhausted retries", {}
