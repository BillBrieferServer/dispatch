"""
section_unintended.py

Dispatch Module A — Unintended Consequences (Section 3).
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
SECTION_MAX_TOKENS = 2000
MAX_RETRIES = 2


def _load_prompt() -> str:
    """Load the unintended consequences prompt."""
    path = os.path.join(PROMPTS_DIR, "base", "section_unintended.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    # Inline fallback
    return """You are a nonpartisan legislative drafter reviewing a bill for Idaho leadership.

Identify 3-5 drafting and implementation problems the sponsor may not have considered.

Focus on: undefined terms, scope gaps, implementation mechanics, conflicts with existing Idaho Code, enforcement gaps, timeline problems.

NEUTRALITY: Accept the bill's premise. Do not argue against its policy goals. Do not predict litigation or political outcomes. Use "may" and "could," never "will" or "certainly." Cite specific sections.

No item may begin with "The bill" or "This bill."

Output JSON: {"unintended_consequences": ["string", "string", ...]}"""


def _validate(data: dict) -> str | None:
    """Validate unintended consequences output. Returns error message or None."""
    items = data.get("unintended_consequences")
    if not isinstance(items, list):
        return "unintended_consequences must be a list"
    if len(items) < 3 or len(items) > 5:
        return f"Expected 3-5 items, got {len(items)}"
    for i, item in enumerate(items):
        if not isinstance(item, str):
            return f"Item {i} is not a string"
        if len(item) < 80:
            return f"Item {i} too short ({len(item)} chars, min 80)"
        if item.lower().startswith(("the bill", "this bill")):
            return f"Item {i} starts with 'The bill' or 'This bill'"
    return None


def generate_unintended_consequences(
    *,
    bill_number: str,
    bill_title: str,
    bill_text: str,
    bill_summary: str = "",
    fiscal_note_text: str = "",
) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """
    Generate Section 3 (Unintended Consequences) via independent AI call.

    Returns: (result_dict, error_message, token_usage)
    """
    client = get_client()
    if not client:
        return None, "Anthropic API key not configured", {}

    system_prompt = _load_prompt()

    fiscal_block = ""
    if fiscal_note_text:
        fiscal_block = f"\n\nFISCAL NOTE (do not duplicate these items):\n{fiscal_note_text[:2000]}"

    user_message = f"""BILL: {bill_number} — {bill_title}

BILL SUMMARY: {bill_summary}
{fiscal_block}

BILL TEXT:
{bill_text[:15000]}

Generate 3-5 unintended consequences as JSON."""

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
                    logger.warning(f"[{bill_number}] S3 validation failed (attempt {attempt+1}): {error}")
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

            logger.info(f"[{bill_number}] S3 generated: {len(parsed['unintended_consequences'])} items")
            return parsed, None, token_usage

        except json.JSONDecodeError as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"[{bill_number}] S3 JSON parse failed (attempt {attempt+1}): {e}")
                continue
            return None, f"JSON parse failed: {e}", {}

        except Exception as e:
            return None, f"S3 error: {e}", {}

    return None, "S3 generation exhausted retries", {}
