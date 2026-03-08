"""
section6.py

Independent module for generating Section 6 (Debate Preparation) of the Bill Briefer.

This module produces floor statements and talking points via its own API call.
Section 6 is always generated fresh (never cached) to ensure variety across
different legislators requesting the same bill.
"""
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from app.services.anthropic_client import (
    get_client,
    _call_with_retry,
    ANTHROPIC_MODEL,
)

logger = logging.getLogger(__name__)

# Prompt directory
PROMPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")

# Max output tokens for Section 6
# 2 floor statements (~200 words each) + 10-14 talking points (~25 words each)
# ~750 words ≈ ~1000 tokens, but generous to allow substantive content
SECTION6_MAX_TOKENS = 3000


def load_prompt(section: str, stack: str = "base") -> str:
    """Load stack-specific prompt if it exists, otherwise use base."""
    if stack and stack != "base":
        stack_path = os.path.join(PROMPTS_DIR, stack, f"{section}.txt")
        if os.path.exists(stack_path):
            with open(stack_path, "r") as f:
                return f.read()
    base_path = os.path.join(PROMPTS_DIR, "base", f"{section}.txt")
    with open(base_path, "r") as f:
        return f.read()


def validate_section_6(result: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate Section 6 output meets quality requirements.

    Requirements:
    - floor_statement_pro: string, 100+ words
    - floor_statement_con: string, 100+ words
    - talking_points_for: list of 5+ strings, each 15+ words
    - talking_points_against: list of 5+ strings, each 15+ words

    Returns:
        (is_valid, error_message)
    """
    # Floor statement pro
    pro = result.get("floor_statement_pro", "")
    if not isinstance(pro, str) or not pro.strip():
        return False, "Missing floor_statement_pro"
    pro_words = len(pro.split())
    if pro_words < 100:
        return False, f"floor_statement_pro is {pro_words} words, need at least 100"

    # Floor statement con
    con = result.get("floor_statement_con", "")
    if not isinstance(con, str) or not con.strip():
        return False, "Missing floor_statement_con"
    con_words = len(con.split())
    if con_words < 100:
        return False, f"floor_statement_con is {con_words} words, need at least 100"

    # Talking points for
    tp_for = result.get("talking_points_for", [])
    if not isinstance(tp_for, list):
        return False, "talking_points_for is not a list"
    if len(tp_for) < 3:
        return False, f"talking_points_for has {len(tp_for)} items, need at least 3"
    for i, tp in enumerate(tp_for):
        if not isinstance(tp, str) or len(tp.split()) < 10:
            word_count = len(tp.split()) if isinstance(tp, str) else 0
            return False, f"talking_points_for[{i}] is only {word_count} words, need at least 10"

    # Talking points against
    tp_against = result.get("talking_points_against", [])
    if not isinstance(tp_against, list):
        return False, "talking_points_against is not a list"
    if len(tp_against) < 3:
        return False, f"talking_points_against has {len(tp_against)} items, need at least 3"
    for i, tp in enumerate(tp_against):
        if not isinstance(tp, str) or len(tp.split()) < 10:
            word_count = len(tp.split()) if isinstance(tp, str) else 0
            return False, f"talking_points_against[{i}] is only {word_count} words, need at least 10"

    return True, ""


def generate_section_6(
    bill_number: str,
    bill_title: str,
    bill_text: str = "",
    one_paragraph_summary: str = "",
    key_points: Optional[List[str]] = None,
    fiscal_note_text: str = "",
    stack: str = "base",
    max_retries: int = 2,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    """
    Generate Section 6 (Debate Preparation) independently.

    Makes its own API call to Claude with a focused prompt that only
    generates floor statements and talking points. Always generates
    fresh content (never cached) to ensure variety.

    Args:
        bill_number: Bill identifier (e.g., "S1297")
        bill_title: Bill title for context
        bill_text: Full or truncated bill text
        one_paragraph_summary: Summary from Section 2 (if available)
        key_points: Key points from Section 3 (if available)
        fiscal_note_text: Fiscal note text if available
        stack: Stack identifier for prompt selection
        max_retries: Number of validation retries (default 2)

    Returns:
        Tuple of (result_dict, error_message, token_usage)
        result_dict contains floor_statement_pro/con and talking_points_for/against
    """
    client = get_client()
    if not client:
        return None, "ANTHROPIC_API_KEY not configured", {}

    # Load the Section 6 prompt
    try:
        system_prompt = load_prompt("section6_debate", stack)
    except FileNotFoundError as e:
        return None, f"Prompt file not found: {e}", {}

    # Build user message with bill context
    key_points_text = ""
    if key_points:
        key_points_text = "\n".join(f"- {p}" for p in key_points[:7])

    parts = [f"Generate floor debate materials for this Idaho bill.\n"]
    parts.append(f"BILL NUMBER: {bill_number}")
    parts.append(f"BILL TITLE: {bill_title}")

    if one_paragraph_summary:
        parts.append(f"\nSUMMARY:\n{one_paragraph_summary}")

    if key_points_text:
        parts.append(f"\nKEY POINTS:\n{key_points_text}")

    if fiscal_note_text and fiscal_note_text.strip():
        parts.append(f"\nFISCAL NOTE:\n{fiscal_note_text[:3000]}")

    if bill_text and bill_text.strip():
        # Include bill text for specificity (truncated to manage tokens)
        parts.append(f"\nBILL TEXT:\n{bill_text[:8000]}")

    parts.append("\nReturn ONLY valid JSON with the four required keys.")

    user_message = "\n".join(parts)

    total_token_usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "model": ANTHROPIC_MODEL,
        "provider": "anthropic",
        "call_type": "section_6",
    }

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            response = _call_with_retry(
                client=client,
                model=ANTHROPIC_MODEL,
                max_tokens=SECTION6_MAX_TOKENS,
                messages=[{"role": "user", "content": user_message}],
                system=system_prompt,
            )

            # Track tokens
            if hasattr(response, "usage") and response.usage:
                total_token_usage["input_tokens"] += response.usage.input_tokens
                total_token_usage["output_tokens"] += response.usage.output_tokens

            raw = response.content[0].text.strip()

            # Extract JSON from potential markdown wrapping
            if "```json" in raw:
                raw = raw.split("```json")[1].split("```")[0].strip()
            elif "```" in raw:
                raw = raw.split("```")[1].split("```")[0].strip()

            result = json.loads(raw)

            # Validate
            is_valid, error_msg = validate_section_6(result)
            if is_valid:
                logger.info(
                    f"[{bill_number}] Section 6 generated successfully "
                    f"(attempt {attempt + 1}, "
                    f"pro={len(result['floor_statement_pro'].split())}w, "
                    f"con={len(result['floor_statement_con'].split())}w, "
                    f"tp_for={len(result['talking_points_for'])}, "
                    f"tp_against={len(result['talking_points_against'])}, "
                    f"tokens: {total_token_usage['input_tokens']}+{total_token_usage['output_tokens']})"
                )
                return result, None, total_token_usage

            last_error = error_msg
            if attempt < max_retries:
                logger.warning(
                    f"[{bill_number}] Section 6 validation failed "
                    f"(attempt {attempt + 1}): {error_msg}, retrying..."
                )
                # Modify user message to include feedback
                user_message = (
                    f"Your previous response failed validation: {error_msg}\n\n"
                    f"Please try again. Requirements:\n"
                    f"- floor_statement_pro: 2-3 paragraphs, 150-225 words\n"
                    f"- floor_statement_con: 2-3 paragraphs, 150-225 words\n"
                    f"- talking_points_for: 3-5 complete persuasive sentences\n"
                    f"- talking_points_against: 3-5 complete persuasive sentences\n\n"
                    f"BILL: {bill_number} — {bill_title}\n"
                )
                if one_paragraph_summary:
                    user_message += f"\nSUMMARY:\n{one_paragraph_summary}\n"
                if key_points_text:
                    user_message += f"\nKEY POINTS:\n{key_points_text}\n"
                if bill_text:
                    user_message += f"\nBILL TEXT:\n{bill_text[:8000]}\n"
                user_message += "\nReturn ONLY valid JSON."
                continue

        except json.JSONDecodeError as e:
            last_error = f"JSON parse error: {e}"
            if attempt < max_retries:
                logger.warning(
                    f"[{bill_number}] Section 6 JSON parse error "
                    f"(attempt {attempt + 1}): {e}, retrying..."
                )
                continue

        except Exception as e:
            return None, f"API error: {type(e).__name__}: {e}", total_token_usage

    return (
        None,
        f"Section 6 failed after {max_retries + 1} attempts: {last_error}",
        total_token_usage,
    )
