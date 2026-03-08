"""
section7.py

Independent module for generating Section 7 (Key Questions) of the Bill Briefer.

This module makes its own API call to Claude, separate from the monolithic
briefing generation. This isolation ensures that prompt changes to Section 7
cannot affect any other section's output quality.
"""
import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

from app.services.anthropic_client import (
    get_client,
    _call_with_retry,
    ANTHROPIC_MODEL,
)

logger = logging.getLogger(__name__)

# Prompt directory lives alongside the app code
PROMPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")

# Max output tokens for Section 7
# 1 set x 2-5 questions x 2 answers x ~150 words ≈ ~3000 tokens
SECTION7_MAX_TOKENS = 4000


def load_prompt(section: str, stack: str = "base") -> str:
    """Load stack-specific prompt if it exists, otherwise use base.

    Args:
        section: Prompt filename without extension (e.g., "section7_questions")
        stack: Stack identifier (base, aic, iac, chamber)

    Returns:
        Prompt text content
    """
    if stack and stack != "base":
        stack_path = os.path.join(PROMPTS_DIR, stack, f"{section}.txt")
        if os.path.exists(stack_path):
            with open(stack_path, "r") as f:
                return f.read()
    base_path = os.path.join(PROMPTS_DIR, "base", f"{section}.txt")
    with open(base_path, "r") as f:
        return f.read()


def validate_section_7(result: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate Section 7 output meets quality requirements.

    Requirements:
    - questions_to_ask must be a list of 2-5 question objects
    - Each question object must have 'question' (20+ chars) and 'sample_answers' (exactly 2, each 80+ chars)

    Returns:
        (is_valid, error_message)
    """
    questions = result.get("questions_to_ask")
    if not isinstance(questions, list):
        return False, "questions_to_ask is not a list"
    if len(questions) < 2 or len(questions) > 5:
        return False, f"Expected 2-5 questions, got {len(questions)}"

    for qi, q_obj in enumerate(questions):
        if not isinstance(q_obj, dict):
            return False, f"Question {qi+1} is not a dict"

        question = q_obj.get("question", "")
        if not isinstance(question, str) or len(question) < 20:
            q_len = len(question) if isinstance(question, str) else 0
            return False, f"Q{qi+1}: question too short ({q_len} chars, need 20+)"

        answers = q_obj.get("sample_answers", [])
        if not isinstance(answers, list) or len(answers) != 2:
            a_count = len(answers) if isinstance(answers, list) else 0
            return False, f"Q{qi+1}: need exactly 2 answers, got {a_count}"

        for ai, answer in enumerate(answers):
            if not isinstance(answer, str) or len(answer) < 80:
                a_len = len(answer) if isinstance(answer, str) else 0
                return False, f"Q{qi+1}, answer {ai+1}: too short ({a_len} chars, need 80+)"

    return True, ""


def generate_section_7(
    bill_text: str,
    bill_number: str,
    bill_title: str = "",
    bill_summary: str = "",
    fiscal_note_text: str = "",
    stack: str = "base",
    max_retries: int = 2,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    """
    Generate Section 7 (Key Questions) independently.

    Makes its own API call to Claude with a focused prompt that only
    generates committee questions. This isolation prevents prompt
    changes from affecting other sections.

    Args:
        bill_text: Full or truncated bill text
        bill_number: Bill identifier (e.g., "S1297")
        bill_title: Bill title for context
        bill_summary: One-paragraph summary if available (from Section 2)
        fiscal_note_text: Statement of Purpose / Fiscal Note text if available
        stack: Stack identifier for prompt selection (base, aic, iac)
        max_retries: Number of validation retries (default 2)

    Returns:
        Tuple of (result_dict, error_message, token_usage)
        result_dict contains "questions_to_ask" on success
    """
    client = get_client()
    if not client:
        return None, "ANTHROPIC_API_KEY not configured", {}

    # Load the Section 7 prompt
    try:
        system_prompt = load_prompt("section7_questions", stack)
    except FileNotFoundError as e:
        return None, f"Prompt file not found: {e}", {}

    # Build user message with bill context
    parts = [f"Analyze this Idaho bill and generate probing committee questions.\n"]
    parts.append(f"BILL NUMBER: {bill_number}")
    if bill_title:
        parts.append(f"BILL TITLE: {bill_title}")
    if bill_summary:
        parts.append(f"\nBILL SUMMARY:\n{bill_summary}")
    if fiscal_note_text and fiscal_note_text.strip():
        parts.append(f"\nSTATEMENT OF PURPOSE / FISCAL NOTE:\n{fiscal_note_text[:4000]}")
    parts.append(f"\nBILL TEXT:\n{bill_text}")
    parts.append("\nGenerate 2 to 5 questions based on bill complexity, following the system prompt instructions.")
    parts.append("Return ONLY valid JSON.")

    user_message = "\n".join(parts)

    total_token_usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "model": ANTHROPIC_MODEL,
        "provider": "anthropic",
        "call_type": "section_7",
    }

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            response = _call_with_retry(
                client=client,
                model=ANTHROPIC_MODEL,
                max_tokens=SECTION7_MAX_TOKENS,
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
            is_valid, error_msg = validate_section_7(result)
            if is_valid:
                logger.info(
                    f"[{bill_number}] Section 7 generated successfully "
                    f"(attempt {attempt + 1}, "
                    f"tokens: {total_token_usage['input_tokens']}+{total_token_usage['output_tokens']})"
                )
                return result, None, total_token_usage

            last_error = error_msg
            if attempt < max_retries:
                logger.warning(
                    f"[{bill_number}] Section 7 validation failed "
                    f"(attempt {attempt + 1}): {error_msg}, retrying..."
                )
                # Modify user message to include feedback
                user_message = (
                    f"Your previous response failed validation: {error_msg}\n\n"
                    f"Please try again with these requirements:\n"
                    f"- 2 to 5 questions (based on bill complexity)\n"
                    f"- Each question: \"question\" string (20+ chars) and "
                    f"\"sample_answers\" array of exactly 2 strings (each 80+ chars)\n"
                    f"- One answer supportive, one skeptical\n\n"
                    f"BILL NUMBER: {bill_number}\n"
                    f"BILL TITLE: {bill_title}\n"
                )
                if bill_summary:
                    user_message += f"\nBILL SUMMARY:\n{bill_summary}\n"
                if fiscal_note_text and fiscal_note_text.strip():
                    user_message += f"\nSTATEMENT OF PURPOSE / FISCAL NOTE:\n{fiscal_note_text[:4000]}\n"
                user_message += f"\nBILL TEXT:\n{bill_text}\n"
                user_message += "\nReturn ONLY valid JSON."
                continue

        except json.JSONDecodeError as e:
            last_error = f"JSON parse error: {e}"
            if attempt < max_retries:
                logger.warning(
                    f"[{bill_number}] Section 7 JSON parse error "
                    f"(attempt {attempt + 1}): {e}, retrying..."
                )
                continue

        except Exception as e:
            return None, f"API error: {type(e).__name__}: {e}", total_token_usage

    return (
        None,
        f"Section 7 failed after {max_retries + 1} attempts: {last_error}",
        total_token_usage,
    )
