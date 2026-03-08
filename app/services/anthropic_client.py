"""
anthropic_client.py
Anthropic Claude API wrapper for Idaho Bill Briefer.
Primary AI provider for briefing generation.

Implements:
- Clear, factual bill analysis
- Monolithic briefing generation (Sections 2-5, 8)
"""
import os
import json
import logging
import time
import random
from typing import Any, Dict, Optional, Tuple

import anthropic

logger = logging.getLogger(__name__)

from app.services.domain_data import _build_domain_context, _extract_subjects, SUBJECT_TO_DOMAIN, DOMAIN_GUIDANCE
from app.services.prompt_builder import get_system_prompt

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514").strip()
ANTHROPIC_MAX_TOKENS = int(os.getenv("ANTHROPIC_MAX_TOKENS", "10000"))  # Increased for detailed sourcing

# ═══════════════════════════════════════════════════════════════════════════════
# DOMAIN-SPECIFIC POLICY GUIDANCE
# Maps LegiScan subjects to policy domains with Idaho-specific analysis guidance
# ═══════════════════════════════════════════════════════════════════════════════


def get_client() -> Optional[anthropic.Anthropic]:
    """Get Anthropic client if API key is configured."""
    if not ANTHROPIC_API_KEY:
        return None
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _call_with_retry(
    client: anthropic.Anthropic,
    model: str,
    max_tokens: int,
    messages: list,
    system: Optional[str] = None,
    temperature: float = 0.7,
    max_retries: int = 3,
):
    """
    Call Anthropic API with exponential backoff retry for transient errors.

    Retries on:
    - 500 Internal Server Error
    - 502 Bad Gateway
    - 503 Service Unavailable
    - 529 Overloaded

    Args:
        client: Anthropic client instance
        model: Model to use
        max_tokens: Maximum tokens in response
        messages: List of message dicts
        system: Optional system prompt
        max_retries: Maximum retry attempts (default 3)

    Returns:
        Anthropic Message response

    Raises:
        anthropic.APIError: If all retries exhausted or non-retryable error
    """
    retryable_status_codes = {500, 502, 503, 529}

    for attempt in range(max_retries):
        try:
            kwargs = {
                "model": model,
                "max_tokens": max_tokens,
                "messages": messages,
                "temperature": temperature,
            }
            if system:
                kwargs["system"] = system

            return client.messages.create(**kwargs)

        except anthropic.APIStatusError as e:
            is_retryable = e.status_code in retryable_status_codes
            is_last_attempt = attempt >= max_retries - 1

            if is_retryable and not is_last_attempt:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    f"API error {e.status_code}, retrying in {wait_time:.1f}s "
                    f"(attempt {attempt + 1}/{max_retries}): {e.message}"
                )
                time.sleep(wait_time)
                continue

            # Non-retryable or exhausted retries
            raise

        except anthropic.APIConnectionError as e:
            # Network errors are retryable
            is_last_attempt = attempt >= max_retries - 1

            if not is_last_attempt:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    f"API connection error, retrying in {wait_time:.1f}s "
                    f"(attempt {attempt + 1}/{max_retries}): {e}"
                )
                time.sleep(wait_time)
                continue

            raise



def generate_briefing(
    bill_number: str,
    bill_text: str,
    legiscan_bill: Dict[str, Any],
    census_context: Optional[Dict[str, Any]] = None,
    fiscal_note_text: str = "",
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Generate a complete bill briefing using Claude.

    Args:
        bill_number: Bill identifier (e.g., "H0416", "S1212")
        bill_text: Full or truncated bill text
        legiscan_bill: Raw LegiScan API response with bill metadata
        census_context: Optional dict with 'enabled', 'content', 'source'
        fiscal_note_text: Extracted text from Statement of Purpose/Fiscal Note

    Returns:
        Tuple of (parsed_json_dict, error_message)
        On success: (dict, None)
        On failure: (None, error_string)
    """
    client = get_client()
    if not client:
        return None, "Anthropic API key not configured", {}

    # Extract subjects for domain-specific analysis guidance
    subjects = _extract_subjects(legiscan_bill)
    logger.info(f"[{bill_number}] Extracted subjects for domain guidance: {subjects}")

    system_prompt = get_system_prompt(census_context, fiscal_note_text, subjects=subjects, tenant_id=os.getenv('TENANT_ID', 'base'))

    # Build user message with bill data
    user_payload = {
        "bill_number": bill_number,
        "bill_text": bill_text,
        "bill_metadata": {
            "title": legiscan_bill.get("title", ""),
            "description": legiscan_bill.get("description", ""),
            "state": legiscan_bill.get("state", "ID"),
            "session": legiscan_bill.get("session", {}),
            "sponsors": legiscan_bill.get("sponsors", []),
            "subjects": subjects,  # Include subjects for AI context
            "history": legiscan_bill.get("history", [])[:20],  # Limit history items
            "status": legiscan_bill.get("status", ""),
            "last_action": legiscan_bill.get("last_action", ""),
            "last_action_date": legiscan_bill.get("last_action_date", ""),
        }
    }

    # Note if SOP/FN is available
    sop_note = ""
    if fiscal_note_text and fiscal_note_text.strip():
        sop_note = """
NOTE: A Statement of Purpose/Fiscal Note has been provided in the system context.
- Integrate the sponsor's stated purpose and fiscal impacts into Section 2
- Flag any discrepancies between the SOP and the actual bill text
"""

    user_message = f"""Analyze this Idaho bill and generate a complete briefing.


{sop_note}
BILL NUMBER: {bill_number}

BILL METADATA:
{json.dumps(user_payload['bill_metadata'], indent=2)}

BILL TEXT:
{bill_text}

Generate the briefing JSON with:
1. Source citations for all factual claims
2. Clear distinction between facts and analysis
3. Clear, factual analysis
4. Clear, readable prose

NOTE: Do NOT generate questions_to_ask. Section 7 (Key Questions) is generated separately.
Do NOT generate floor_statement_pro, floor_statement_con, talking_points_for, or talking_points_against. Section 6 (Debate Preparation) is generated separately.

For potential_impacts (Section 5 - Policy Considerations):
- Generate 3-5 items per category (pros, cons). Do not exceed 5 per category.
- Generate 4-6 unknowns. Each must cite a specific section/provision of the bill.
- Each item must make a distinct point. Do not pad with restated arguments.

For potential_impacts.unknowns (Section 5 - Key Unknowns):
- Every item must point to something MISSING or UNCLEAR in the bill text
- Cite the specific section or provision where the gap exists
- Frame as drafting observations: undefined terms, missing standards, ambiguous thresholds
- Do NOT include predictions about what might happen after implementation (that is Section 8)

CRITICAL for risk_flags (Section 8 - Uncertainties to Watch):
- Generate 4-5 risk_flags items. 5 is the hard ceiling — keep only the strongest
- Every item must describe something that could happen IN THE REAL WORLD after the bill takes effect
- Focus on: enforcement challenges, unintended consequences, behavioral responses, interplay with existing law, timing risks
- DEDUPLICATION: If Section 5 already identifies a text gap, Section 8 must NOT restate that gap. Instead, describe the downstream behavioral consequence. Example: if Section 5 says "does not define X," Section 8 should NOT say "lack of definition creates uncertainty" — instead say "entities may exploit the ambiguity by [specific behavior]"
- Every risk_flag should be a specific, actionable concern — not a vague worry"""

    try:
        logger.info(f"Calling Anthropic API for {bill_number} using {ANTHROPIC_MODEL}")

        message = _call_with_retry(
            client=client,
            model=ANTHROPIC_MODEL,
            max_tokens=ANTHROPIC_MAX_TOKENS,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_message}
            ]
        )

        # Extract response text
        response_text = message.content[0].text

        # Parse JSON response
        # Handle potential markdown code blocks
        if response_text.startswith("```"):
            # Extract JSON from code block
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

        # Add the standard disclaimer to the response
        parsed["disclaimer"] = {
            "title": "ABOUT THIS BRIEFER",
            "content": f"""This analysis was created using Claude AI (Anthropic) from official legislative documents and demographic data. The AI synthesizes bill language, fiscal notes, and district data to provide context for legislative decision-making.

All factual claims are sourced from:
• Bill text: Idaho Legislature (legislature.idaho.gov)
• Fiscal analysis: Idaho Legislative Services fiscal notes (when available)
• District data: US Census Bureau American Community Survey (ACS) 2023
• Idaho statutes: Idaho State Legislature

Analytical sections contain AI-generated insights based on these facts. Please review carefully and verify any information you plan to cite publicly."""
        }

        # Log token usage
        token_usage = {}
        if hasattr(message, "usage") and message.usage:
            token_usage = {
                "input_tokens": message.usage.input_tokens,
                "output_tokens": message.usage.output_tokens,
                "model": ANTHROPIC_MODEL,
                "provider": "anthropic",
            }
            logger.info(
                f"[{bill_number}] Anthropic tokens: "
                f"input={message.usage.input_tokens}, output={message.usage.output_tokens}, "
                f"model={ANTHROPIC_MODEL}"
            )

        logger.info(f"Anthropic API success for {bill_number}")
        return parsed, None, token_usage

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse Anthropic response as JSON: {e}"
        logger.error(error_msg)
        return None, error_msg, {}

    except anthropic.APIError as e:
        error_msg = f"Anthropic API error: {e}"
        logger.error(error_msg)
        return None, error_msg, {}

    except Exception as e:
        error_msg = f"Unexpected error calling Anthropic: {e}"
        logger.exception(error_msg)
        return None, error_msg, {}
