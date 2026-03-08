"""
anthropic_client.py
Anthropic Claude API wrapper for Dispatch leadership briefer.

Generates AI sections: Bill Summary (S1), Sponsor Profile (S2), Momentum (S5).
Modules handle Unintended Consequences (S3) and Power Flag (S4) separately.
"""
import os
import json
import logging
import time
import random
from typing import Any, Dict, Optional, Tuple

import anthropic

logger = logging.getLogger(__name__)

from app.services.prompt_builder import get_system_prompt

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514").strip()
ANTHROPIC_MAX_TOKENS = int(os.getenv("ANTHROPIC_MAX_TOKENS", "4000"))


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
    """Call Anthropic API with exponential backoff retry for transient errors."""
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
            raise

        except anthropic.APIConnectionError as e:
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
    fiscal_note_text: str = "",
    sponsor_context: str = "",
    momentum_context: str = "",
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict]:
    """
    Generate Dispatch briefing (Sections 1, 2, 5) using Claude.

    Args:
        bill_number: Bill identifier (e.g., "H0416")
        bill_text: Full or truncated bill text
        legiscan_bill: Bill metadata dict
        fiscal_note_text: Statement of Purpose / Fiscal Note
        sponsor_context: Pre-formatted sponsor data string
        momentum_context: Pre-formatted momentum data string

    Returns:
        Tuple of (parsed_json_dict, error_message, token_usage)
    """
    client = get_client()
    if not client:
        return None, "Anthropic API key not configured", {}

    system_prompt = get_system_prompt(
        fiscal_note_text=fiscal_note_text,
        sponsor_context=sponsor_context,
        momentum_context=momentum_context,
    )

    # Build concise metadata
    metadata = {
        "title": legiscan_bill.get("title", ""),
        "description": legiscan_bill.get("description", ""),
        "sponsors": legiscan_bill.get("sponsors", []),
        "status": legiscan_bill.get("status", ""),
        "last_action": legiscan_bill.get("last_action", ""),
        "last_action_date": legiscan_bill.get("last_action_date", ""),
    }

    user_message = f"""Analyze this Idaho bill and generate the Dispatch briefing JSON.

BILL NUMBER: {bill_number}

BILL METADATA:
{json.dumps(metadata, indent=2)}

BILL TEXT:
{bill_text}

Generate JSON with exactly these keys:
- bill_summary (string): 2-3 sentence orientor
- sponsor_profile (object): name, chamber, district, bills_this_session, iaci_scores, narrative
- momentum (object): trajectory, days_since_introduction, hearing_status, narrative"""

    try:
        logger.info(f"[{bill_number}] Calling Anthropic for Dispatch briefing ({ANTHROPIC_MODEL})")

        message = _call_with_retry(
            client=client,
            model=ANTHROPIC_MODEL,
            max_tokens=ANTHROPIC_MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        response_text = message.content[0].text

        # Strip markdown code fences if present
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
                f"input={message.usage.input_tokens}, output={message.usage.output_tokens}"
            )

        logger.info(f"[{bill_number}] Dispatch briefing generated successfully")
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
