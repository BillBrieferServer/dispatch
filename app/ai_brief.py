"""
ai_brief.py

AI-written analysis sections for Idaho Bill Briefer.

Primary: Anthropic Claude Sonnet 4
Fallback: OpenAI GPT

Outputs (structured):
- one_paragraph_summary
- key_points
- who_it_affects
- potential_impacts (pros/cons/unknowns)
- questions_to_ask (with sample answers)
- risk_flags
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional, Tuple

from openai import OpenAI

# Import Anthropic client
try:
    from app.services.anthropic_client import generate_briefing as anthropic_generate
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    anthropic_generate = None

# Import modular Section 7 generator
try:
    from app.sections.section7 import generate_section_7
    SECTION7_AVAILABLE = True
    from app.sections.section6 import generate_section_6
    SECTION6_AVAILABLE = True
except ImportError:
    SECTION7_AVAILABLE = False
    generate_section_7 = None
    SECTION6_AVAILABLE = False
    generate_section_6 = None

# Import AI cache
try:
    from app.ai_cache import (
        get_cached_briefing,
        cache_briefing,
        init_ai_cache_db,
    )
    AI_CACHE_AVAILABLE = True
except ImportError:
    AI_CACHE_AVAILABLE = False
    get_cached_briefing = None
    cache_briefing = None
    init_ai_cache_db = None

logger = logging.getLogger(__name__)

# Cache version — bump when modular architecture changes to force regeneration

# Configuration
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()
OPENAI_VERBOSITY = os.getenv("OPENAI_VERBOSITY", "low").strip()
OPENAI_USE_WEB_SEARCH = os.getenv("OPENAI_USE_WEB_SEARCH", "false").strip().lower() in {"1", "true", "yes", "y"}
MAX_BILL_TEXT_CHARS = int(os.getenv("MAX_BILL_TEXT_CHARS", "30000"))

# AI Provider selection
AI_PROVIDER = os.getenv("AI_PROVIDER", "anthropic").strip().lower()  # "anthropic" or "openai"
AI_FALLBACK_ENABLED = os.getenv("AI_FALLBACK_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y"}


def _truncate(text: str, max_chars: int) -> str:
    text = text or ""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1000] + "\n\n[TRUNCATED]\n\n" + text[-900:]


def _schema() -> Dict[str, Any]:
    """JSON schema for AI output validation."""
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "one_paragraph_summary": {"type": "string"},
            "key_points": {"type": "array", "items": {"type": "string"}},
            "who_it_affects": {"type": "array", "items": {"type": "string"}},
            "potential_impacts": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "pros": {"type": "array", "items": {"type": "string"}},
                    "cons": {"type": "array", "items": {"type": "string"}},
                    "unknowns": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["pros", "cons", "unknowns"],
            },
            "questions_to_ask": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "question": {"type": "string"},
                        "sample_answers": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["question", "sample_answers"],
                },
            },
            "risk_flags": {"type": "array", "items": {"type": "string"}},
            "budget_extracted": {
                "type": "object",
                "properties": {
                    "is_appropriation": {"type": "boolean"},
                },
                "required": ["is_appropriation"],
            },
        },
        "required": [
            "one_paragraph_summary",
            "key_points",
            "who_it_affects",
            "potential_impacts",
            "questions_to_ask",
            "risk_flags",
            "budget_extracted",
        ],
    }


def _build_openai_brief(
    bill_number: str,
    legiscan_bill: Dict[str, Any],
    bill_text: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Generate briefing using OpenAI (fallback provider)."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None, "OPENAI_API_KEY not set", {}

    client = OpenAI(api_key=api_key)

    system_prompt = (
        "You are Idaho Bill Briefer, a NONPARTISAN legislative analyst.\n"
        "Explain what the bill does in plain English for a busy elected official.\n"
        "Do NOT write persuasive advocacy. Do NOT recommend political strategy.\n"
        "Write balanced content: what supporters may argue vs what critics may argue.\n"
        "If bill text is missing, do NOT invent details—use conditional phrasing and say verification is needed.\n"
        "\n"
        "OUTPUT REQUIREMENTS:\n"
        "- Provide 6–8 'questions_to_ask', each with 2–3 'sample_answers' that are neutral/realistic.\n"
        "- Do NOT generate floor_statement_pro, floor_statement_con, talking_points_for, or talking_points_against. Section 6 is generated separately.\n"
        "- Output MUST match the JSON schema exactly.\n"
        "\n"
        "BUDGET EXTRACTION:\n"
        "- Determine if this is an appropriation/funding bill (is_appropriation: true/false).\n"
        "- If YES: extract agency_name, total_appropriation, general_fund, dedicated_fund, federal_fund, ftp, fiscal_year, yoy_change_dollars, yoy_change_percent from bill text or fiscal note.\n"
        "- Set confidence to 'high' if numbers are clear, 'medium' if interpretation needed, 'low' if unclear.\n"
        "- If NOT an appropriation bill: set is_appropriation to false, confidence to 'high', all other budget fields to null.\n"
        "\n"
        "FISCAL ANALYSIS (for appropriation bills only):\n"
        "- funding_type: Is this 'one-time' (single year), 'ongoing' (continuing/recurring), or 'mixed'?\n"
        "- funding_nature: Is this 'enhancement' (new programs/expansion), 'maintenance' (continuing existing operations), or 'both'?\n"
        "- fiscal_implications: Write 2-3 sentences explaining the fiscal significance. Consider: What ongoing obligations does this create? What happens if federal funds change? Is this sustainable long-term?"
    )

    user_payload = {
        "bill_number": bill_number,
        "bill_text_truncated": bill_text,
        "legiscan_bill_raw": legiscan_bill,
    }

    tools = []
    if OPENAI_USE_WEB_SEARCH:
        tools = [{"type": "web_search"}]

    try:
        resp = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            tools=tools,
            store=False,
            text={
                "verbosity": OPENAI_VERBOSITY,
                "format": {
                    "type": "json_schema",
                    "name": "idaho_bill_briefer_ai_sections",
                    "strict": True,
                    "schema": _schema(),
                },
            },
        )

        raw = (resp.output_text or "").strip()
        if not raw:
            return None, "OpenAI returned empty output", {}

        # Capture OpenAI token usage
        oai_token_usage = {}
        if hasattr(resp, "usage") and resp.usage:
            oai_token_usage = {
                "input_tokens": getattr(resp.usage, "input_tokens", 0),
                "output_tokens": getattr(resp.usage, "output_tokens", 0),
                "model": OPENAI_MODEL,
                "provider": "openai",
            }
            logger.info(
                f"OpenAI tokens: input={oai_token_usage['input_tokens']}, "
                f"output={oai_token_usage['output_tokens']}, model={OPENAI_MODEL}"
            )

        return json.loads(raw), None, oai_token_usage

    except json.JSONDecodeError:
        return None, "OpenAI returned non-JSON output"
    except Exception as e:
        return None, f"OpenAI error: {type(e).__name__}: {e}", {}


def build_ai_brief(
    *,
    bill_number: str,
    legiscan_bill: Dict[str, Any],
    bill_text: str,
    census_context: Optional[Dict[str, Any]] = None,
    fiscal_note_text: str = "",
    bill_id: int = None,
    session_id: int = None,
    bill_change_hash: str = None,
    use_cache: bool = True,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], str, bool]:
    """
    Generate AI analysis for a bill.

    Args:
        bill_number: Bill identifier (e.g., "H0416")
        legiscan_bill: Raw LegiScan API response
        bill_text: Full or truncated bill text
        census_context: Optional demographic context dict
        fiscal_note_text: Extracted text from Statement of Purpose/Fiscal Note
        bill_id: LegiScan bill ID for caching
        session_id: LegiScan session ID for caching
        bill_change_hash: Bill change hash for cache invalidation
        use_cache: Whether to check/use cache (default True)

    Returns:
        Tuple of (parsed_json, error_message, model_used, was_invalidated)
        - On success: (dict, None, "anthropic" or "openai", bool)
        - On failure: (None, error_string, "none", False)
        - was_invalidated: True if cache existed but was stale (bill changed)
    """
    bill_text = _truncate(bill_text or "", MAX_BILL_TEXT_CHARS)

    was_invalidated = False

    # Check cache first (if enabled and bill_id provided)
    if use_cache and AI_CACHE_AVAILABLE and bill_id:
        cached = get_cached_briefing(bill_id, bill_change_hash)
        if cached:
            logger.info(f"[{bill_number}] AI CACHE HIT - using cached sections 2-5, 8 for bill_id={bill_id}")
            cached_json = cached["ai_json"]

        if cached:

            # Generate FRESH Section 6 (floor statements and talking points)
            s6_token_usage = {}
            if SECTION6_AVAILABLE:
                bill_title = legiscan_bill.get("bill", {}).get("title", "") if legiscan_bill else ""
                s6_result, s6_error, s6_tokens = generate_section_6(
                    bill_number=bill_number,
                    bill_title=bill_title,
                    one_paragraph_summary=cached_json.get("one_paragraph_summary", ""),
                    key_points=cached_json.get("key_points", []),
                )
                if s6_result:
                    cached_json["floor_statement_pro"] = s6_result["floor_statement_pro"]
                    cached_json["floor_statement_con"] = s6_result["floor_statement_con"]
                    cached_json["talking_points_for"] = s6_result["talking_points_for"]
                    cached_json["talking_points_against"] = s6_result["talking_points_against"]
                    s6_token_usage = s6_tokens
                    logger.info(f"[{bill_number}] Generated fresh Section 6 (modular)")
                else:
                    logger.warning(f"[{bill_number}] Section 6 generation failed: {s6_error}, using cached")

            return cached_json, None, "cached+fresh_s6", False, s6_token_usage
        else:
            # Cache miss - check if it was an invalidation (entry exists but hash mismatched)
            stale = get_cached_briefing(bill_id, None)
            if stale:
                was_invalidated = True
                logger.info(f"[{bill_number}] Cache INVALIDATED - bill changed since last briefer")

    # Determine which provider to try first
    use_anthropic_first = AI_PROVIDER == "anthropic" and ANTHROPIC_AVAILABLE

    if use_anthropic_first:
        # Try Anthropic first
        logger.info(f"[{bill_number}] Trying Anthropic Claude as primary AI provider")
        result, error, token_usage = anthropic_generate(
            bill_number=bill_number,
            bill_text=bill_text,
            legiscan_bill=legiscan_bill,
            census_context=census_context,
            fiscal_note_text=fiscal_note_text,
        )

        if result is not None:
            logger.info(f"[{bill_number}] Anthropic Claude succeeded")

            # Generate Section 6 + Section 7 in PARALLEL
            bill_title = ""
            if legiscan_bill:
                bill_title = legiscan_bill.get("title", "") or legiscan_bill.get("description", "")
            summary = result.get("one_paragraph_summary", "")
            key_pts = result.get("key_points", [])

            s6_result, s6_error, s6_tokens = None, None, {}
            s7_result, s7_error, s7_tokens = None, None, {}

            if SECTION6_AVAILABLE and SECTION7_AVAILABLE:
                logger.info(f"[{bill_number}] Generating Section 6 + Section 7 in parallel...")
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_s7 = executor.submit(
                        generate_section_7,
                        bill_text=bill_text,
                        bill_number=bill_number,
                        bill_title=bill_title,
                        bill_summary=summary,
                        fiscal_note_text=fiscal_note_text,
                        stack=os.getenv("TENANT_ID", "base"),
                    )
                    future_s6 = executor.submit(
                        generate_section_6,
                        bill_number=bill_number,
                        bill_title=bill_title,
                        bill_text=bill_text,
                        one_paragraph_summary=summary,
                        key_points=key_pts,
                        fiscal_note_text=fiscal_note_text,
                        stack=os.getenv("TENANT_ID", "base"),
                    )
                    s7_result, s7_error, s7_tokens = future_s7.result()
                    s6_result, s6_error, s6_tokens = future_s6.result()
            else:
                # Fallback to sequential if one module is missing
                if SECTION7_AVAILABLE:
                    s7_result, s7_error, s7_tokens = generate_section_7(
                        bill_text=bill_text, bill_number=bill_number,
                        bill_title=bill_title, bill_summary=summary,
                        fiscal_note_text=fiscal_note_text, stack=os.getenv("TENANT_ID", "base"),
                    )
                if SECTION6_AVAILABLE:
                    s6_result, s6_error, s6_tokens = generate_section_6(
                        bill_number=bill_number, bill_title=bill_title,
                        bill_text=bill_text, one_paragraph_summary=summary,
                        key_points=key_pts, fiscal_note_text=fiscal_note_text,
                        stack=os.getenv("TENANT_ID", "base"),
                    )

            # Merge Section 7
            if s7_result:
                result["questions_to_ask"] = s7_result["questions_to_ask"]
                logger.info(f"[{bill_number}] Section 7 merged successfully")
                if s7_tokens:
                    token_usage["section_7_input_tokens"] = s7_tokens.get("input_tokens", 0)
                    token_usage["section_7_output_tokens"] = s7_tokens.get("output_tokens", 0)
            else:
                logger.warning(f"[{bill_number}] Section 7 generation failed: {s7_error}")

            # Merge Section 6
            if s6_result:
                result["floor_statement_pro"] = s6_result["floor_statement_pro"]
                result["floor_statement_con"] = s6_result["floor_statement_con"]
                result["talking_points_for"] = s6_result["talking_points_for"]
                result["talking_points_against"] = s6_result["talking_points_against"]
                logger.info(f"[{bill_number}] Section 6 merged successfully")
                if s6_tokens:
                    token_usage["section_6_input_tokens"] = s6_tokens.get("input_tokens", 0)
                    token_usage["section_6_output_tokens"] = s6_tokens.get("output_tokens", 0)
            else:
                logger.warning(f"[{bill_number}] Section 6 generation failed: {s6_error}")
                result.setdefault("floor_statement_pro", "")
                result.setdefault("floor_statement_con", "")
                result.setdefault("talking_points_for", [])
                result.setdefault("talking_points_against", [])

            # Cache the result
            if AI_CACHE_AVAILABLE and bill_id and session_id:
                try:
                    cache_briefing(
                        bill_id=bill_id,
                        session_id=session_id,
                        bill_number=bill_number,
                        ai_json=result,
                        model_used="anthropic",
                        bill_change_hash=bill_change_hash,
                    )
                    logger.info(f"[{bill_number}] CACHED AI result for bill_id={bill_id}")
                except Exception as e:
                    logger.warning(f"[{bill_number}] Failed to cache AI result: {e}")
            return result, None, "anthropic", was_invalidated, token_usage

        # Anthropic failed - try fallback if enabled
        logger.warning(f"[{bill_number}] Anthropic failed: {error}")

        if AI_FALLBACK_ENABLED:
            logger.info(f"[{bill_number}] Falling back to OpenAI")
            result, error, token_usage = _build_openai_brief(bill_number, legiscan_bill, bill_text)
            if result is not None:
                logger.info(f"[{bill_number}] OpenAI fallback succeeded")
                # Cache the result
                if AI_CACHE_AVAILABLE and bill_id and session_id:
                    try:
                        cache_briefing(
                            bill_id=bill_id,
                            session_id=session_id,
                            bill_number=bill_number,
                            ai_json=result,
                            model_used="openai",
                            bill_change_hash=bill_change_hash,
                        )
                        logger.info(f"[{bill_number}] Cached OpenAI fallback result for bill_id={bill_id}")
                    except Exception as e:
                        logger.warning(f"[{bill_number}] Failed to cache OpenAI result: {e}")
                return result, None, "openai", was_invalidated, token_usage
            logger.error(f"[{bill_number}] OpenAI fallback also failed: {error}")
            return None, f"Both AI providers failed. Anthropic: {error}", "none", False, {}
        else:
            return None, error, "none", False, {}

    else:
        # Use OpenAI as primary (original behavior)
        logger.info(f"[{bill_number}] Using OpenAI as primary AI provider")
        result, error, token_usage = _build_openai_brief(bill_number, legiscan_bill, bill_text)
        if result is not None:
            return result, None, "openai", was_invalidated, token_usage

        # OpenAI failed - try Anthropic fallback if available and enabled
        if AI_FALLBACK_ENABLED and ANTHROPIC_AVAILABLE:
            logger.info(f"[{bill_number}] OpenAI failed, falling back to Anthropic")
            result, error, token_usage = anthropic_generate(
                bill_number=bill_number,
                bill_text=bill_text,
                legiscan_bill=legiscan_bill,
                census_context=census_context,
                fiscal_note_text=fiscal_note_text,
            )
            if result is not None:
                logger.info(f"[{bill_number}] Anthropic fallback succeeded")
                return result, None, "anthropic", was_invalidated, token_usage

        return None, error, "none", False, {}

