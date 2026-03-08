"""
tenant_config.py
Simple tenant configuration loader for multi-tenant Bill Briefer.
Reads prompts/{TENANT_ID}/tenant.json, caches in memory.
Returns defaults for "base" tenant.
"""
import json
import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

_cache: Dict[str, Dict[str, Any]] = {}

BASE_DIR = os.path.dirname(__file__)

DEFAULTS = {
    "tenant_id": "base",
    "org_name": "Idaho Bill Briefer",
    "org_full_name": "Idaho Bill Briefer",
    "tagline": "Informed legislators craft better policy. Better policy serves everyone.",
    "header_format": "IDAHO BILL BRIEFER — {session_year} SESSION",
    "footer_line": "Idaho Bill Briefer — from Quiet Impact • info@billbriefer.com",
    "disclaimer_audience": "legislators",
}


def get_tenant_config(tenant_id: str = None) -> Dict[str, Any]:
    """Load tenant config from prompts/{tenant_id}/tenant.json. Cached after first load."""
    if tenant_id is None:
        tenant_id = os.getenv("TENANT_ID", "base")
    if tenant_id in _cache:
        return _cache[tenant_id]
    if tenant_id == "base":
        _cache[tenant_id] = DEFAULTS.copy()
        return _cache[tenant_id]
    path = os.path.join(BASE_DIR, "prompts", tenant_id, "tenant.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            data = json.load(f)
        config = {**DEFAULTS, **data}
        _cache[tenant_id] = config
        logger.info(f"Loaded tenant config for {tenant_id}")
        return config
    logger.warning(f"No tenant.json for {tenant_id}, using defaults")
    _cache[tenant_id] = DEFAULTS.copy()
    return _cache[tenant_id]
