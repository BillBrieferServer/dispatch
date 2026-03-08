# Bill Briefer — CLAUDE.md

## What This Is
AI-powered bill analysis tool for Idaho state legislators. Generates 10-section briefing documents (HTML email + PDF) from bill text. FastAPI app, deployed via Docker on prod.

## Project Structure
```
/opt/billbriefer/app/
├── main.py                  # FastAPI app, all routes, scheduler, job queue (3,360 lines)
├── ai_brief.py              # Orchestrates Sections 2-5, 8 via Anthropic Sonnet
├── ai_cache.py              # SQLite cache for AI sections (skips S6/S7/S9)
├── briefer_format.py        # Assembles all sections into HTML email
├── pdf_render.py            # PDF output via ReportLab
├── branding.py              # Stack-aware branding constants
├── bls_data_fetch.py        # BLS economic data (unemployment, wages, CPI)
├── census.py                # Census ACS district demographics
├── district_county_mapping.py # District → county population weights
├── district_analysis.py     # Section 9 District Profile generator
├── legislators.py           # Loads legislators from QIBrain
├── legiscan.py              # LegiScan API (kept as bill text fallback)
├── legiscan_sync.py         # LegiScan bulk sync (scheduler uses this)
├── db_cache.py              # LegiScan payload cache (initialized but largely bypassed)
├── usage_report.py          # PDF usage reports from master log
├── auth/                    # Auth module: SQLite DB, SMTP email, routes, bcrypt/MFA
├── data/
│   ├── city_context.json    # (archived — AIC only)
│   └── statewide_context.json # FY2026 budget context for appropriation bills
├── etl/
│   └── bls_to_qibrain.py   # BLS → QIBrain ETL (misplaced, belongs in /root/quietimpact/etl/)
├── prompts/
│   ├── base/                # Active prompts: S6, S7, S9 (legacy fallback)
│   └── aic/                 # (archived — AIC only)
├── sections/
│   ├── section6.py          # Floor debate prep (independent API call, never cached)
│   ├── section7.py          # Committee questions (independent API call)
│   ├── section9.py          # S9 router: policy → District Profile, appropriation → statewide
│   ├── section9_composer.py # Deterministic S9 pipeline: Haiku classifier → mechanism tags → templates
│   └── mechanism_templates.json # 22 mechanism tags, 20 fact block templates
├── services/
│   ├── anthropic_client.py  # Anthropic API wrapper, system prompts, token tracking
│   ├── content_validator.py # Post-generation Haiku validator (skips composer-generated S9)
│   └── qibrain_data.py      # QIBrain PostgreSQL client (primary data source)
└── _archived/               # Removed from active codebase, kept for reference
```

## Tech Stack
- Python / FastAPI
- Anthropic API (Claude Sonnet for generation, Haiku for classification/validation)
- PostgreSQL via QIBrain (primary data source)
- SQLite (auth DB, AI cache)
- ReportLab (PDF generation)
- Docker deployment
- SMTP via IONOS (auth emails)

## Key Conventions
- QIBrain is the source of truth for bill data, not LegiScan directly
- Section 9 uses the deterministic Composer pipeline (SECTION9_COMPOSER_ENABLED=1), not the legacy LLM path
- Sections 6 and 7 are independent API calls, never cached
- Sections 2-5, 8 are cached per bill via ai_cache.py
- Branding is stack-aware (branding.py) — different branding for different deployment contexts
- Auth uses SQLite at auth/auth.sqlite — use `.backup` command for backups, never file copy

## DO NOT Touch
- legiscan.py / legiscan_sync.py — still wired as fallbacks and scheduler imports
- db_cache.py — initialized at startup in main.py, removing requires main.py refactor
- prompts/base/section9_district.txt — legacy S9 fallback path still reachable in section9.py:633
- _archived/ folder — reference only, not active

## Known Technical Debt
- main.py is 140KB / 3,360 lines — everything in one file
- Dual Section 9 pipelines (Composer + legacy LLM) — Composer is active, legacy is fallback
- etl/bls_to_qibrain.py is misplaced in app container
- db_cache.py is initialized but effectively bypassed since QIBrain took over
