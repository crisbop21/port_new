# IBKR Trade Journal

## What this is
Personal trading journal built in Python and Streamlit. Parses IBKR
Custom Date Range PDF statements, stores holdings and trade history
in Supabase, deployed on Streamlit Community Cloud from this repo.

## Architecture
Three-layer architecture: Presentation (pages/), Service (src/parser.py,
src/models.py), Data (src/db.py). Layers only communicate downward.
Never import from a layer above.

## Stack
- pdfplumber for PDF parsing
- pandas for data wrangling
- supabase-py for database
- streamlit for UI
- pydantic for schema validation

## How to run locally
pip install -r requirements.txt
cp .env.example .env  # fill in Supabase credentials
streamlit run app.py

## How to verify changes
streamlit run app.py and manually test the affected page.
No test runner yet — tests live in tests/ and run with pytest.

## Constraints (non-negotiable)
- Free tiers only — no paid APIs in Phase 1
- No Anthropic API until Phase 3
- No hardcoded credentials anywhere
- All Supabase queries must use st.cache_data or st.cache_resource
- Errors must surface via st.error, never swallowed silently
- Financial figures must never be silently mutated

## Asset classes in scope
Stocks, ETFs, and Options only. Skip and log everything else.

## Definition of done for any feature
- Tested against a real IBKR PDF
- requirements.txt updated and version-pinned
- No secrets in code
- Error states visible to user
