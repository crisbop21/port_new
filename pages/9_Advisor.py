"""Options Advisor — AI-powered strategy recommendations using Claude API."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from src.context import build_position_context, serialize_context
from src.advisor import ask_advisor
from src.db import get_account_ids, get_portfolio_symbols

st.title("Options Advisor")

# ── API key check ───────────────────────────────────────────────────────────

api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not api_key:
    try:
        api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
    except (KeyError, FileNotFoundError):
        pass

if not api_key:
    st.error(
        "Missing **ANTHROPIC_API_KEY**.\n\n"
        "**Local:** add it to your `.env` file.\n\n"
        "**Streamlit Cloud:** add it in App settings → Secrets."
    )
    st.stop()

# ── Account selector ────────────────────────────────────────────────────────

account_ids = get_account_ids()
if not account_ids:
    st.info("No statements uploaded yet.")
    st.page_link("pages/1_Upload.py", label="Go to Upload", icon="📤")
    st.stop()

with st.sidebar:
    account_options = ["All Accounts"] + account_ids
    selected_account = st.selectbox("Account", account_options, key="advisor_account")
    account_filter = None if selected_account == "All Accounts" else selected_account

# ── Volatility overrides ────────────────────────────────────────────────────

# Get symbols that have options positions
symbols = get_portfolio_symbols(account_id=account_filter)

with st.expander("Volatility Overrides (optional)"):
    st.caption(
        "Override the realized volatility with your own implied volatility "
        "estimate for any underlying. Leave blank to use computed realized vol."
    )

    if "vol_overrides" not in st.session_state:
        st.session_state.vol_overrides = {}

    # Show input for each symbol
    if symbols:
        cols = st.columns(min(len(symbols), 4))
        for i, sym in enumerate(symbols):
            with cols[i % len(cols)]:
                current_override = st.session_state.vol_overrides.get(sym)
                val = st.number_input(
                    f"{sym} IV %",
                    min_value=0.0,
                    max_value=500.0,
                    value=float(current_override * 100) if current_override else 0.0,
                    step=1.0,
                    key=f"vol_{sym}",
                    help=f"Enter implied volatility for {sym} as a percentage (e.g., 35 for 35%)",
                )
                if val > 0:
                    st.session_state.vol_overrides[sym] = val / 100
                elif sym in st.session_state.vol_overrides:
                    del st.session_state.vol_overrides[sym]
    else:
        st.info("No stock/ETF symbols found. Upload a statement with holdings first.")

    if st.session_state.vol_overrides:
        st.caption(
            "Active overrides: "
            + ", ".join(
                f"**{s}**: {v:.0%}" for s, v in st.session_state.vol_overrides.items()
            )
        )

# ── Build context ───────────────────────────────────────────────────────────

if "advisor_context" not in st.session_state or st.button("Refresh Context"):
    if account_filter:
        accounts_to_process = [account_filter]
    else:
        accounts_to_process = account_ids

    with st.spinner("Assembling portfolio context..."):
        all_contexts = []
        for acct in accounts_to_process:
            ctx = build_position_context(
                acct, vol_overrides=st.session_state.vol_overrides
            )
            if ctx["positions"]:
                all_contexts.append(ctx)

        if all_contexts:
            # Merge contexts if multiple accounts
            if len(all_contexts) == 1:
                merged = all_contexts[0]
            else:
                merged = {
                    "account_id": "All Accounts",
                    "as_of_date": all_contexts[0]["as_of_date"],
                    "positions": [],
                    "underlyings": {},
                }
                for ctx in all_contexts:
                    merged["positions"].extend(ctx["positions"])
                    merged["underlyings"].update(ctx["underlyings"])

            st.session_state.advisor_context = merged
            st.session_state.advisor_context_str = serialize_context(merged)
        else:
            st.session_state.advisor_context = None
            st.session_state.advisor_context_str = None

# Show context preview
if st.session_state.get("advisor_context_str"):
    with st.expander("View portfolio context sent to advisor"):
        st.markdown(st.session_state.advisor_context_str)
elif st.session_state.get("advisor_context") is None:
    st.warning("No positions found. Upload statements and fetch prices first.")
    st.page_link("pages/1_Upload.py", label="Go to Upload", icon="📤")
    st.page_link("pages/6_Prices.py", label="Fetch prices", icon="📈")
    st.stop()

# ── Chat interface ──────────────────────────────────────────────────────────

if "advisor_messages" not in st.session_state:
    st.session_state.advisor_messages = []

# Display conversation history
for msg in st.session_state.advisor_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Suggested prompts for first message
if not st.session_state.advisor_messages:
    st.markdown("**Suggested questions:**")
    suggestions = [
        "Review my options positions and suggest exit strategies for each.",
        "Which positions have the highest risk right now?",
        "What covered call or cash-secured put strategies fit my portfolio?",
        "Analyse my portfolio's overall options exposure and risk profile.",
    ]
    cols = st.columns(2)
    for i, suggestion in enumerate(suggestions):
        with cols[i % 2]:
            if st.button(suggestion, key=f"suggest_{i}"):
                st.session_state._pending_question = suggestion
                st.rerun()

# Handle pending question from button click
pending = st.session_state.pop("_pending_question", None)

# Chat input
question = st.chat_input("Ask about your options positions...")

# Use pending question if no typed question
if pending and not question:
    question = pending

if question and st.session_state.get("advisor_context_str"):
    # Show user message
    st.session_state.advisor_messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Build history for API (exclude current question)
    history = st.session_state.advisor_messages[:-1]

    # Call Claude
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                response = ask_advisor(
                    context=st.session_state.advisor_context_str,
                    question=question,
                    history=history,
                    api_key=api_key,
                )
                st.markdown(response)
                st.session_state.advisor_messages.append(
                    {"role": "assistant", "content": response}
                )
            except Exception as e:
                st.error(f"Advisor error: {e}")

# Clear conversation button
if st.session_state.advisor_messages:
    if st.button("Clear Conversation"):
        st.session_state.advisor_messages = []
        st.rerun()
