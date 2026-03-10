"""Upload page — parse an IBKR PDF and save to Supabase."""

import streamlit as st

from src.parser import parse_statement
from src.db import upsert_statement, check_duplicates, clear_query_caches, get_client

st.title("Upload IBKR Statement")

uploaded = st.file_uploader(
    "Choose an IBKR Custom Date Range PDF",
    type=["pdf"],
    help="Export from IBKR → Reports → Custom Date Range → PDF",
)

if uploaded is not None:
    # Parse only once per file — store in session_state so the result
    # survives the rerun triggered by the "Save" button click.
    file_key = f"parsed_{uploaded.name}_{uploaded.size}"
    if file_key not in st.session_state:
        with st.spinner("Parsing PDF..."):
            try:
                st.session_state[file_key] = parse_statement(uploaded)
            except Exception as e:
                st.error(f"Failed to parse PDF: {e}")
                st.stop()

    statements = st.session_state.get(file_key)

    if not statements:
        st.warning("No accounts found in the PDF.")
        st.stop()

    for parsed in statements:
        meta = parsed.meta
        st.subheader(f"Account: {meta.account_id}")
        st.markdown(
            f"**Period:** {meta.period_start} → {meta.period_end} &nbsp;|&nbsp; "
            f"**Currency:** {meta.base_currency}"
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("Positions", len(parsed.positions))
        col2.metric("Trades", len(parsed.trades))
        col3.metric("Skipped rows", len(parsed.skipped_rows))

        # Preview positions
        if parsed.positions:
            with st.expander("Preview positions", expanded=False):
                st.dataframe(
                    [p.model_dump() for p in parsed.positions],
                    use_container_width=True,
                )

        # Preview trades
        if parsed.trades:
            with st.expander("Preview trades", expanded=False):
                st.dataframe(
                    [t.model_dump() for t in parsed.trades],
                    use_container_width=True,
                )

        # Show skipped items with details
        if parsed.skipped_rows:
            with st.expander(
                f"Skipped items ({len(parsed.skipped_rows)}) — review these",
                expanded=True,
            ):
                st.caption(
                    "Rows skipped due to unsupported asset classes or parse errors. "
                    "Stocks, ETFs, and Options are supported."
                )
                st.dataframe(parsed.skipped_rows, use_container_width=True)

    st.divider()

    # ── Pre-save duplicate analysis ─────────────────────────────────────
    dup_key = f"dup_analysis_{uploaded.name}_{uploaded.size}"
    if dup_key not in st.session_state:
        with st.spinner("Checking for duplicates against database..."):
            try:
                analysis = {}
                for parsed in statements:
                    analysis[parsed.meta.account_id] = check_duplicates(parsed)
                st.session_state[dup_key] = analysis
            except Exception as e:
                st.warning(f"Could not check duplicates (will still save): {e}")
                st.session_state[dup_key] = {}

    dup_analysis = st.session_state.get(dup_key, {})

    if dup_analysis:
        st.subheader("Duplicate analysis")
        st.caption(
            "Only new data will be added — existing trades and positions "
            "are never deleted or overwritten."
        )
        for acct, info in dup_analysis.items():
            new_t = info["new_trades"]
            dup_t = info["dup_trades"]
            new_p = info["new_positions"]
            dup_p = info["dup_positions"]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric(f"{acct} — New trades", new_t)
            c2.metric("Duplicate trades", dup_t)
            c3.metric("New positions", new_p)
            c4.metric("Duplicate positions", dup_p)

            if new_t == 0 and new_p == 0:
                st.info(
                    f"**{acct}**: All data from this PDF already exists in the "
                    f"database. Saving will have no effect."
                )

    if st.button("Save to database", type="primary"):
        saved = 0
        errors = []
        for parsed in statements:
            try:
                stmt_id, trades_skipped, positions_skipped = upsert_statement(parsed)
                trades_new = len(parsed.trades) - trades_skipped
                positions_new = len(parsed.positions) - positions_skipped
                msg = (
                    f"Saved account {parsed.meta.account_id} → `{stmt_id}` "
                    f"({positions_new} new positions, {trades_new} new trades)"
                )
                if trades_skipped or positions_skipped:
                    msg += (
                        f" — skipped {trades_skipped} duplicate trades, "
                        f"{positions_skipped} duplicate positions"
                    )
                st.success(msg)
                saved += 1
            except Exception as exc:
                errors.append(str(exc))
                st.error(f"Failed to save account {parsed.meta.account_id}: {exc}")

        if saved > 0:
            clear_query_caches()

            # Verify data actually landed in Supabase
            try:
                client = get_client()
                verify = (
                    client.table("statements")
                    .select("id", count="exact")
                    .execute()
                )
                st.info(f"Verification: {verify.count} total statement(s) in database.")
            except Exception as exc:
                st.warning(f"Could not verify: {exc}")

        if saved == len(statements):
            st.balloons()
        elif saved == 0:
            st.error(
                "No accounts were saved. Check the errors above.\n\n"
                "Common causes:\n"
                "- SUPABASE_KEY is the **anon** key but RLS blocks writes\n"
                "- Network issue reaching Supabase\n"
                "- Table schema doesn't match expected columns"
            )
