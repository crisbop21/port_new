"""Upload page — parse an IBKR PDF and save to Supabase."""

import streamlit as st

from src.parser import parse_statement
from src.db import upsert_statement, clear_query_caches, get_client

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
            except ValueError as e:
                st.error(f"Failed to parse PDF: {e}")
                st.stop()

    statements = st.session_state[file_key]

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

    if st.button("Save to database", type="primary"):
        saved = 0
        with st.spinner("Saving to database..."):
            for parsed in statements:
                try:
                    stmt_id = upsert_statement(parsed)
                    st.success(
                        f"Saved account {parsed.meta.account_id} → `{stmt_id}` "
                        f"({len(parsed.positions)} positions, {len(parsed.trades)} trades)"
                    )
                    saved += 1
                except Exception:
                    # upsert_statement already calls st.error
                    pass

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
            except Exception:
                pass

        if saved == len(statements):
            st.balloons()
