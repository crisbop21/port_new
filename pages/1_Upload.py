"""Upload page — parse an IBKR PDF and save to Supabase."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from src.parser import parse_statement, _extract_tables, _split_accounts, diagnose_positions
from src.db import (
    upsert_statement,
    check_duplicates,
    get_snapshot_dates,
    reconcile_pair,
    clear_query_caches,
    get_client,
)

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
    reparse_key = f"reparse_{uploaded.name}_{uploaded.size}"

    if file_key not in st.session_state or st.session_state.get(reparse_key):
        st.session_state.pop(reparse_key, None)
        with st.spinner("Parsing PDF..."):
            try:
                uploaded.seek(0)
                st.session_state[file_key] = parse_statement(uploaded)
            except Exception as e:
                st.error(f"Failed to parse PDF: {e}")
                st.stop()

    statements = st.session_state.get(file_key)

    if st.button("🔄 Re-parse PDF"):
        st.session_state[reparse_key] = True
        # Also clear diagnostic cache so it re-runs
        diag_k = f"diag_{uploaded.name}_{uploaded.size}"
        st.session_state.pop(diag_k, None)
        dup_k = f"dup_analysis_{uploaded.name}_{uploaded.size}"
        st.session_state.pop(dup_k, None)
        st.rerun()

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

    # ── Parser diagnostic: row-by-row classification ─────────────────────
    diag_key = f"diag_{uploaded.name}_{uploaded.size}"

    def _run_diagnostic():
        try:
            uploaded.seek(0)
            raw_rows = _extract_tables(uploaded)
            uploaded.seek(0)
            account_groups = _split_accounts(raw_rows)
            data = {}
            for group in account_groups:
                from src.parser import _extract_meta
                try:
                    meta = _extract_meta(group)
                    acct_id = meta.account_id
                except Exception:
                    acct_id = f"Unknown-{len(data)}"
                data[acct_id] = diagnose_positions(group)
            st.session_state[diag_key] = data
        except Exception as e:
            st.session_state[diag_key] = {"error": str(e)}

    if diag_key not in st.session_state:
        _run_diagnostic()

    if st.button("🔄 Re-analyze PDF (clear diagnostic cache)"):
        _run_diagnostic()
        st.rerun()

    diag_data = st.session_state.get(diag_key, {})

    if diag_data and "error" not in diag_data:
        with st.expander("🔍 Parser Diagnostic — row-by-row classification", expanded=False):
            st.caption(
                "Every row from the PDF's Open Positions section is shown below "
                "with how the parser classified it. Use this to find missing positions. "
                "Look for rows classified as **skipped**, **error**, or **outside_section** "
                "that contain your expected symbol."
            )

            for acct_id, rows in diag_data.items():
                st.markdown(f"**Account: {acct_id}**")

                # Summary counts
                from collections import Counter
                counts = Counter(r["classification"] for r in rows)
                col_a, col_b, col_c, col_d = st.columns(4)
                col_a.metric("Parsed", counts.get("parsed", 0))
                col_b.metric("Skipped / Errors",
                             counts.get("skipped_no_asset_class", 0)
                             + counts.get("skipped_no_mapping", 0)
                             + counts.get("skipped_empty_symbol", 0)
                             + counts.get("error", 0))
                col_c.metric("Totals/Headers",
                             counts.get("total", 0)
                             + counts.get("column_header", 0)
                             + counts.get("section_header", 0))
                col_d.metric("Outside section", counts.get("outside_section", 0))

                # Symbol search filter
                search = st.text_input(
                    "Search for a symbol",
                    key=f"diag_search_{acct_id}",
                    placeholder="e.g. SOFI, AAPL",
                )

                # Build display data
                import pandas as pd
                display_rows = []
                for r in rows:
                    raw_cells = r["raw_row"] or []
                    # Show first 3 cells for quick scanning
                    first_cell = str(raw_cells[0] or "")[:50] if len(raw_cells) > 0 else ""
                    raw_preview = " | ".join(
                        str(c or "")[:20] for c in raw_cells[:4]
                    )

                    display_rows.append({
                        "Row #": r["row_num"],
                        "Classification": r["classification"],
                        "Asset Class": r["asset_class"] or "",
                        "Detail": r["detail"],
                        "First Cell": first_cell,
                        "Raw Preview": raw_preview,
                        "Cols": len(raw_cells),
                    })

                df = pd.DataFrame(display_rows)

                if search:
                    search_lower = search.strip().lower()
                    mask = (
                        df["Detail"].str.lower().str.contains(search_lower, na=False)
                        | df["First Cell"].str.lower().str.contains(search_lower, na=False)
                    )
                    match_indices = df.index[mask].tolist()
                    if not match_indices:
                        st.warning(
                            f"Symbol **{search}** not found in any row. "
                            "This means pdfplumber did not extract it from the PDF at all. "
                            "The issue is at the PDF extraction level, not the parser."
                        )
                    else:
                        st.dataframe(df.loc[mask], use_container_width=True, hide_index=True)

                        # Show context around problem matches
                        problem_matches = [
                            idx for idx in match_indices
                            if df.loc[idx, "Classification"] in {
                                "skipped_no_asset_class", "skipped_no_mapping",
                                "skipped_empty_symbol", "error",
                            }
                        ]
                        if problem_matches:
                            st.markdown("**Context around skipped/errored rows** "
                                        "(10 rows before & 5 after):")
                            context_indices: set[int] = set()
                            for idx in problem_matches:
                                for offset in range(-10, 6):
                                    ctx = idx + offset
                                    if 0 <= ctx < len(df):
                                        context_indices.add(ctx)
                            ctx_df = df.loc[sorted(context_indices)]
                            st.dataframe(ctx_df, use_container_width=True, hide_index=True)

                            # Show full raw rows for the problem entries
                            st.markdown("**Full raw row data for skipped entries:**")
                            for idx in problem_matches:
                                r = rows[idx]
                                st.code(
                                    f"Row {r['row_num']} ({r['classification']}):\n"
                                    f"  Cells ({len(r['raw_row'])}): {r['raw_row']}",
                                    language=None,
                                )
                else:
                    # Color-code: show problem rows prominently
                    problem_classifications = {
                        "skipped_no_asset_class", "skipped_no_mapping",
                        "skipped_empty_symbol", "error", "asset_class_skipped",
                    }
                    problem_df = df[df["Classification"].isin(problem_classifications)]
                    if not problem_df.empty:
                        st.markdown("**⚠️ Problem rows (skipped or errored):**")
                        st.dataframe(problem_df, use_container_width=True, hide_index=True)

                    st.markdown("**All rows:**")
                    st.dataframe(df, use_container_width=True, hide_index=True)

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

        # ── Post-save: reconciliation alerts ─────────────────────────
        if saved > 0:
            st.divider()
            st.subheader("Post-upload integrity check")
            for parsed in statements:
                acct = parsed.meta.account_id

                snapshot_dates = get_snapshot_dates(acct)
                if len(snapshot_dates) < 2:
                    st.info(
                        f"**{acct}**: Only {len(snapshot_dates)} snapshot(s) — "
                        "need at least 2 to reconcile."
                    )
                    continue

                # Reconcile the last two consecutive snapshots
                base_d = snapshot_dates[-2]
                target_d = snapshot_dates[-1]
                recon = reconcile_pair(acct, base_d, target_d)

                if recon["ok"]:
                    st.success(
                        f"**{acct}**: Reconciliation passed — "
                        f"{recon['base_date']} + trades = {recon['target_date']}."
                    )
                else:
                    mismatched = sum(
                        1 for h in recon["holdings"].values() if not h["match"]
                    )
                    st.error(
                        f"**{acct}**: Reconciliation FAILED — {mismatched} holding(s) "
                        f"differ between {recon['base_date']} and {recon['target_date']}. "
                        "Go to **Holdings** for details."
                    )

        if saved == len(statements):
            st.balloons()
            st.divider()
            st.subheader("Next steps")
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.page_link("pages/6_Prices.py", label="Fetch Prices", icon="📈")
            with col_b:
                st.page_link("pages/2_Holdings.py", label="View Holdings", icon="📋")
            with col_c:
                st.page_link("pages/4_Dashboard.py", label="Dashboard", icon="📊")
        elif saved == 0:
            st.error(
                "No accounts were saved. Check the errors above.\n\n"
                "Common causes:\n"
                "- SUPABASE_KEY is the **anon** key but RLS blocks writes\n"
                "- Network issue reaching Supabase\n"
                "- Table schema doesn't match expected columns"
            )
