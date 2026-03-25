---
description: "Modify Supabase database schema — create a numbered migration, update models and db.py, then output SQL for the user to run."
user_invocable: true
name: modify-db
---

# Modify Database Schema

Use this skill to safely make schema changes. Never alter the database directly — always create a migration file and let the user run it in the Supabase SQL Editor.

## Step 1: Determine the Next Migration Number

Read the `sql/` directory to find the highest-numbered migration file. The new migration will be `<next_number>_<descriptive_name>.sql`.

For example, if the latest is `007_reporting_frequency.sql`, the next is `008_<name>.sql`.

Exception: standalone table files (like `daily_prices.sql`) don't follow the numbering — only use those for entirely new independent tables.

## Step 2: Confirm the Change with the User

Before writing anything, clearly describe:

1. **What will change** — new table, new columns, altered constraints, new indexes, etc.
2. **Why** — which feature or fix requires this
3. **Impact** — will existing data be affected? Is this additive (safe) or destructive (needs care)?

Ask the user to confirm before proceeding.

## Step 3: Write the Migration SQL

Create the migration file in `sql/` following these conventions:

- Use `CREATE TABLE IF NOT EXISTS` for new tables
- Use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for new columns
- Use `DROP CONSTRAINT IF EXISTS` before re-creating changed constraints
- Include `CREATE INDEX IF NOT EXISTS` for new indexes
- Enable RLS and add permissive policies for new tables (matching existing pattern)
- Use `NUMERIC` for all financial values (never `float` or `double precision`)
- Use `uuid DEFAULT gen_random_uuid() PRIMARY KEY` for id columns
- Add a header comment explaining what the migration does and when to run it
- All migrations must be idempotent (safe to run multiple times)

## Step 4: Update Pydantic Models

If the schema change adds/removes/renames columns, update `src/models.py` to match:

- Add new fields with correct types (`Decimal` for numeric, `date` for dates, `Optional` where nullable)
- Keep validators consistent with existing patterns
- Do NOT remove fields that existing code depends on without checking references first

## Step 5: Update Database Layer

If needed, update `src/db.py`:

- Add/update upsert, query, or delete functions for new tables
- Follow existing patterns: `@st.cache_data` for reads, `_ser()` for serialization
- Include the new table in `clear_query_caches()` if applicable
- Use `get_client()` for all Supabase calls

## Step 6: Output the SQL for the User

After creating the migration file, display the full SQL content and instruct the user:

```
=== MIGRATION READY ===

File: sql/<NNN>_<name>.sql

Please run the following SQL in your Supabase SQL Editor:

<full SQL content>

After running, confirm here so I can proceed with application code.
```

## Rules

- **Never run SQL directly** — the user runs migrations manually in Supabase SQL Editor
- **Never drop columns or tables** without explicit user approval and a clear warning
- **Always preserve existing data** — use `IF NOT EXISTS` / `IF EXISTS` guards
- **Financial columns must be NUMERIC** — never float/double precision
- **Keep migrations atomic** — one logical change per file
- **Update the docstring** at the top of `src/db.py` if you add a new table
