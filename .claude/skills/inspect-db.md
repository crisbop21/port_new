---
description: "Inspect Supabase database schema — run this before any code change to check if schema adjustments are needed."
user_invocable: true
name: inspect-db
---

# Inspect Database Schema

Before writing any code, you MUST run this skill to understand the current database state and determine if schema changes are needed.

## Step 1: Read the SQL Migrations

Read ALL files in `sql/` (in order: 001 through the latest numbered migration, plus `daily_prices.sql`) to build a complete picture of the current schema.

List every table with its columns, types, constraints, indexes, and RLS policies.

## Step 2: Cross-Reference with Code

Read `src/models.py` to see the Pydantic models and `src/db.py` to see which tables/columns the application code actually references.

Identify any discrepancies:
- Columns in SQL but not in models
- Columns in models but not in SQL
- Missing indexes that queries would benefit from

## Step 3: Present the Schema Summary

Output a clear, structured summary:

```
=== CURRENT DATABASE SCHEMA ===

Table: <name>
  Columns: <name> <type> [constraints]
  Indexes: <list>
  Unique:  <list>
  RLS:     enabled / disabled

(repeat for each table)

=== DISCREPANCIES ===
(any mismatches between SQL, models, and db.py — or "None found")

=== MIGRATION FILES ===
(numbered list of all migration files applied)
```

## Step 4: Ask Before Proceeding

After presenting the schema, ask the user:

> "Here is the current schema. Does the feature you're planning require any schema changes? If so, I'll use `/modify-db` to handle that before writing application code."

**Do NOT proceed to write application code until the user confirms the schema is ready.**
