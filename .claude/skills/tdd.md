---
description: "TDD workflow — write tests first, then implement, then verify. Use this for every code change."
user_invocable: true
name: tdd
---

# Test-Driven Development Workflow

You MUST follow this strict TDD cycle for every code change. No exceptions.

## Step 1: RED — Write Failing Tests First

Before writing ANY implementation code:

1. Understand what the user is asking for.
2. Identify which module(s) will be affected (under `src/` or `pages/`).
3. Write test(s) in the corresponding `tests/test_*.py` file that assert the desired behavior.
   - If no test file exists for the module, create one following the naming convention `tests/test_<module>.py`.
   - Tests must be specific and meaningful — not trivial stubs.
   - Use `pytest` conventions (functions starting with `test_`, use `assert`).
   - Mock external dependencies (Supabase, yfinance, Streamlit) — never call real services in tests.
4. Run the tests with `pytest` and confirm they **FAIL** (Red phase).
   - Show the user the failing test output.
   - If tests pass before implementation, the tests are not testing new behavior — rewrite them.

## Step 2: GREEN — Implement the Minimum Code to Pass

1. Now write the implementation code to make the failing tests pass.
2. Keep changes minimal — only what's needed to pass the tests.
3. Run `pytest` again and confirm all tests **PASS** (Green phase).
   - Show the user the passing test output.
   - If tests still fail, iterate on the implementation (NOT the tests) until they pass.
   - Maximum 5 iterations. If still failing after 5 attempts, stop and ask the user for guidance.

## Step 3: REFACTOR (if needed)

1. Once tests pass, review the code for obvious cleanup opportunities.
2. Only refactor if there's a clear improvement (duplication, naming, structure).
3. Run `pytest` again after any refactor to ensure nothing broke.

## Rules

- **Never skip Step 1.** Always write tests before implementation.
- **Never modify tests to make them pass** — fix the implementation instead.
- **Run the full test suite** (`pytest tests/`) at the end to catch regressions, not just the new tests.
- **Show test output** to the user at each phase (Red, Green, final).
- **If pytest is not installed**, install it first: `pip install pytest`.
- **Test file location**: All tests go in the `tests/` directory.
- **Imports**: Use `from src.<module> import ...` for importing project code in tests.

## Output Format

At each phase, clearly label the output:

```
=== TDD PHASE: RED (expecting failures) ===
<test output>

=== TDD PHASE: GREEN (expecting all pass) ===
<test output>

=== TDD PHASE: FINAL (full suite) ===
<test output>
```
