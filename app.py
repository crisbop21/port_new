import os
import sys
from pathlib import Path

# Ensure the repo root is on sys.path so `import src` resolves to our package,
# not the Streamlit Cloud mount directory (/mount/src/).
sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    # python-dotenv is not strictly required on Streamlit Cloud,
    # where secrets are injected via the platform.
    load_dotenv = None

from src.logging_config import setup_logging

_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists() and load_dotenv is not None:
    load_dotenv(_env_file)
setup_logging()

st.set_page_config(
    page_title="IBKR Trade Journal",
    page_icon="📊",
    layout="wide",
)

# Reduce default padding so content uses more of the screen
st.markdown(
    """
    <style>
    .block-container {
        padding-left: 2rem;
        padding-right: 2rem;
        padding-top: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Resolve secrets: prefer env vars, fall back to st.secrets ---
for var in ("SUPABASE_URL", "SUPABASE_KEY"):
    if not os.environ.get(var):
        try:
            os.environ[var] = st.secrets[var]
        except (KeyError, FileNotFoundError):
            pass

REQUIRED_VARS = ("SUPABASE_URL", "SUPABASE_KEY")
missing = [var for var in REQUIRED_VARS if not os.environ.get(var)]
if missing:
    st.error(
        f"Missing required secrets: **{', '.join(missing)}**.\n\n"
        "**Local:** set them in your `.env` file (see `.env.example`).\n\n"
        "**Streamlit Cloud:** add them in App settings → Secrets as:\n"
        "```\nSUPABASE_URL = \"your-url\"\nSUPABASE_KEY = \"your-key\"\n```"
    )
    st.stop()

st.title("IBKR Trade Journal")
st.markdown("Upload your Interactive Brokers statements and track your portfolio.")
