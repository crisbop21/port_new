import os
import streamlit as st
from dotenv import load_dotenv

from src.logging_config import setup_logging

load_dotenv()
setup_logging()

st.set_page_config(
    page_title="IBKR Trade Journal",
    page_icon="📊",
    layout="wide",
)

# --- Validate required environment variables ---
REQUIRED_VARS = ("SUPABASE_URL", "SUPABASE_KEY")
missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing:
    st.error(
        f"Missing required environment variables: **{', '.join(missing)}**.\n\n"
        "Set them in your `.env` file (see `.env.example`) or in "
        "Streamlit Cloud → App settings → Secrets."
    )
    st.stop()

st.title("IBKR Trade Journal")
st.markdown("Upload your Interactive Brokers statements and track your portfolio.")
