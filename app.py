import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="IBKR Trade Journal",
    page_icon="📊",
    layout="wide",
)

# --- Validate required environment variables ---
missing = [
    var for var in ("SUPABASE_URL", "SUPABASE_KEY")
    if not os.getenv(var)
]
if missing:
    st.error(f"Missing required environment variables: {', '.join(missing)}. "
             "See .env.example for the expected keys.")
    st.stop()

st.title("IBKR Trade Journal")
st.markdown("Upload your Interactive Brokers statements and track your portfolio.")
