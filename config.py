"""Central configuration for Trade Reports system."""
import os
from pathlib import Path

import pytz

# --- Environment ---
# Load .env if present (local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent
TELEGRAM_EXPORT_ROOT = Path(os.getenv(
    "TELEGRAM_EXPORT_ROOT",
    r"C:\Users\GlennPrince\Downloads\Telegram Desktop",
))
CSV_DATA_DIR = Path(os.getenv(
    "CSV_DATA_DIR",
    r"C:\TradingBots\kalshi_btc_bot\data",
))
OUTPUT_DIR = PROJECT_ROOT / "output"
PUBLISHED_DATA_DIR = PROJECT_ROOT / "published_data"

# --- Cloud detection ---
IS_CLOUD = not TELEGRAM_EXPORT_ROOT.exists()

# --- Timezone ---
LOCAL_TZ = pytz.timezone("US/Central")

# --- CSV file mapping (asset -> filename) ---
CSV_FILES = {
    "BTC": "data_feed5_btc.txt",
    "ETH": "data_feed5_eth.txt",
    "SOL": "data_feed5_sol.txt",
    "XRP": "data_feed5_xrp.txt",
}

# --- Supported assets ---
ASSETS = list(CSV_FILES.keys())

# --- Contract timing ---
CONTRACT_WINDOW_MINUTES = 15       # 15-minute contracts
SIGNAL_LEAD_MINUTES = 2            # Signal arrives up to 2 min before expiry
RESULT_LAG_MINUTES = 2             # Win/Loss arrives up to 2 min after expiry
CHART_LOOKBACK_SECONDS = 90        # 90-second chart window

# --- Price filter ---
PENNY_TRADE_CENTS = 2              # Filter for $0.02 trades

# --- Telegram bot ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --- Streamlit Cloud URL ---
STREAMLIT_APP_URL = os.getenv(
    "STREAMLIT_APP_URL",
    "https://gprince73-trade-reports.streamlit.app",
)
