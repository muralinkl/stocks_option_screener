"""
Stock Screener Application - UI Matching Original Design
=========================================================

Features:
- Three-column layout (Bullish/Bearish/Neutral)
- Expandable stock cards with detailed metrics
- MACD Histogram 5-day differences
- Detailed analysis page with charts
- Auto-refresh functionality

Author: Redesigned version
Date: 2024-12-11
"""

import os
import random
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

# Load environment variables from .env file
from dotenv import load_dotenv
from plotly.subplots import make_subplots
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import date
load_dotenv()


# Create a session with connection pooling for faster requests
def get_http_session():
    session = requests.Session()
    retry = Retry(total=1, backoff_factor=0.1)
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# Global session for reuse
HTTP_SESSION = get_http_session()

# ----------------- Configuration and Setup -----------------
st.set_page_config(layout="wide", page_title="NSE Stock Screener (Ichimoku & MACD)")

# Upstox API Configuration - Loaded from .env file with fallback defaults
API_KEY = os.environ.get("UPSTOX_API_KEY", "edc571d8-281a-4299-8e7e-6419072d63c5")
API_SECRET = os.environ.get("UPSTOX_API_SECRET", "i2mxuje8lb")
REDIRECT_URL = os.environ.get("UPSTOX_REDIRECT_URL", "https://127.0.0.0")

# API URLs
TOKEN_URL = os.environ.get(
    "UPSTOX_TOKEN_URL", "https://api.upstox.com/v2/login/authorization/token"
)
BASE_URL = os.environ.get("UPSTOX_BASE_URL", "https://api.upstox.com")
HISTORICAL_BASE_URL = BASE_URL

# API Endpoints
HISTORICAL_CANDLE_V2_URL = f"{HISTORICAL_BASE_URL}/v2/historical-candle"
INTRADAY_CANDLE_V2_URL = f"{HISTORICAL_BASE_URL}/v2/historical-candle/intraday"
OPTION_CONTRACT_URL = f"{BASE_URL}/v2/option/contract"
OPTION_CHAIN_URL = f"{BASE_URL}/v2/option/chain"
USER_PROFILE_URL = f"{BASE_URL}/v2/user/profile"

# App Settings - Loaded from .env file with defaults
DB_NAME = os.environ.get("DB_NAME", "stock_screener.db")
AUTO_REFRESH_INTERVAL = int(
    os.environ.get("AUTO_REFRESH_INTERVAL", "10")
)  # 10 seconds auto-refresh
MAX_PARALLEL_WORKERS = int(
    os.environ.get("MAX_PARALLEL_WORKERS", "50")
)  # Increased for faster screening
API_TIMEOUT = 5  # Reduced timeout for faster response

# Trading Settings
DEFAULT_PROFIT_TARGET_PCT = 2.5  # Default profit target percentage for sell orders
DEFAULT_BUY_BUFFER_PCT = 0.2  # Buffer above LTP for buy limit orders (0.2%)

# Session State Initialization
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "page" not in st.session_state:
    st.session_state.page = "auth"
if "selected_symbol" not in st.session_state:
    st.session_state.selected_symbol = None
if "selected_stock_data" not in st.session_state:
    st.session_state.selected_stock_data = None
if "stock_list_data" not in st.session_state:
    st.session_state.stock_list_data = []
if "last_refresh_time" not in st.session_state:
    st.session_state.last_refresh_time = None
if "force_refresh" not in st.session_state:
    st.session_state.force_refresh = False
if "use_mock_data" not in st.session_state:
    st.session_state.use_mock_data = True
if "expand_mode" not in st.session_state:
    st.session_state.expand_mode = "none"  # none, all, bullish, bearish

# Sample Stock List (NSE FO Stocks) - 211 Stocks
STOCK_LIST = [
    {
        "symbol": "BAJAJ-AUTO",
        "name": "BAJAJ AUTO LIMITED",
        "isin": "INE917I01010",
        "has_options": 1,
    },
    {
        "symbol": "360ONE",
        "name": "360 ONE WAM LIMITED",
        "isin": "INE466L01038",
        "has_options": 1,
    },
    {
        "symbol": "BDL",
        "name": "BHARAT DYNAMICS LIMITED",
        "isin": "INE171Z01026",
        "has_options": 1,
    },
    {
        "symbol": "COFORGE",
        "name": "COFORGE LIMITED",
        "isin": "INE591G01025",
        "has_options": 1,
    },
    {
        "symbol": "CDSL",
        "name": "CENTRAL DEPO SER (I) LTD",
        "isin": "INE736A01011",
        "has_options": 1,
    },
    {
        "symbol": "BIOCON",
        "name": "BIOCON LIMITED.",
        "isin": "INE376G01013",
        "has_options": 1,
    },
    {
        "symbol": "BHARATFORG",
        "name": "BHARAT FORGE LTD",
        "isin": "INE465A01025",
        "has_options": 1,
    },
    {
        "symbol": "ALKEM",
        "name": "ALKEM LABORATORIES LTD.",
        "isin": "INE540L01014",
        "has_options": 1,
    },
    {
        "symbol": "CANBK",
        "name": "CANARA BANK",
        "isin": "INE476A01022",
        "has_options": 1,
    },
    {
        "symbol": "BANKBARODA",
        "name": "BANK OF BARODA",
        "isin": "INE028A01039",
        "has_options": 1,
    },
    {
        "symbol": "ABCAPITAL",
        "name": "ADITYA BIRLA CAPITAL LTD.",
        "isin": "INE674K01013",
        "has_options": 1,
    },
    {
        "symbol": "ASTRAL",
        "name": "ASTRAL LIMITED",
        "isin": "INE006I01046",
        "has_options": 1,
    },
    {
        "symbol": "BRITANNIA",
        "name": "BRITANNIA INDUSTRIES LTD",
        "isin": "INE216A01030",
        "has_options": 1,
    },
    {
        "symbol": "CONCOR",
        "name": "CONTAINER CORP OF IND LTD",
        "isin": "INE111A01025",
        "has_options": 1,
    },
    {
        "symbol": "CHOLAFIN",
        "name": "CHOLAMANDALAM IN & FIN CO",
        "isin": "INE121A01024",
        "has_options": 1,
    },
    {
        "symbol": "ADANIPORTS",
        "name": "ADANI PORT & SEZ LTD",
        "isin": "INE742F01042",
        "has_options": 1,
    },
    {
        "symbol": "ADANIENSOL",
        "name": "ADANI ENERGY SOLUTION LTD",
        "isin": "INE931S01010",
        "has_options": 1,
    },
    {
        "symbol": "ASIANPAINT",
        "name": "ASIAN PAINTS LIMITED",
        "isin": "INE021A01026",
        "has_options": 1,
    },
    {
        "symbol": "CAMS",
        "name": "COMPUTER AGE MNGT SER LTD",
        "isin": "INE596I01020",
        "has_options": 1,
    },
    {
        "symbol": "APLAPOLLO",
        "name": "APL APOLLO TUBES LTD",
        "isin": "INE702C01027",
        "has_options": 1,
    },
    {
        "symbol": "ABB",
        "name": "ABB INDIA LIMITED",
        "isin": "INE117A01022",
        "has_options": 1,
    },
    {
        "symbol": "APOLLOHOSP",
        "name": "APOLLO HOSPITALS ENTER. L",
        "isin": "INE437A01024",
        "has_options": 1,
    },
    {
        "symbol": "COALINDIA",
        "name": "COAL INDIA LTD",
        "isin": "INE522F01014",
        "has_options": 1,
    },
    {
        "symbol": "BAJFINANCE",
        "name": "BAJAJ FINANCE LIMITED",
        "isin": "INE296A01032",
        "has_options": 1,
    },
    {
        "symbol": "AMBUJACEM",
        "name": "AMBUJA CEMENTS LTD",
        "isin": "INE079A01024",
        "has_options": 1,
    },
    {
        "symbol": "CROMPTON",
        "name": "CROMPT GREA CON ELEC LTD",
        "isin": "INE299U01018",
        "has_options": 1,
    },
    {
        "symbol": "AMBER",
        "name": "AMBER ENTERPRISES (I) LTD",
        "isin": "INE371P01015",
        "has_options": 1,
    },
    {
        "symbol": "BAJAJFINSV",
        "name": "BAJAJ FINSERV LTD.",
        "isin": "INE918I01026",
        "has_options": 1,
    },
    {
        "symbol": "BHARTIARTL",
        "name": "BHARTI AIRTEL LIMITED",
        "isin": "INE397D01024",
        "has_options": 1,
    },
    {"symbol": "CIPLA", "name": "CIPLA LTD", "isin": "INE059A01026", "has_options": 1},
    {
        "symbol": "ANGELONE",
        "name": "ANGEL ONE LIMITED",
        "isin": "INE732I01013",
        "has_options": 1,
    },
    {
        "symbol": "AUBANK",
        "name": "AU SMALL FINANCE BANK LTD",
        "isin": "INE949L01017",
        "has_options": 1,
    },
    {
        "symbol": "CUMMINSIND",
        "name": "CUMMINS INDIA LTD",
        "isin": "INE298A01020",
        "has_options": 1,
    },
    {
        "symbol": "CYIENT",
        "name": "CYIENT LIMITED",
        "isin": "INE136B01020",
        "has_options": 1,
    },
    {"symbol": "BSE", "name": "BSE LIMITED", "isin": "INE118H01025", "has_options": 1},
    {
        "symbol": "ADANIGREEN",
        "name": "ADANI GREEN ENERGY LTD",
        "isin": "INE364U01010",
        "has_options": 1,
    },
    {
        "symbol": "AXISBANK",
        "name": "AXIS BANK LIMITED",
        "isin": "INE238A01034",
        "has_options": 1,
    },
    {
        "symbol": "BEL",
        "name": "BHARAT ELECTRONICS LTD",
        "isin": "INE263A01024",
        "has_options": 1,
    },
    {
        "symbol": "BANKINDIA",
        "name": "BANK OF INDIA",
        "isin": "INE084A01016",
        "has_options": 1,
    },
    {
        "symbol": "BOSCHLTD",
        "name": "BOSCH LIMITED",
        "isin": "INE323A01026",
        "has_options": 1,
    },
    {
        "symbol": "BANDHANBNK",
        "name": "BANDHAN BANK LIMITED",
        "isin": "INE545U01014",
        "has_options": 1,
    },
    {
        "symbol": "AUROPHARMA",
        "name": "AUROBINDO PHARMA LTD",
        "isin": "INE406A01037",
        "has_options": 1,
    },
    {
        "symbol": "ASHOKLEY",
        "name": "ASHOK LEYLAND LTD",
        "isin": "INE208A01029",
        "has_options": 1,
    },
    {
        "symbol": "BLUESTARCO",
        "name": "BLUE STAR LIMITED",
        "isin": "INE472A01039",
        "has_options": 1,
    },
    {
        "symbol": "CGPOWER",
        "name": "CG POWER AND IND SOL LTD",
        "isin": "INE067A01029",
        "has_options": 1,
    },
    {
        "symbol": "ADANIENT",
        "name": "ADANI ENTERPRISES LIMITED",
        "isin": "INE423A01024",
        "has_options": 1,
    },
    {
        "symbol": "COLPAL",
        "name": "COLGATE PALMOLIVE LTD.",
        "isin": "INE259A01022",
        "has_options": 1,
    },
    {"symbol": "BHEL", "name": "BHEL", "isin": "INE257A01026", "has_options": 1},
    {
        "symbol": "BPCL",
        "name": "BHARAT PETROLEUM CORP LT",
        "isin": "INE029A01011",
        "has_options": 1,
    },
    {
        "symbol": "DALBHARAT",
        "name": "DALMIA BHARAT LIMITED",
        "isin": "INE00R701025",
        "has_options": 1,
    },
    {
        "symbol": "HINDZINC",
        "name": "HINDUSTAN ZINC LIMITED",
        "isin": "INE267A01025",
        "has_options": 1,
    },
    {
        "symbol": "GODREJCP",
        "name": "GODREJ CONSUMER PRODUCTS",
        "isin": "INE102D01028",
        "has_options": 1,
    },
    {
        "symbol": "INFY",
        "name": "INFOSYS LIMITED",
        "isin": "INE009A01021",
        "has_options": 1,
    },
    {
        "symbol": "DIVISLAB",
        "name": "DIVI S LABORATORIES LTD",
        "isin": "INE361B01024",
        "has_options": 1,
    },
    {
        "symbol": "HINDUNILVR",
        "name": "HINDUSTAN UNILEVER LTD.",
        "isin": "INE030A01027",
        "has_options": 1,
    },
    {
        "symbol": "HEROMOTOCO",
        "name": "HERO MOTOCORP LIMITED",
        "isin": "INE158A01026",
        "has_options": 1,
    },
    {
        "symbol": "HINDPETRO",
        "name": "HINDUSTAN PETROLEUM CORP",
        "isin": "INE094A01015",
        "has_options": 1,
    },
    {
        "symbol": "ICICIPRULI",
        "name": "ICICI PRU LIFE INS CO LTD",
        "isin": "INE726G01019",
        "has_options": 1,
    },
    {
        "symbol": "GODREJPROP",
        "name": "GODREJ PROPERTIES LTD",
        "isin": "INE484J01027",
        "has_options": 1,
    },
    {
        "symbol": "GRASIM",
        "name": "GRASIM INDUSTRIES LTD",
        "isin": "INE047A01021",
        "has_options": 1,
    },
    {
        "symbol": "HFCL",
        "name": "HFCL LIMITED",
        "isin": "INE548A01028",
        "has_options": 1,
    },
    {
        "symbol": "IDEA",
        "name": "VODAFONE IDEA LIMITED",
        "isin": "INE669E01016",
        "has_options": 1,
    },
    {
        "symbol": "INOXWIND",
        "name": "INOX WIND LIMITED",
        "isin": "INE066P01011",
        "has_options": 1,
    },
    {
        "symbol": "IRCTC",
        "name": "INDIAN RAIL TOUR CORP LTD",
        "isin": "INE335Y01020",
        "has_options": 1,
    },
    {
        "symbol": "IRFC",
        "name": "INDIAN RAILWAY FIN CORP L",
        "isin": "INE053F01010",
        "has_options": 1,
    },
    {
        "symbol": "HDFCAMC",
        "name": "HDFC AMC LIMITED",
        "isin": "INE127D01025",
        "has_options": 1,
    },
    {
        "symbol": "IEX",
        "name": "INDIAN ENERGY EXC LTD",
        "isin": "INE022Q01020",
        "has_options": 1,
    },
    {
        "symbol": "INDHOTEL",
        "name": "THE INDIAN HOTELS CO. LTD",
        "isin": "INE053A01029",
        "has_options": 1,
    },
    {
        "symbol": "INDUSTOWER",
        "name": "INDUS TOWERS LIMITED",
        "isin": "INE121J01017",
        "has_options": 1,
    },
    {
        "symbol": "HAL",
        "name": "HINDUSTAN AERONAUTICS LTD",
        "isin": "INE066F01020",
        "has_options": 1,
    },
    {
        "symbol": "HDFCBANK",
        "name": "HDFC BANK LTD",
        "isin": "INE040A01034",
        "has_options": 1,
    },
    {
        "symbol": "IREDA",
        "name": "INDIAN RENEWABLE ENERGY",
        "isin": "INE202E01016",
        "has_options": 1,
    },
    {
        "symbol": "EICHERMOT",
        "name": "EICHER MOTORS LTD",
        "isin": "INE066A01021",
        "has_options": 1,
    },
    {"symbol": "DLF", "name": "DLF LIMITED", "isin": "INE271C01023", "has_options": 1},
    {
        "symbol": "DRREDDY",
        "name": "DR. REDDY S LABORATORIES",
        "isin": "INE089A01031",
        "has_options": 1,
    },
    {
        "symbol": "INDIGO",
        "name": "INTERGLOBE AVIATION LTD",
        "isin": "INE646L01027",
        "has_options": 1,
    },
    {
        "symbol": "EXIDEIND",
        "name": "EXIDE INDUSTRIES LTD",
        "isin": "INE302A01020",
        "has_options": 1,
    },
    {
        "symbol": "DMART",
        "name": "AVENUE SUPERMARTS LIMITED",
        "isin": "INE192R01011",
        "has_options": 1,
    },
    {
        "symbol": "HDFCLIFE",
        "name": "HDFC LIFE INS CO LTD",
        "isin": "INE795G01014",
        "has_options": 1,
    },
    {
        "symbol": "INDUSINDBK",
        "name": "INDUSIND BANK LIMITED",
        "isin": "INE095A01012",
        "has_options": 1,
    },
    {
        "symbol": "INDIANB",
        "name": "INDIAN BANK",
        "isin": "INE562A01011",
        "has_options": 1,
    },
    {
        "symbol": "DIXON",
        "name": "DIXON TECHNO (INDIA) LTD",
        "isin": "INE935N01020",
        "has_options": 1,
    },
    {
        "symbol": "HINDALCO",
        "name": "HINDALCO INDUSTRIES LTD",
        "isin": "INE038A01020",
        "has_options": 1,
    },
    {
        "symbol": "HUDCO",
        "name": "HSG & URBAN DEV CORPN LTD",
        "isin": "INE031A01017",
        "has_options": 1,
    },
    {
        "symbol": "IOC",
        "name": "INDIAN OIL CORP LTD",
        "isin": "INE242A01010",
        "has_options": 1,
    },
    {
        "symbol": "FORTIS",
        "name": "FORTIS HEALTHCARE LTD",
        "isin": "INE061F01013",
        "has_options": 1,
    },
    {
        "symbol": "HCLTECH",
        "name": "HCL TECHNOLOGIES LTD",
        "isin": "INE860A01027",
        "has_options": 1,
    },
    {
        "symbol": "FEDERALBNK",
        "name": "FEDERAL BANK LTD",
        "isin": "INE171A01029",
        "has_options": 1,
    },
    {
        "symbol": "GMRAIRPORT",
        "name": "GMR AIRPORTS LIMITED",
        "isin": "INE776C01039",
        "has_options": 1,
    },
    {
        "symbol": "HAVELLS",
        "name": "HAVELLS INDIA LIMITED",
        "isin": "INE176B01034",
        "has_options": 1,
    },
    {
        "symbol": "IIFL",
        "name": "IIFL FINANCE LIMITED",
        "isin": "INE530B01024",
        "has_options": 1,
    },
    {
        "symbol": "ETERNAL",
        "name": "ETERNAL LIMITED",
        "isin": "INE758T01015",
        "has_options": 1,
    },
    {"symbol": "ITC", "name": "ITC LTD", "isin": "INE154A01025", "has_options": 1},
    {
        "symbol": "DELHIVERY",
        "name": "DELHIVERY LIMITED",
        "isin": "INE148O01028",
        "has_options": 1,
    },
    {
        "symbol": "ICICIBANK",
        "name": "ICICI BANK LTD.",
        "isin": "INE090A01021",
        "has_options": 1,
    },
    {
        "symbol": "IDFCFIRSTB",
        "name": "IDFC FIRST BANK LIMITED",
        "isin": "INE092T01019",
        "has_options": 1,
    },
    {
        "symbol": "ICICIGI",
        "name": "ICICI LOMBARD GIC LIMITED",
        "isin": "INE765G01017",
        "has_options": 1,
    },
    {
        "symbol": "GAIL",
        "name": "GAIL (INDIA) LTD",
        "isin": "INE129A01019",
        "has_options": 1,
    },
    {
        "symbol": "GLENMARK",
        "name": "GLENMARK PHARMACEUTICALS",
        "isin": "INE935A01035",
        "has_options": 1,
    },
    {
        "symbol": "DABUR",
        "name": "DABUR INDIA LTD",
        "isin": "INE016A01026",
        "has_options": 1,
    },
    {
        "symbol": "JINDALSTEL",
        "name": "JINDAL STEEL LIMITED",
        "isin": "INE749A01030",
        "has_options": 1,
    },
    {
        "symbol": "KAYNES",
        "name": "KAYNES TECHNOLOGY IND LTD",
        "isin": "INE918Z01012",
        "has_options": 1,
    },
    {
        "symbol": "KOTAKBANK",
        "name": "KOTAK MAHINDRA BANK LTD",
        "isin": "INE237A01028",
        "has_options": 1,
    },
    {
        "symbol": "LODHA",
        "name": "LODHA DEVELOPERS LIMITED",
        "isin": "INE670K01029",
        "has_options": 1,
    },
    {
        "symbol": "LTF",
        "name": "L&T FINANCE LIMITED",
        "isin": "INE498L01015",
        "has_options": 1,
    },
    {
        "symbol": "LUPIN",
        "name": "LUPIN LIMITED",
        "isin": "INE326A01037",
        "has_options": 1,
    },
    {
        "symbol": "LICHSGFIN",
        "name": "LIC HOUSING FINANCE LTD",
        "isin": "INE115A01026",
        "has_options": 1,
    },
    {
        "symbol": "JSWENERGY",
        "name": "JSW ENERGY LIMITED",
        "isin": "INE121E01018",
        "has_options": 1,
    },
    {
        "symbol": "JSWSTEEL",
        "name": "JSW STEEL LIMITED",
        "isin": "INE019A01038",
        "has_options": 1,
    },
    {
        "symbol": "LICI",
        "name": "LIFE INSURA CORP OF INDIA",
        "isin": "INE0J1Y01017",
        "has_options": 1,
    },
    {
        "symbol": "LAURUSLABS",
        "name": "LAURUS LABS LIMITED",
        "isin": "INE947Q01028",
        "has_options": 1,
    },
    {
        "symbol": "JIOFIN",
        "name": "JIO FIN SERVICES LTD",
        "isin": "INE758E01017",
        "has_options": 1,
    },
    {
        "symbol": "KFINTECH",
        "name": "KFIN TECHNOLOGIES LIMITED",
        "isin": "INE138Y01010",
        "has_options": 1,
    },
    {
        "symbol": "JUBLFOOD",
        "name": "JUBILANT FOODWORKS LTD",
        "isin": "INE797F01020",
        "has_options": 1,
    },
    {
        "symbol": "KPITTECH",
        "name": "KPIT TECHNOLOGIES LIMITED",
        "isin": "INE04I401011",
        "has_options": 1,
    },
    {
        "symbol": "KEI",
        "name": "KEI INDUSTRIES LTD.",
        "isin": "INE878B01027",
        "has_options": 1,
    },
    {
        "symbol": "LTIM",
        "name": "LTIMINDTREE LIMITED",
        "isin": "INE214T01019",
        "has_options": 1,
    },
    {
        "symbol": "KALYANKJIL",
        "name": "KALYAN JEWELLERS IND LTD",
        "isin": "INE303R01014",
        "has_options": 1,
    },
    {
        "symbol": "LT",
        "name": "LARSEN & TOUBRO LTD.",
        "isin": "INE018A01030",
        "has_options": 1,
    },
    {
        "symbol": "MARUTI",
        "name": "MARUTI SUZUKI INDIA LTD.",
        "isin": "INE585B01010",
        "has_options": 1,
    },
    {
        "symbol": "MANAPPURAM",
        "name": "MANAPPURAM FINANCE LTD",
        "isin": "INE522D01027",
        "has_options": 1,
    },
    {
        "symbol": "MCX",
        "name": "MULTI COMMODITY EXCHANGE",
        "isin": "INE745G01035",
        "has_options": 1,
    },
    {
        "symbol": "MAXHEALTH",
        "name": "MAX HEALTHCARE INS LTD",
        "isin": "INE027H01010",
        "has_options": 1,
    },
    {
        "symbol": "MUTHOOTFIN",
        "name": "MUTHOOT FINANCE LIMITED",
        "isin": "INE414G01012",
        "has_options": 1,
    },
    {
        "symbol": "MPHASIS",
        "name": "MPHASIS LIMITED",
        "isin": "INE356A01018",
        "has_options": 1,
    },
    {
        "symbol": "MARICO",
        "name": "MARICO LIMITED",
        "isin": "INE196A01026",
        "has_options": 1,
    },
    {
        "symbol": "MANKIND",
        "name": "MANKIND PHARMA LIMITED",
        "isin": "INE634S01028",
        "has_options": 1,
    },
    {
        "symbol": "MOTHERSON",
        "name": "SAMVARDHANA MOTHERSON INT",
        "isin": "INE775A01035",
        "has_options": 1,
    },
    {
        "symbol": "MFSL",
        "name": "MAX FINANCIAL SERV LTD",
        "isin": "INE180A01020",
        "has_options": 1,
    },
    {
        "symbol": "MAZDOCK",
        "name": "MAZAGON DOCK SHIPBUIL LTD",
        "isin": "INE249Z01020",
        "has_options": 1,
    },
    {
        "symbol": "M&M",
        "name": "MAHINDRA & MAHINDRA LTD",
        "isin": "INE101A01026",
        "has_options": 1,
    },
    {
        "symbol": "NATIONALUM",
        "name": "NATIONAL ALUMINIUM CO LTD",
        "isin": "INE139A01034",
        "has_options": 1,
    },
    {
        "symbol": "NUVAMA",
        "name": "NUVAMA WEALTH MANAGE LTD",
        "isin": "INE531F01015",
        "has_options": 1,
    },
    {
        "symbol": "OBEROIRLTY",
        "name": "OBEROI REALTY LIMITED",
        "isin": "INE093I01010",
        "has_options": 1,
    },
    {"symbol": "NMDC", "name": "NMDC LTD.", "isin": "INE584A01023", "has_options": 1},
    {"symbol": "NCC", "name": "NCC LIMITED", "isin": "INE868B01028", "has_options": 1},
    {
        "symbol": "ONGC",
        "name": "OIL AND NATURAL GAS CORP.",
        "isin": "INE213A01029",
        "has_options": 1,
    },
    {"symbol": "NTPC", "name": "NTPC LTD", "isin": "INE733E01010", "has_options": 1},
    {
        "symbol": "NYKAA",
        "name": "FSN E COMMERCE VENTURES",
        "isin": "INE388Y01029",
        "has_options": 1,
    },
    {
        "symbol": "NESTLEIND",
        "name": "NESTLE INDIA LIMITED",
        "isin": "INE239A01024",
        "has_options": 1,
    },
    {
        "symbol": "NBCC",
        "name": "NBCC (INDIA) LIMITED",
        "isin": "INE095N01031",
        "has_options": 1,
    },
    {
        "symbol": "NAUKRI",
        "name": "INFO EDGE (I) LTD",
        "isin": "INE663F01032",
        "has_options": 1,
    },
    {"symbol": "NHPC", "name": "NHPC LTD", "isin": "INE848E01016", "has_options": 1},
    {
        "symbol": "OFSS",
        "name": "ORACLE FIN SERV SOFT LTD.",
        "isin": "INE881D01027",
        "has_options": 1,
    },
    {
        "symbol": "OIL",
        "name": "OIL INDIA LTD",
        "isin": "INE274J01014",
        "has_options": 1,
    },
    {
        "symbol": "PNB",
        "name": "PUNJAB NATIONAL BANK",
        "isin": "INE160A01022",
        "has_options": 1,
    },
    {
        "symbol": "PFC",
        "name": "POWER FIN CORP LTD.",
        "isin": "INE134E01011",
        "has_options": 1,
    },
    {
        "symbol": "PATANJALI",
        "name": "PATANJALI FOODS LIMITED",
        "isin": "INE619A01035",
        "has_options": 1,
    },
    {
        "symbol": "PRESTIGE",
        "name": "PRESTIGE ESTATE LTD",
        "isin": "INE811K01011",
        "has_options": 1,
    },
    {
        "symbol": "PHOENIXLTD",
        "name": "THE PHOENIX MILLS LTD",
        "isin": "INE211B01039",
        "has_options": 1,
    },
    {
        "symbol": "PGEL",
        "name": "PG ELECTROPLAST LTD",
        "isin": "INE457L01029",
        "has_options": 1,
    },
    {
        "symbol": "PIIND",
        "name": "PI INDUSTRIES LTD",
        "isin": "INE603J01030",
        "has_options": 1,
    },
    {
        "symbol": "POWERGRID",
        "name": "POWER GRID CORP. LTD.",
        "isin": "INE752E01010",
        "has_options": 1,
    },
    {
        "symbol": "PIDILITIND",
        "name": "PIDILITE INDUSTRIES LTD",
        "isin": "INE318A01026",
        "has_options": 1,
    },
    {
        "symbol": "PAYTM",
        "name": "ONE 97 COMMUNICATIONS LTD",
        "isin": "INE982J01020",
        "has_options": 1,
    },
    {
        "symbol": "PAGEIND",
        "name": "PAGE INDUSTRIES LTD",
        "isin": "INE761H01022",
        "has_options": 1,
    },
    {
        "symbol": "PNBHOUSING",
        "name": "PNB HOUSING FIN LTD.",
        "isin": "INE572E01012",
        "has_options": 1,
    },
    {
        "symbol": "PPLPHARMA",
        "name": "PIRAMAL PHARMA LIMITED",
        "isin": "INE0DK501011",
        "has_options": 1,
    },
    {
        "symbol": "PERSISTENT",
        "name": "PERSISTENT SYSTEMS LTD",
        "isin": "INE262H01021",
        "has_options": 1,
    },
    {
        "symbol": "POLICYBZR",
        "name": "PB FINTECH LIMITED",
        "isin": "INE417T01026",
        "has_options": 1,
    },
    {
        "symbol": "POLYCAB",
        "name": "POLYCAB INDIA LIMITED",
        "isin": "INE455K01017",
        "has_options": 1,
    },
    {
        "symbol": "PETRONET",
        "name": "PETRONET LNG LIMITED",
        "isin": "INE347G01014",
        "has_options": 1,
    },
    {
        "symbol": "POWERINDIA",
        "name": "HITACHI ENERGY INDIA LTD",
        "isin": "INE07Y701011",
        "has_options": 1,
    },
    {
        "symbol": "SHREECEM",
        "name": "SHREE CEMENT LIMITED",
        "isin": "INE070A01015",
        "has_options": 1,
    },
    {
        "symbol": "SHRIRAMFIN",
        "name": "SHRIRAM FINANCE LIMITED",
        "isin": "INE721A01047",
        "has_options": 1,
    },
    {
        "symbol": "SBILIFE",
        "name": "SBI LIFE INSURANCE CO LTD",
        "isin": "INE123W01016",
        "has_options": 1,
    },
    {
        "symbol": "SYNGENE",
        "name": "SYNGENE INTERNATIONAL LTD",
        "isin": "INE398R01022",
        "has_options": 1,
    },
    {
        "symbol": "SONACOMS",
        "name": "SONA BLW PRECISION FRGS L",
        "isin": "INE073K01018",
        "has_options": 1,
    },
    {
        "symbol": "SBIN",
        "name": "STATE BANK OF INDIA",
        "isin": "INE062A01020",
        "has_options": 1,
    },
    {
        "symbol": "RELIANCE",
        "name": "RELIANCE INDUSTRIES LTD",
        "isin": "INE002A01018",
        "has_options": 1,
    },
    {
        "symbol": "SAMMAANCAP",
        "name": "SAMMAAN CAPITAL LIMITED",
        "isin": "INE148I01020",
        "has_options": 1,
    },
    {
        "symbol": "SUPREMEIND",
        "name": "SUPREME INDUSTRIES LTD",
        "isin": "INE195A01028",
        "has_options": 1,
    },
    {
        "symbol": "SUNPHARMA",
        "name": "SUN PHARMACEUTICAL IND L",
        "isin": "INE044A01036",
        "has_options": 1,
    },
    {
        "symbol": "RECLTD",
        "name": "REC LIMITED",
        "isin": "INE020B01018",
        "has_options": 1,
    },
    {"symbol": "SRF", "name": "SRF LTD", "isin": "INE647A01010", "has_options": 1},
    {
        "symbol": "RBLBANK",
        "name": "RBL BANK LIMITED",
        "isin": "INE976G01028",
        "has_options": 1,
    },
    {
        "symbol": "SBICARD",
        "name": "SBI CARDS & PAY SER LTD",
        "isin": "INE018E01016",
        "has_options": 1,
    },
    {
        "symbol": "RVNL",
        "name": "RAIL VIKAS NIGAM LIMITED",
        "isin": "INE415G01027",
        "has_options": 1,
    },
    {
        "symbol": "SOLARINDS",
        "name": "SOLAR INDUSTRIES (I) LTD",
        "isin": "INE343H01029",
        "has_options": 1,
    },
    {
        "symbol": "SUZLON",
        "name": "SUZLON ENERGY LIMITED",
        "isin": "INE040H01021",
        "has_options": 1,
    },
    {
        "symbol": "SAIL",
        "name": "STEEL AUTHORITY OF INDIA",
        "isin": "INE114A01011",
        "has_options": 1,
    },
    {
        "symbol": "SIEMENS",
        "name": "SIEMENS LTD",
        "isin": "INE003A01024",
        "has_options": 1,
    },
    {
        "symbol": "TATACONSUM",
        "name": "TATA CONSUMER PRODUCT LTD",
        "isin": "INE192A01025",
        "has_options": 1,
    },
    {
        "symbol": "TATATECH",
        "name": "TATA TECHNOLOGIES LIMITED",
        "isin": "INE142M01025",
        "has_options": 1,
    },
    {"symbol": "TRENT", "name": "TRENT LTD", "isin": "INE849A01020", "has_options": 1},
    {
        "symbol": "TECHM",
        "name": "TECH MAHINDRA LIMITED",
        "isin": "INE669C01036",
        "has_options": 1,
    },
    {
        "symbol": "TATASTEEL",
        "name": "TATA STEEL LIMITED",
        "isin": "INE081A01020",
        "has_options": 1,
    },
    {
        "symbol": "TIINDIA",
        "name": "TUBE INVEST OF INDIA LTD",
        "isin": "INE974X01010",
        "has_options": 1,
    },
    {
        "symbol": "TORNTPOWER",
        "name": "TORRENT POWER LTD",
        "isin": "INE813H01021",
        "has_options": 1,
    },
    {
        "symbol": "TATAPOWER",
        "name": "TATA POWER CO LTD",
        "isin": "INE245A01021",
        "has_options": 1,
    },
    {
        "symbol": "TITAGARH",
        "name": "TITAGARH RAIL SYSTEMS LTD",
        "isin": "INE615H01020",
        "has_options": 1,
    },
    {
        "symbol": "TCS",
        "name": "TATA CONSULTANCY SERV LT",
        "isin": "INE467B01029",
        "has_options": 1,
    },
    {
        "symbol": "TVSMOTOR",
        "name": "TVS MOTOR COMPANY LTD",
        "isin": "INE494B01023",
        "has_options": 1,
    },
    {
        "symbol": "TITAN",
        "name": "TITAN COMPANY LIMITED",
        "isin": "INE280A01028",
        "has_options": 1,
    },
    {
        "symbol": "TATAMOTORS",
        "name": "TATA MOTORS LTD",
        "isin": "INE155A01022",
        "has_options": 1,
    },
    {
        "symbol": "TORNTPHARM",
        "name": "TORRENT PHARMACEUTICALS L",
        "isin": "INE685A01028",
        "has_options": 1,
    },
    {
        "symbol": "TATAELXSI",
        "name": "TATA ELXSI LIMITED",
        "isin": "INE670A01012",
        "has_options": 1,
    },
    {
        "symbol": "UNOMINDA",
        "name": "UNO MINDA LIMITED",
        "isin": "INE405E01023",
        "has_options": 1,
    },
    {
        "symbol": "YESBANK",
        "name": "YES BANK LIMITED",
        "isin": "INE528G01035",
        "has_options": 1,
    },
    {"symbol": "WIPRO", "name": "WIPRO LTD", "isin": "INE075A01022", "has_options": 1},
    {
        "symbol": "VEDL",
        "name": "VEDANTA LIMITED",
        "isin": "INE205A01025",
        "has_options": 1,
    },
    {
        "symbol": "UNITDSPR",
        "name": "UNITED SPIRITS LIMITED",
        "isin": "INE854D01024",
        "has_options": 1,
    },
    {
        "symbol": "VOLTAS",
        "name": "VOLTAS LTD",
        "isin": "INE226A01021",
        "has_options": 1,
    },
    {
        "symbol": "ZYDUSLIFE",
        "name": "ZYDUS LIFESCIENCES LTD",
        "isin": "INE010B01027",
        "has_options": 1,
    },
    {
        "symbol": "UNIONBANK",
        "name": "UNION BANK OF INDIA",
        "isin": "INE692A01016",
        "has_options": 1,
    },
    {"symbol": "UPL", "name": "UPL LIMITED", "isin": "INE628A01036", "has_options": 1},
    {
        "symbol": "ULTRACEMCO",
        "name": "ULTRATECH CEMENT LIMITED",
        "isin": "INE481G01011",
        "has_options": 1,
    },
    {
        "symbol": "VBL",
        "name": "VARUN BEVERAGES LIMITED",
        "isin": "INE200M01039",
        "has_options": 1,
    },
]


# ----------------- Database Connection -----------------
@contextmanager
def get_db_connection(db_name=DB_NAME):
    conn = sqlite3.connect(db_name, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()


def get_stock_isin_cached(symbol):
    """Get ISIN for a symbol - first from database, then fallback to STOCK_LIST"""
    try:
        # First try database
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT isin FROM stocks WHERE symbol = ?", (symbol,))
            result = c.fetchone()
        if result and result[0]:
            return result[0]
    except Exception:
        pass

    # Fallback to STOCK_LIST if not found in database or ISIN is null
    for stock in STOCK_LIST:
        if stock["symbol"] == symbol:
            return stock["isin"]

    return None


# ----------------- Database Initialization -----------------
def check_and_reset_daily_isin():
    """Reset ISIN column at 8 AM every day and re-populate at market open (9:15 AM)"""
    try:
        current_time = datetime.now()
        current_hour = current_time.hour
        current_minute = current_time.minute
        today_date = current_time.strftime("%Y-%m-%d")

        with get_db_connection() as conn:
            c = conn.cursor()

            # Check if we already reset today
            c.execute("SELECT last_updated FROM stocks LIMIT 1")
            result = c.fetchone()
            last_updated = result[0] if result else None

            # At 8:00 AM - Clear ISIN column (before market opens)
            if current_hour == 8 and current_minute < 5:
                # Check if already reset today
                reset_key = f"isin_reset_{today_date}"
                if not st.session_state.get(reset_key, False):
                    c.execute(
                        "UPDATE stocks SET isin = NULL, last_updated = ?", (today_date,)
                    )
                    conn.commit()
                    st.session_state[reset_key] = True
                    return "reset"

            # At 9:15 AM or later - Re-populate ISIN from STOCK_LIST (market opens)
            if current_hour >= 9 and (current_hour > 9 or current_minute >= 15):
                # Check if ISIN is null and needs to be populated
                c.execute("SELECT COUNT(*) FROM stocks WHERE isin IS NULL OR isin = ''")
                null_count = c.fetchone()[0]

                if null_count > 0:
                    # Re-populate ISIN from STOCK_LIST
                    for stock in STOCK_LIST:
                        c.execute(
                            "UPDATE stocks SET isin = ?, last_updated = ? WHERE symbol = ?",
                            (stock["isin"], today_date, stock["symbol"]),
                        )
                    conn.commit()
                    return "populated"

            return "no_action"
    except Exception as e:
        return f"error: {e}"


def init_db(db_name=DB_NAME):
    # Only initialize once per session
    if st.session_state.get("db_initialized", False):
        return True

    try:
        with get_db_connection(db_name) as conn:
            c = conn.cursor()
            c.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY, name TEXT, isin TEXT, has_options INTEGER, last_updated TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, date TEXT,
                open REAL, high REAL, low REAL, close REAL, volume INTEGER, UNIQUE(symbol, date)
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS api_tokens (
                id INTEGER PRIMARY KEY, access_token TEXT, refresh_token TEXT, expires_at TEXT, created_at TEXT
            )""")
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date ON daily_prices(symbol, date)"
            )
            conn.commit()

            # Always insert/update all stocks from STOCK_LIST
            # This ensures new stocks are added when the list is updated
            for stock in STOCK_LIST:
                c.execute(
                    "INSERT OR REPLACE INTO stocks VALUES (?, ?, ?, ?, ?)",
                    (
                        stock["symbol"],
                        stock["name"],
                        stock["isin"],
                        stock["has_options"],
                        datetime.now().strftime("%Y-%m-%d"),
                    ),
                )
            conn.commit()

        st.session_state.db_initialized = True
        return True
    except Exception as e:
        st.error(f"❌ Database error: {e}")
        return False


# ----------------- Token Management -----------------
class TokenManager:
    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name

    def save_token(self, access_token, refresh_token=None, expires_in=None):
        try:
            with get_db_connection(self.db_name) as conn:
                c = conn.cursor()
                expires_at = (
                    (datetime.now() + timedelta(seconds=expires_in)).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if expires_in
                    else None
                )
                c.execute("DELETE FROM api_tokens")
                c.execute(
                    "INSERT INTO api_tokens (access_token, refresh_token, expires_at, created_at) VALUES (?, ?, ?, ?)",
                    (
                        access_token,
                        refresh_token,
                        expires_at,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
                conn.commit()
            return access_token
        except Exception:
            return None

    def get_token(self):
        try:
            with get_db_connection(self.db_name) as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT access_token, refresh_token, expires_at FROM api_tokens ORDER BY id DESC LIMIT 1"
                )
                result = c.fetchone()
            if result:
                access_token, refresh_token, expires_at = result
                if expires_at:
                    if datetime.strptime(
                        expires_at, "%Y-%m-%d %H:%M:%S"
                    ) <= datetime.now() + timedelta(hours=1):
                        return (
                            self.refresh_token_method(refresh_token)
                            if refresh_token
                            else None
                        )
                return access_token
            return None
        except Exception:
            return None

    def get_token_with_auto_refresh(self):
        """Get token and auto-refresh if expired"""
        try:
            with get_db_connection(self.db_name) as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT access_token, refresh_token, expires_at FROM api_tokens ORDER BY id DESC LIMIT 1"
                )
                result = c.fetchone()

            if result:
                access_token, refresh_token, expires_at = result

                # Check if token is expired or about to expire (within 1 hour)
                if expires_at:
                    try:
                        expiry_time = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
                        if expiry_time <= datetime.now() + timedelta(minutes=30):
                            # Token expired or expiring soon - try to refresh
                            if refresh_token:
                                new_token = self.refresh_token_method(refresh_token)
                                if new_token:
                                    return new_token, "refreshed"
                                else:
                                    return None, "refresh_failed"
                            else:
                                return None, "no_refresh_token"
                    except:
                        pass

                return access_token, "valid"
            return None, "no_token"
        except Exception as e:
            return None, f"error: {str(e)}"

    def refresh_token_method(self, refresh_token):
        try:
            response = requests.post(
                TOKEN_URL,
                data={
                    "refresh_token": refresh_token,
                    "client_id": API_KEY,
                    "client_secret": API_SECRET,
                    "grant_type": "refresh_token",
                },
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                new_access_token = data.get("access_token")
                new_refresh_token = data.get("refresh_token", refresh_token)
                expires_in = data.get("expires_in", 86400)  # Default 24 hours
                self.save_token(new_access_token, new_refresh_token, expires_in)
                return new_access_token
            elif response.status_code == 401:
                # Refresh token also expired - need full re-auth
                return None
        except Exception:
            pass
        return None

    def get_token_info(self):
        """Get detailed token information"""
        try:
            with get_db_connection(self.db_name) as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT access_token, refresh_token, expires_at, created_at FROM api_tokens ORDER BY id DESC LIMIT 1"
                )
                result = c.fetchone()

            if result:
                access_token, refresh_token, expires_at, created_at = result
                return {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "expires_at": expires_at,
                    "created_at": created_at,
                    "has_refresh_token": refresh_token is not None
                    and len(refresh_token) > 0,
                }
            return None
        except Exception:
            return None

    def get_new_token(self, auth_code):
        try:
            response = requests.post(
                TOKEN_URL,
                data={
                    "client_id": API_KEY,
                    "client_secret": API_SECRET,
                    "redirect_uri": REDIRECT_URL,
                    "code": auth_code,
                    "grant_type": "authorization_code",
                },
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                self.save_token(
                    data.get("access_token"),
                    data.get("refresh_token"),
                    data.get("expires_in"),
                )
                return data.get("access_token")
        except Exception:
            pass
        return None


# ----------------- Upstox API -----------------
class UpstoxAPI:
    def __init__(self, token_manager):
        self.token_manager = token_manager

    def get_headers(self):
        token = self.token_manager.get_token()
        return (
            {"Accept": "application/json", "Authorization": f"Bearer {token}"}
            if token
            else None
        )

    def _get_instrument_key(self, symbol):
        isin = get_stock_isin_cached(symbol)
        return f"NSE_EQ|{isin}" if isin else None

    def get_historical_data(self, symbol, days=200):
        try:
            headers = self.get_headers()
            if not headers:
                return None
            instrument_key = self._get_instrument_key(symbol)
            if not instrument_key:
                return None
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            url = f"{HISTORICAL_CANDLE_V2_URL}/{instrument_key}/day/{end_date}/{start_date}"
            response = HTTP_SESSION.get(url, headers=headers, timeout=API_TIMEOUT)
            if response.status_code == 200:
                candles = response.json().get("data", {}).get("candles", [])
                return [
                    {
                        "date": c[0][:10],
                        "open": c[1],
                        "high": c[2],
                        "low": c[3],
                        "close": c[4],
                        "volume": c[5],
                    }
                    for c in candles
                ]
        except Exception:
            pass
        return None

    def get_current_data(self, symbol, interval_minutes=1):
        """Get intraday candle data for a symbol

        Returns:
            Tuple of (candles_list, error_message) - candles_list is None if failed
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"
            instrument_key = self._get_instrument_key(symbol)
            if not instrument_key:
                return None, f"Could not find instrument key for {symbol}"

            # Correct interval mapping for Upstox API
            # Intraday API accepts: 1minute, 30minute
            interval_map = {1: "1minute", 30: "30minute"}
            interval_str = interval_map.get(interval_minutes, "1minute")

            url = f"{INTRADAY_CANDLE_V2_URL}/{instrument_key}/{interval_str}"

            # Single request with short timeout for speed
            response = HTTP_SESSION.get(url, headers=headers, timeout=API_TIMEOUT)

            if response.status_code == 200:
                candles = response.json().get("data", {}).get("candles", [])
                if candles:
                    return [
                        {
                            "datetime": c[0],
                            "open": c[1],
                            "high": c[2],
                            "low": c[3],
                            "close": c[4],
                            "volume": c[5],
                        }
                        for c in candles
                    ], None
                else:
                    # Check if market is open
                    current_hour = datetime.now().hour
                    current_minute = datetime.now().minute
                    is_market_hours = (
                        (current_hour == 9 and current_minute >= 15)
                        or (current_hour > 9 and current_hour < 15)
                        or (current_hour == 15 and current_minute <= 30)
                    )

                    if not is_market_hours:
                        return None, "Market closed - No live data available"
                    else:
                        return None, "No candle data available"
            elif response.status_code == 401:
                return None, "Token expired"
            elif response.status_code == 429:
                return None, "Rate limited"
            else:
                return None, f"HTTP {response.status_code}"

        except requests.exceptions.Timeout:
            return None, "Timeout"
        except Exception as e:
            return None, str(e)[:50]

    def get_option_contracts(self, instrument_key, expiry_date=None):
        """
        Fetch option contracts for an underlying symbol

        Args:
            instrument_key: Key of underlying symbol (e.g., "NSE_INDEX|Nifty 50", "NSE_EQ|INE009A01021")
            expiry_date: Optional expiry date in YYYY-MM-DD format

        Returns:
            Tuple of (contracts_list, error_message) - contracts_list is None if failed
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"

            # URL encode the instrument key
            import urllib.parse

            encoded_key = urllib.parse.quote(instrument_key, safe="")

            url = f"{OPTION_CONTRACT_URL}?instrument_key={encoded_key}"

            if expiry_date:
                url += f"&expiry_date={expiry_date}"

            # Try up to 3 times with delay for rate limiting
            max_retries = 3
            for attempt in range(max_retries):
                response = requests.get(url, headers=headers, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "success":
                        contracts = data.get("data", [])
                        return contracts, None
                    else:
                        return (
                            None,
                            f"API Error: {data.get('message', 'Unknown error')}",
                        )
                elif response.status_code == 401:
                    return None, "Authentication failed - Token may be expired"
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return (
                            None,
                            "Rate limited (429) - Too many requests. Please wait a few seconds and try again.",
                        )
                elif response.status_code == 400:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get("errors", [{}])[0].get(
                            "message", "Bad request"
                        )
                    except:
                        error_msg = "Bad request"
                    return None, f"Bad Request: {error_msg}"
                else:
                    return None, f"HTTP {response.status_code}"

        except requests.exceptions.Timeout:
            return None, "Request timed out"
        except Exception as e:
            return None, f"Error: {str(e)}"

    def get_option_contracts_for_stock(self, symbol, expiry_date=None):
        """
        Fetch option contracts for a stock symbol

        Args:
            symbol: Stock symbol (e.g., "RELIANCE", "TCS")
            expiry_date: Optional expiry date in YYYY-MM-DD format

        Returns:
            Tuple of (contracts_list, error_message, instrument_key)
        """
        try:
            # Get the ISIN for the stock
            isin = get_stock_isin_cached(symbol)
            if not isin:
                return None, f"Could not find ISIN for {symbol}", None

            # For stocks, use NSE_EQ instrument key
            instrument_key = f"NSE_EQ|{isin}"

            contracts, error = self.get_option_contracts(instrument_key, expiry_date)
            return contracts, error, instrument_key
        except Exception as e:
            return None, f"Error: {str(e)}", None

    def get_option_chain_for_index(self, index_name="Nifty 50", expiry_date=None):
        """
        Fetch option contracts for an index

        Args:
            index_name: Index name (e.g., "Nifty 50", "Nifty Bank")
            expiry_date: Optional expiry date in YYYY-MM-DD format

        Returns:
            List of option contracts or None if failed
        """
        try:
            instrument_key = f"NSE_INDEX|{index_name}"
            return self.get_option_contracts(instrument_key, expiry_date)
        except Exception:
            return None

    def get_option_chain(self, instrument_key, expiry_date):
        """
        Fetch full option chain with market data and Greeks

        Args:
            instrument_key: Instrument key (e.g., "NSE_INDEX|Nifty 50", "NSE_EQ|INE009A01021")
            expiry_date: Expiry date in YYYY-MM-DD format (required)

        Returns:
            Tuple of (option_chain_list, spot_price, error_message)
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, None, "No valid API token found"

            import urllib.parse

            encoded_key = urllib.parse.quote(instrument_key, safe="")

            url = f"{OPTION_CHAIN_URL}?instrument_key={encoded_key}&expiry_date={expiry_date}"

            response = HTTP_SESSION.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    option_data = data.get("data", [])

                    if not option_data:
                        return (
                            None,
                            None,
                            f"No option chain data for expiry {expiry_date}. Try a valid expiry date (usually Thursday).",
                        )

                    option_chain = []
                    spot_price = None

                    for option in option_data:
                        expiry = option.get("expiry")
                        strike_price = option.get("strike_price")
                        underlying_spot_price = option.get("underlying_spot_price")

                        if spot_price is None and underlying_spot_price:
                            spot_price = underlying_spot_price

                        # Call options
                        call_option = option.get("call_options", {})
                        call_market_data = call_option.get("market_data", {})
                        call_greeks = call_option.get("option_greeks", {})

                        if call_market_data:
                            option_chain.append(
                                {
                                    "strike_price": strike_price,
                                    "expiry": expiry,
                                    "option_type": "CE",
                                    "oi": call_market_data.get("oi", 0),
                                    "oi_change": call_market_data.get(
                                        "oi_day_change", 0
                                    ),
                                    "volume": call_market_data.get("volume", 0),
                                    "ltp": call_market_data.get("ltp", 0),
                                    "bid_price": call_market_data.get("bid_price", 0),
                                    "ask_price": call_market_data.get("ask_price", 0),
                                    "iv": call_greeks.get("iv", 0),
                                    "delta": call_greeks.get("delta", 0),
                                    "theta": call_greeks.get("theta", 0),
                                    "gamma": call_greeks.get("gamma", 0),
                                    "vega": call_greeks.get("vega", 0),
                                    "instrument_key": call_option.get(
                                        "instrument_key", ""
                                    ),
                                }
                            )

                        # Put options
                        put_option = option.get("put_options", {})
                        put_market_data = put_option.get("market_data", {})
                        put_greeks = put_option.get("option_greeks", {})

                        if put_market_data:
                            option_chain.append(
                                {
                                    "strike_price": strike_price,
                                    "expiry": expiry,
                                    "option_type": "PE",
                                    "oi": put_market_data.get("oi", 0),
                                    "oi_change": put_market_data.get(
                                        "oi_day_change", 0
                                    ),
                                    "volume": put_market_data.get("volume", 0),
                                    "ltp": put_market_data.get("ltp", 0),
                                    "bid_price": put_market_data.get("bid_price", 0),
                                    "ask_price": put_market_data.get("ask_price", 0),
                                    "iv": put_greeks.get("iv", 0),
                                    "delta": put_greeks.get("delta", 0),
                                    "theta": put_greeks.get("theta", 0),
                                    "gamma": put_greeks.get("gamma", 0),
                                    "vega": put_greeks.get("vega", 0),
                                    "instrument_key": put_option.get(
                                        "instrument_key", ""
                                    ),
                                }
                            )

                    return option_chain, spot_price, None
                else:
                    error_msg = data.get("message", "Unknown error")
                    return None, None, f"API Error: {error_msg}"
            elif response.status_code == 401:
                return (
                    None,
                    None,
                    "Token expired. Please get a new token from Upstox and update in Token Management.",
                )
            elif response.status_code == 429:
                return (
                    None,
                    None,
                    "Rate limited - Please wait a few seconds and try again",
                )
            elif response.status_code == 400:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("errors", [{}])[0].get(
                        "message", "Bad request"
                    )
                    if "expiry" in error_msg.lower() or "date" in error_msg.lower():
                        return (
                            None,
                            None,
                            f"Invalid expiry date: {expiry_date}. Select a valid expiry (usually Thursday).",
                        )
                except:
                    error_msg = "Bad request"
                return None, None, f"Bad Request: {error_msg}"
            else:
                return None, None, f"HTTP Error: {response.status_code}"

        except requests.exceptions.Timeout:
            return None, None, "Request timed out - Try again"
        except Exception as e:
            return None, None, f"Error: {str(e)}"

    def get_option_chain_for_stock(self, symbol, expiry_date):
        """
        Fetch option chain for a stock symbol

        Args:
            symbol: Stock symbol (e.g., "RELIANCE", "TCS")
            expiry_date: Expiry date in YYYY-MM-DD format

        Returns:
            Tuple of (option_chain_list, spot_price, error_message)
        """
        try:
            isin = get_stock_isin_cached(symbol)
            if not isin:
                return None, None, f"Could not find ISIN for {symbol}"

            instrument_key = f"NSE_EQ|{isin}"
            return self.get_option_chain(instrument_key, expiry_date)
        except Exception as e:
            return None, None, f"Error: {str(e)}"

    def get_ltp(self, instrument_key):
        """
        Get Last Traded Price for an instrument

        Args:
            instrument_key: Instrument key (e.g., "NSE_FO|RELIANCE24DEC2900CE")

        Returns:
            Tuple of (ltp, error_message)
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"

            import urllib.parse

            encoded_key = urllib.parse.quote(instrument_key, safe="")

            url = f"{BASE_URL}/v2/market-quote/ltp?instrument_key={encoded_key}"
            response = HTTP_SESSION.get(url, headers=headers, timeout=API_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    ltp_data = data.get("data", {})
                    # Get LTP from response
                    for key, value in ltp_data.items():
                        if "last_price" in value:
                            return value["last_price"], None
                    return None, "LTP not found in response"
                else:
                    return None, f"API Error: {data.get('message', 'Unknown error')}"
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, f"Error: {str(e)}"

    def place_order(
        self,
        instrument_key,
        quantity,
        transaction_type,
        order_type="MARKET",
        price=None,
        trigger_price=None,
        product="D",
    ):
        """
        Place an order via Upstox API

        Args:
            instrument_key: Instrument key (e.g., "NSE_FO|RELIANCE24DEC2900CE")
            quantity: Number of lots
            transaction_type: "BUY" or "SELL"
            order_type: "MARKET", "LIMIT", "SL", "SL-M"
            price: Limit price (required for LIMIT orders)
            trigger_price: Trigger price (required for SL orders)
            product: "I" (Intraday), "D" (Delivery), "CO" (Cover Order), "OCO" (One Cancels Other)

        Returns:
            Tuple of (order_id, error_message)
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"

            headers["Content-Type"] = "application/json"

            order_data = {
                "quantity": quantity,
                "product": product,
                "validity": "DAY",
                "price": price if price else 0,
                "tag": "SCREENER_AUTO",
                "instrument_token": instrument_key,
                "order_type": order_type,
                "transaction_type": transaction_type,
                "disclosed_quantity": 0,
                "trigger_price": trigger_price if trigger_price else 0,
                "is_amo": False,
            }

            url = f"{BASE_URL}/v2/order/place"
            response = HTTP_SESSION.post(
                url, headers=headers, json=order_data, timeout=API_TIMEOUT
            )

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    order_id = data.get("data", {}).get("order_id")
                    return order_id, None
                else:
                    return None, f"API Error: {data.get('message', 'Unknown error')}"
            elif response.status_code == 401:
                return None, "Authentication failed - Token may be expired"
            else:
                try:
                    error_data = response.json()
                    error_msg = error_data.get(
                        "message",
                        error_data.get("errors", [{}])[0].get(
                            "message", "Unknown error"
                        ),
                    )
                except:
                    error_msg = f"HTTP {response.status_code}"
                return None, error_msg
        except Exception as e:
            return None, f"Error: {str(e)}"

    def get_order_status(self, order_id):
        """
        Get status of a specific order

        Args:
            order_id: Order ID to check

        Returns:
            Tuple of (order_details, error_message)
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"

            url = f"{BASE_URL}/v2/order/details?order_id={order_id}"
            response = HTTP_SESSION.get(url, headers=headers, timeout=API_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return data.get("data"), None
                else:
                    return None, f"API Error: {data.get('message', 'Unknown error')}"
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, f"Error: {str(e)}"

    def get_order_book(self):
        """
        Get all orders for today

        Returns:
            Tuple of (orders_list, error_message)
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"

            url = f"{BASE_URL}/v2/order/retrieve-all"
            response = HTTP_SESSION.get(url, headers=headers, timeout=API_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return data.get("data", []), None
                else:
                    return None, f"API Error: {data.get('message', 'Unknown error')}"
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, f"Error: {str(e)}"

    def get_positions(self):
        """
        Get current positions

        Returns:
            Tuple of (positions_list, error_message)
        """
        try:
            headers = self.get_headers()
            if not headers:
                return None, "No valid API token found"

            url = f"{BASE_URL}/v2/portfolio/short-term-positions"
            response = HTTP_SESSION.get(url, headers=headers, timeout=API_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return data.get("data", []), None
                else:
                    return None, f"API Error: {data.get('message', 'Unknown error')}"
            else:
                return None, f"HTTP {response.status_code}"
        except Exception as e:
            return None, f"Error: {str(e)}"

    def get_nearest_expiry(self, symbol):
        """Get the nearest expiry date for a stock's options"""
        try:
            contracts, error, _ = self.get_option_contracts_for_stock(symbol)
            if contracts:
                expiry_dates = set()
                for c in contracts:
                    if c.get("expiry"):
                        expiry_dates.add(c.get("expiry"))
                if expiry_dates:
                    sorted_dates = sorted(expiry_dates)
                    return sorted_dates[0], None  # Return nearest expiry
            return None, error if error else "No expiry dates found"
        except Exception as e:
            return None, str(e)

    def find_itm_option(self, symbol, current_price, option_type, expiry_date=None):
        """
        Find the closest In-The-Money (ITM) option contract

        Args:
            symbol: Stock symbol (e.g., "RELIANCE")
            current_price: Current stock price
            option_type: "CE" for Call, "PE" for Put
            expiry_date: Optional expiry date, if None will use nearest expiry

        Returns:
            Tuple of (contract, error_message)
        """
        try:
            # Get option contracts
            contracts, error, instrument_key = self.get_option_contracts_for_stock(
                symbol, expiry_date
            )

            if not contracts:
                return None, error if error else "No contracts found"

            # Filter by option type
            filtered_contracts = [
                c for c in contracts if c.get("instrument_type") == option_type
            ]

            if not filtered_contracts:
                return None, f"No {option_type} contracts found"

            # Find ITM options
            itm_contracts = []
            for c in filtered_contracts:
                strike = c.get("strike_price", 0)
                if option_type == "CE":
                    # For Call: ITM when strike < current price
                    if strike < current_price:
                        itm_contracts.append(
                            (c, current_price - strike)
                        )  # Distance from strike
                else:  # PE
                    # For Put: ITM when strike > current price
                    if strike > current_price:
                        itm_contracts.append(
                            (c, strike - current_price)
                        )  # Distance from strike

            if not itm_contracts:
                return (
                    None,
                    f"No ITM {option_type} contracts found for price {current_price}",
                )

            # Sort by distance (closest ITM first)
            itm_contracts.sort(key=lambda x: x[1])

            # Return the closest ITM contract
            return itm_contracts[0][0], None

        except Exception as e:
            return None, f"Error: {str(e)}"


# ----------------- Option Trading Strategy -----------------
def execute_option_trade_strategy(api, stocks_to_trade, profit_target_pct=2.5):
    """
    Execute the option trading strategy:
    1. At 3:05 PM, identify the closest ITM option contract
    2. Purchase at current market price
    3. Place limit sell order at 2.5% profit

    Args:
        api: UpstoxAPI instance
        stocks_to_trade: List of stock dictionaries with 'symbol' and 'trend'
        profit_target_pct: Target profit percentage (default 2.5%)

    Returns:
        List of trade results
    """
    trade_results = []

    for stock in stocks_to_trade:
        symbol = stock.get("symbol")
        trend = stock.get("trend")
        current_price = stock.get("current_price", 0)

        result = {
            "symbol": symbol,
            "trend": trend,
            "current_price": current_price,
            "status": "pending",
            "option_type": None,
            "contract": None,
            "buy_order_id": None,
            "sell_order_id": None,
            "error": None,
        }

        try:
            # Determine option type based on trend
            if trend == "Bullish":
                option_type = "CE"  # Call option for bullish
            elif trend == "Bearish":
                option_type = "PE"  # Put option for bearish
            else:
                result["error"] = "Stock is not Bullish or Bearish"
                result["status"] = "skipped"
                trade_results.append(result)
                continue

            result["option_type"] = option_type

            # Get nearest expiry
            expiry_date, error = api.get_nearest_expiry(symbol)
            if not expiry_date:
                result["error"] = f"Could not get expiry: {error}"
                result["status"] = "failed"
                trade_results.append(result)
                continue

            result["expiry_date"] = expiry_date

            # Find closest ITM option
            contract, error = api.find_itm_option(
                symbol, current_price, option_type, expiry_date
            )
            if not contract:
                result["error"] = f"Could not find ITM option: {error}"
                result["status"] = "failed"
                trade_results.append(result)
                continue

            result["contract"] = contract
            result["strike_price"] = contract.get("strike_price")
            result["trading_symbol"] = contract.get("trading_symbol")
            result["lot_size"] = contract.get("lot_size", 1)

            # Get instrument key for the option
            option_instrument_key = contract.get("instrument_key")
            if not option_instrument_key:
                result["error"] = "No instrument key in contract"
                result["status"] = "failed"
                trade_results.append(result)
                continue

            # Get LTP for the option
            option_ltp, error = api.get_ltp(option_instrument_key)
            if not option_ltp:
                result["error"] = f"Could not get option LTP: {error}"
                result["status"] = "failed"
                trade_results.append(result)
                continue

            result["option_ltp"] = option_ltp

            # Calculate buy limit price (slightly above LTP for better fill)
            buy_limit_price = round(
                option_ltp * (1 + DEFAULT_BUY_BUFFER_PCT / 100), 2
            )  # Buffer above LTP
            result["buy_limit_price"] = buy_limit_price

            # Calculate sell target price (default profit target)
            sell_target_price = round(option_ltp * (1 + profit_target_pct / 100), 2)
            result["sell_target_price"] = sell_target_price

            # Place BUY order (LIMIT order - MARKET not allowed for options)
            buy_order_id, error = api.place_order(
                instrument_key=option_instrument_key,
                quantity=result["lot_size"],
                transaction_type="BUY",
                order_type="LIMIT",
                price=buy_limit_price,
                product="I",  # Intraday
            )

            if not buy_order_id:
                result["error"] = f"Buy order failed: {error}"
                result["status"] = "buy_failed"
                trade_results.append(result)
                continue

            result["buy_order_id"] = buy_order_id

            # Place SELL order (Limit order at 2.5% profit)
            sell_order_id, error = api.place_order(
                instrument_key=option_instrument_key,
                quantity=result["lot_size"],
                transaction_type="SELL",
                order_type="LIMIT",
                price=sell_target_price,
                product="I",  # Intraday
            )

            if not sell_order_id:
                result["error"] = f"Sell order failed: {error}"
                result["status"] = "sell_failed"
                trade_results.append(result)
                continue

            result["sell_order_id"] = sell_order_id
            result["status"] = "success"

        except Exception as e:
            result["error"] = str(e)
            result["status"] = "exception"

        trade_results.append(result)

    return trade_results


# ----------------- Mock Data Generator -----------------
def generate_mock_historical_data(symbol, days=200):
    random.seed(hash(symbol) % 2**32)
    base_price = random.uniform(100, 5000)
    volatility = random.uniform(0.01, 0.03)
    data = []
    current_price = base_price

    for i in range(days):
        date = (datetime.now() - timedelta(days=days - i - 1)).strftime("%Y-%m-%d")
        change = random.gauss(0.0002, volatility)
        current_price *= 1 + change
        daily_range = current_price * random.uniform(0.01, 0.03)
        open_price = current_price + random.uniform(-daily_range / 2, daily_range / 2)
        high_price = max(open_price, current_price) + random.uniform(0, daily_range / 2)
        low_price = min(open_price, current_price) - random.uniform(0, daily_range / 2)
        data.append(
            {
                "date": date,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(current_price, 2),
                "volume": int(random.uniform(100000, 10000000)),
            }
        )
    return data[::-1]


# ----------------- Technical Analysis -----------------
def calculate_ema(prices, period):
    if len(prices) < period:
        return [None] * len(prices)
    ema = [None] * (period - 1) + [sum(prices[:period]) / period]
    multiplier = 2 / (period + 1)
    for i in range(period, len(prices)):
        ema.append((prices[i] * multiplier) + (ema[-1] * (1 - multiplier)))
    return ema


def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None, None
    fast_ema = calculate_ema(prices, fast)
    slow_ema = calculate_ema(prices, slow)
    macd = [f - s if f and s else None for f, s in zip(fast_ema, slow_ema)]
    valid_macd = [m for m in macd if m is not None]
    if len(valid_macd) < signal:
        return macd, [None] * len(macd), [None] * len(macd)
    signal_ema = calculate_ema(valid_macd, signal)
    signal_line = [None] * (len(macd) - len(signal_ema)) + signal_ema
    histogram = [m - s if m and s else None for m, s in zip(macd, signal_line)]
    return macd, signal_line, histogram


def calculate_ichimoku(data, tenkan=9, kijun=26, senkou_b=52):
    results = []
    for i in range(len(data)):
        result = {"date": data[i]["date"]}
        if i >= tenkan - 1:
            highs = [data[j]["high"] for j in range(i - tenkan + 1, i + 1)]
            lows = [data[j]["low"] for j in range(i - tenkan + 1, i + 1)]
            result["tenkan_sen"] = (max(highs) + min(lows)) / 2
        else:
            result["tenkan_sen"] = None
        if i >= kijun - 1:
            highs = [data[j]["high"] for j in range(i - kijun + 1, i + 1)]
            lows = [data[j]["low"] for j in range(i - kijun + 1, i + 1)]
            result["kijun_sen"] = (max(highs) + min(lows)) / 2
        else:
            result["kijun_sen"] = None
        result["senkou_span_a"] = (
            (result["tenkan_sen"] + result["kijun_sen"]) / 2
            if result["tenkan_sen"] and result["kijun_sen"]
            else None
        )
        if i >= senkou_b - 1:
            highs = [data[j]["high"] for j in range(i - senkou_b + 1, i + 1)]
            lows = [data[j]["low"] for j in range(i - senkou_b + 1, i + 1)]
            result["senkou_span_b"] = (max(highs) + min(lows)) / 2
        else:
            result["senkou_span_b"] = None
        result["chikou_span"] = data[i]["close"]
        results.append(result)
    return results


def calculate_indicators(data):
    if not data or len(data) < 60:
        return None, (None, None, None)
    closes = [d["close"] for d in data]
    macd, signal, histogram = calculate_macd(closes)
    ichimoku = calculate_ichimoku(data)
    results = []
    for i in range(len(data)):
        results.append(
            {
                "date": data[i]["date"],
                "close": data[i]["close"],
                "open": data[i]["open"],
                "high": data[i]["high"],
                "low": data[i]["low"],
                "macd": macd[i] if macd and i < len(macd) else None,
                "macd_signal": signal[i] if signal and i < len(signal) else None,
                "macd_hist": histogram[i] if histogram and i < len(histogram) else None,
                "tenkan_sen": ichimoku[i]["tenkan_sen"],
                "kijun_sen": ichimoku[i]["kijun_sen"],
                "senkou_span_a": ichimoku[i]["senkou_span_a"],
                "senkou_span_b": ichimoku[i]["senkou_span_b"],
                "chikou_span": ichimoku[i]["chikou_span"],
            }
        )
    return results[::-1], (macd, signal, histogram)


# ----------------- Database Functions -----------------
def save_historical_data(symbol, data):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            for day in data:
                c.execute(
                    "INSERT OR REPLACE INTO daily_prices (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        symbol,
                        day["date"],
                        day["open"],
                        day["high"],
                        day["low"],
                        day["close"],
                        day["volume"],
                    ),
                )
            conn.commit()
        return True
    except Exception:
        return False


def get_historical_data(symbol, days=200):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                "SELECT date, open, high, low, close, volume FROM daily_prices WHERE symbol = ? ORDER BY date DESC LIMIT ?",
                (symbol, days),
            )
            return [
                {
                    "date": r[0],
                    "open": r[1],
                    "high": r[2],
                    "low": r[3],
                    "close": r[4],
                    "volume": r[5],
                }
                for r in c.fetchall()
            ]
    except Exception:
        return []


def get_stock_list():
    """Get stock list from database, with validation and fallback to STOCK_LIST"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT symbol, name, isin, has_options FROM stocks")
            rows = c.fetchall()

            # Validate that we have stocks and ISIN is not null
            if rows:
                stocks = []
                for r in rows:
                    symbol, name, isin, has_options = r
                    # If ISIN is null, try to get it from STOCK_LIST
                    if not isin:
                        for stock in STOCK_LIST:
                            if stock["symbol"] == symbol:
                                isin = stock["isin"]
                                break
                    stocks.append(
                        {
                            "symbol": symbol,
                            "name": name,
                            "isin": isin,
                            "has_options": has_options,
                        }
                    )

                if stocks:
                    return stocks
    except Exception:
        pass

    # Fallback to STOCK_LIST
    return STOCK_LIST


# ----------------- Stock Processing -----------------
def fetch_single_stock_data(
    stock, api, use_live_data, intraday_interval, use_mock=False
):
    try:
        symbol = stock["symbol"]
        if use_mock:
            data_desc = generate_mock_historical_data(symbol, days=200)
        else:
            data_desc = get_historical_data(symbol, days=200)
            if not data_desc:
                data_desc = api.get_historical_data(symbol, days=200)
                if data_desc:
                    save_historical_data(symbol, data_desc)
                else:
                    data_desc = generate_mock_historical_data(symbol, days=200)

        if not data_desc or len(data_desc) < 60:
            return None
        
        current_price = data_desc[0]["close"]
        high_price = data_desc[0]["high"]
        low_price = data_desc[0]["low"]

        # Fetch current/live price if enabled
        if use_live_data and not use_mock:
            try:
                intraday_data, error = api.get_current_data(
                    symbol, interval_minutes=intraday_interval
                )
                if intraday_data and len(intraday_data) > 0:
                    # Get the most recent candle for current price
                    current_price = intraday_data[0]["close"]
                    # Get the high and low from today's intraday data
                    high_price = max([c["high"] for c in intraday_data])
                    low_price = min([c["low"] for c in intraday_data])
                    intradf=pd.DataFrame(intraday_data)
                    open_price = intradf.sort_values('datetime', ascending=True).iloc[0]['open']
                    new_row_dict = {
                        'date': date.today(),
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': current_price,
                        'volume': 22
                    }
                    data_desc.append(new_row_dict)
                    for row in data_desc:
                        # If the date is a string, convert it to a datetime object
                        if isinstance(row['date'], str):
                            row['date'] = datetime.strptime(row['date'], '%Y-%m-%d').date()

                    # Now the sort will work perfectly
                    data_desc.sort(key=lambda x: x['date'], reverse=True)

            except Exception as e:
                if(symbol == "IDEA"):
                    print("Exception Type:", type(e).__name__)
                    print("Exception Message:", e)
                # Combined output
                    print(f"Error: {type(e).__name__} – {e}")
                pass  # Fall back to historical data

        data_asc = data_desc[::-1]
        if(symbol == "IDEA"):
            print(symbol)
            print(data_desc)
        indicators_desc, _ = calculate_indicators(data_asc)

        if not indicators_desc or len(indicators_desc) < 6:
            return None

        latest = indicators_desc[0]
        previous = indicators_desc[1]

        if (
            latest["senkou_span_b"] is None
            or latest["macd_hist"] is None
            or previous["macd_hist"] is None
        ):
            return None

        senkou_span_b = latest["senkou_span_b"]
        latest_macd_hist = latest["macd_hist"]
        previous_macd_hist = previous["macd_hist"]

        cloud_bullish = current_price > senkou_span_b
        cloud_bearish = current_price < senkou_span_b
        macd_hist_increasing = latest_macd_hist > previous_macd_hist
        macd_hist_decreasing = latest_macd_hist < previous_macd_hist

        # Calculate MACD differences for last 5 days
        macd_diffs = []
        for i in range(5):
            if i + 1 < len(indicators_desc):
                curr = indicators_desc[i]["macd_hist"]
                prev = indicators_desc[i + 1]["macd_hist"]
                if curr is not None and prev is not None:
                    macd_diffs.append(round(curr - prev, 4))
                else:
                    macd_diffs.append(0)
            else:
                macd_diffs.append(0)

        # Get MACD hist values for last 6 days
        macd_hist_values = []
        for i in range(6):
            if i < len(indicators_desc) and indicators_desc[i]["macd_hist"] is not None:
                macd_hist_values.append(
                    {
                        "day": i,
                        "date": indicators_desc[i]["date"],
                        "macd_hist": round(indicators_desc[i]["macd_hist"], 4),
                        "close": round(indicators_desc[i]["close"], 2),
                    }
                )
        if current_price > open_price:
            intraday_strength_pct = (
                ((high_price - current_price) / current_price) * 100
                if current_price > 0
                else 0
            )
        else:
            intraday_strength_pct = (
                ((current_price - low_price) / current_price) * 100
                if current_price > 0
                else 0
            )
        # Additional Ichimoku checks for 26 periods ago
        ichimoku_bullish_pass = False
        ichimoku_bearish_pass = False
        if len(indicators_desc) > 26:
            cloud_a_26 = indicators_desc[26]["senkou_span_a"]
            cloud_b_26 = indicators_desc[26]["senkou_span_b"]
            candle_26_close = indicators_desc[26]["close"]
            chikou_span = latest["chikou_span"]

            # Bullish checks
            cloud_color_bullish_26 = cloud_a_26 > cloud_b_26 if cloud_a_26 and cloud_b_26 else False
            cloud_position_bullish_26 = current_price > cloud_a_26 and current_price > cloud_b_26 if cloud_a_26 and cloud_b_26 else False
            chikou_above = chikou_span > candle_26_close if chikou_span and candle_26_close else False
            ichimoku_bullish_pass = cloud_position_bullish_26 and cloud_color_bullish_26 and chikou_above

            # Bearish checks
            cloud_color_bearish_26 = cloud_b_26 > cloud_a_26 if cloud_a_26 and cloud_b_26 else False
            cloud_position_bearish_26 = current_price < cloud_a_26 and current_price < cloud_b_26 if cloud_a_26 and cloud_b_26 else False
            chikou_below = chikou_span < candle_26_close if chikou_span and candle_26_close else False
            ichimoku_bearish_pass = cloud_position_bearish_26 and cloud_color_bearish_26 and chikou_below

        # MACD check: Histogram or Signal Line > 0
        macd_positive = (latest_macd_hist > 0 and (latest["macd_signal"] and latest["macd_signal"] > 0))
        
        # MACD check for bearish: Histogram or Signal Line < 0
        macd_negative = (latest_macd_hist < 0 and (latest["macd_signal"] and latest["macd_signal"] < 0))
        
        print(symbol)
        print(latest)
        print("--------------------------------------------------------------------------------")
        if cloud_bullish and macd_positive and macd_hist_increasing and ichimoku_bullish_pass and latest['close'] > latest['open']:
            trend, color = "Bullish", "green"
        elif cloud_bearish and macd_negative and macd_hist_decreasing and ichimoku_bearish_pass and latest['close'] < latest['open']:
            trend, color = "Bearish", "red"
        else:
            trend, color = "Neutral/Mixed", "gray"

        return {
            "symbol": symbol,
            "name": stock["name"],
            "current_price": round(current_price, 2),
            "high_price": round(high_price, 2),
            "low_price": round(low_price, 2),
            "senkou_span_b": round(senkou_span_b, 2),
            "macd_hist": round(latest_macd_hist, 4),
            "prev_macd_hist": round(previous_macd_hist, 4),
            "trend": trend,
            "color": color,
            "macd_diffs_5d": macd_diffs,
            "macd_hist_values": macd_hist_values,
            "intraday_strength_pct": round(intraday_strength_pct, 4),
            "indicators": indicators_desc,
            "raw_data": data_desc,
            "last_updated": datetime.now().strftime("%H:%M:%S"),
        }
    except Exception:
        return None


def screen_stocks(
    stock_list,
    api,
    use_live_data,
    intraday_interval,
    progress_bar,
    status_text,
    use_mock=False,
):
    """Screen stocks using parallel processing for faster results"""

    results = []
    total = len(stock_list)
    completed = 0

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
        # Submit all tasks
        future_to_stock = {
            executor.submit(
                fetch_single_stock_data,
                stock,
                api,
                use_live_data,
                intraday_interval,
                use_mock,
            ): stock
            for stock in stock_list
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_stock):
            stock = future_to_stock[future]
            completed += 1

            try:
                result = future.result(timeout=10)  # Reduced timeout from 30 to 10
                if result:
                    results.append(result)
            except Exception:
                pass

            # Update progress
            progress_bar.progress(completed / total)
            status_text.text(
                f"Analyzing stocks... ({completed}/{total}) - Found {len(results)} signals"
            )

    return results


# ----------------- Buy Dialog for Option Trading -----------------
def show_buy_dialog():
    """Display the buy dialog for executing option trades on top 3 stocks"""

    st.markdown("---")
    st.markdown("## 🛒 Option Trading - Buy Top 3 Stocks")

    # Check current time
    current_time = datetime.now()
    current_hour = current_time.hour
    current_minute = current_time.minute

    # Strategy info
    with st.expander("📋 Trading Strategy Details", expanded=True):
        st.markdown("""
        **Strategy Rules:**
        1. ⏰ **Execution Time:** 3:05 PM (or manual trigger)
        2. 🎯 **Selection:** Closest In-The-Money (ITM) option contract
        3. 💰 **Buy:** At current market price
        4. 📈 **Sell Order:** Limit order at **+2.5% profit**

        **Direction Rules:**
        - 🟢 **Bullish Stock** → Buy **CALL (CE)** option
        - 🔴 **Bearish Stock** → Buy **PUT (PE)** option
        """)

    # Get top stocks from session state
    if not st.session_state.get("stock_list_data"):
        st.warning("⚠️ No stock data available. Please refresh the screener first.")
        if st.button("❌ Close", key="close_buy_dialog"):
            st.session_state.show_buy_dialog = False
            st.rerun()
        return

    stock_data = st.session_state.stock_list_data

    # Get top 3 bullish and bearish stocks
    bullish_stocks = [s for s in stock_data if s["trend"] == "Bullish"]
    bearish_stocks = [s for s in stock_data if s["trend"] == "Bearish"]

    # Sort by intraday strength
    bullish_stocks.sort(key=lambda x: x["intraday_strength_pct"], reverse=False)
    bearish_stocks.sort(key=lambda x: x["intraday_strength_pct"], reverse=False)

    # Selection options
    st.markdown("### 📊 Select Stocks to Trade")

    trade_option = st.radio(
        "Choose stocks to trade:",
        [
            "Top 3 Bullish (Buy CALL)",
            "Top 3 Bearish (Buy PUT)",
            "Top 3 Mixed (Bullish + Bearish)",
        ],
        key="trade_option",
    )

    # Determine stocks to trade
    stocks_to_trade = []
    if trade_option == "Top 3 Bullish (Buy CALL)":
        stocks_to_trade = bullish_stocks[:3]
    elif trade_option == "Top 3 Bearish (Buy PUT)":
        stocks_to_trade = bearish_stocks[:3]
    else:  # Mixed
        stocks_to_trade = (
            bullish_stocks[:2] + bearish_stocks[:1]
            if len(bullish_stocks) >= 2
            else bullish_stocks + bearish_stocks[: 3 - len(bullish_stocks)]
        )

    if not stocks_to_trade:
        st.warning("⚠️ No stocks available for the selected option.")
        if st.button("❌ Close", key="close_buy_dialog_no_stocks"):
            st.session_state.show_buy_dialog = False
            st.rerun()
        return

    # Display selected stocks
    st.markdown("### 🎯 Selected Stocks for Trading")

    for i, stock in enumerate(stocks_to_trade, 1):
        option_type = "CALL (CE)" if stock["trend"] == "Bullish" else "PUT (PE)"
        trend_color = "🟢" if stock["trend"] == "Bullish" else "🔴"

        st.markdown(
            f"""
        <div style="background: {"#d4edda" if stock["trend"] == "Bullish" else "#f8d7da"}; padding: 15px; border-radius: 10px; margin: 10px 0;">
            <h4 style="margin: 0;">{trend_color} {i}. {stock["symbol"]} - {stock["name"]}</h4>
            <p style="margin: 5px 0;">💰 <strong>Current Price:</strong> ₹{stock["current_price"]:,.2f}</p>
            <p style="margin: 5px 0;">📊 <strong>Trend:</strong> {stock["trend"]}</p>
            <p style="margin: 5px 0;">🎯 <strong>Option Type:</strong> {option_type}</p>
            <p style="margin: 5px 0;">💪 <strong>Intraday Strength:</strong> {stock["intraday_strength_pct"]:.4f}%</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    # Profit target
    profit_target = st.slider(
        "📈 Profit Target (%)",
        min_value=1.0,
        max_value=10.0,
        value=DEFAULT_PROFIT_TARGET_PCT,
        step=0.5,
        key="profit_target",
    )

    st.markdown("---")

    # Time check warning
    if not (current_hour == 15 and current_minute >= 5):
        st.warning(
            f"⚠️ Current time is {current_time.strftime('%H:%M')}. Strategy recommends execution at 3:05 PM. You can still execute manually."
        )
    else:
        st.success(
            f"✅ Current time is {current_time.strftime('%H:%M')} - Optimal execution window!"
        )

    # Action buttons
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🚀 Execute Trades", key="execute_trades_btn", type="primary"):
            execute_trades_with_progress(stocks_to_trade, profit_target)

    with col2:
        if st.button("🔍 Preview Only", key="preview_trades_btn"):
            preview_trades(stocks_to_trade, profit_target)

    with col3:
        if st.button("❌ Cancel", key="cancel_buy_dialog"):
            st.session_state.show_buy_dialog = False
            st.rerun()


def preview_trades(stocks_to_trade, profit_target):
    """Preview the trades without executing"""
    st.markdown("### 🔍 Trade Preview")

    token_manager = TokenManager()
    api = UpstoxAPI(token_manager)

    for stock in stocks_to_trade:
        symbol = stock["symbol"]
        trend = stock["trend"]
        current_price = stock["current_price"]
        option_type = "CE" if trend == "Bullish" else "PE"

        st.markdown(f"**{symbol}** ({trend})")

        with st.spinner(f"Fetching ITM {option_type} option for {symbol}..."):
            # Get nearest expiry
            expiry_date, error = api.get_nearest_expiry(symbol)
            if error:
                st.error(f"❌ Could not get expiry: {error}")
                continue

            # Find ITM option
            contract, error = api.find_itm_option(
                symbol, current_price, option_type, expiry_date
            )
            if error:
                st.error(f"❌ Could not find ITM option: {error}")
                continue

            strike_price = contract.get("strike_price", 0)
            trading_symbol = contract.get("trading_symbol", "N/A")
            lot_size = contract.get("lot_size", 1)

            st.success(f"""
            ✅ **Found ITM Option:**
            - Trading Symbol: `{trading_symbol}`
            - Strike Price: ₹{strike_price:,.2f}
            - Lot Size: {lot_size}
            - Expiry: {expiry_date}
            - Target Sell Price: +{profit_target}% from buy price
            """)


def execute_trades_with_progress(stocks_to_trade, profit_target):
    """Execute trades with progress display"""
    st.markdown("### 🚀 Executing Trades...")

    token_manager = TokenManager()
    api = UpstoxAPI(token_manager)

    # Check if API is authenticated
    if not token_manager.get_token():
        st.error("❌ Not authenticated. Please authenticate with Upstox first.")
        return

    # Execute trades
    progress_bar = st.progress(0)

    results = []
    total = len(stocks_to_trade)

    for i, stock in enumerate(stocks_to_trade):
        symbol = stock["symbol"]
        trend = stock["trend"]
        current_price = stock["current_price"]
        option_type = "CE" if trend == "Bullish" else "PE"

        status_container = st.empty()
        status_container.info(f"🔄 Processing {symbol}... ({i + 1}/{total})")

        result = {
            "symbol": symbol,
            "trend": trend,
            "option_type": option_type,
            "status": "pending",
        }

        try:
            # Get nearest expiry
            expiry_date, error = api.get_nearest_expiry(symbol)
            if error:
                result["status"] = "failed"
                result["error"] = f"Expiry error: {error}"
                results.append(result)
                progress_bar.progress((i + 1) / total)
                continue

            result["expiry_date"] = expiry_date

            # Find ITM option
            contract, error = api.find_itm_option(
                symbol, current_price, option_type, expiry_date
            )
            if error:
                result["status"] = "failed"
                result["error"] = f"ITM option error: {error}"
                results.append(result)
                progress_bar.progress((i + 1) / total)
                continue

            result["trading_symbol"] = contract.get("trading_symbol")
            result["strike_price"] = contract.get("strike_price")
            result["lot_size"] = contract.get("lot_size", 1)

            option_instrument_key = contract.get("instrument_key")

            # Get LTP
            option_ltp, error = api.get_ltp(option_instrument_key)
            if error:
                result["status"] = "failed"
                result["error"] = f"LTP error: {error}"
                results.append(result)
                progress_bar.progress((i + 1) / total)
                continue

            result["buy_price"] = option_ltp
            result["sell_target"] = round(option_ltp * (1 + profit_target / 100), 2)

            # Calculate buy limit price (slightly above LTP to ensure execution)
            buy_limit_price = round(
                option_ltp * (1 + DEFAULT_BUY_BUFFER_PCT / 100), 2
            )  # Buffer above LTP for better fill
            result["buy_limit_price"] = buy_limit_price

            # Place BUY order (LIMIT order since MARKET not allowed for options)
            buy_order_id, error = api.place_order(
                instrument_key=option_instrument_key,
                quantity=result["lot_size"],
                transaction_type="BUY",
                order_type="LIMIT",
                price=buy_limit_price,
                product="I",
            )

            if error:
                result["status"] = "buy_failed"
                result["error"] = f"Buy order error: {error}"
                results.append(result)
                progress_bar.progress((i + 1) / total)
                continue

            result["buy_order_id"] = buy_order_id

            # Place SELL order (limit at profit target)
            sell_order_id, error = api.place_order(
                instrument_key=option_instrument_key,
                quantity=result["lot_size"],
                transaction_type="SELL",
                order_type="LIMIT",
                price=result["sell_target"],
                product="I",
            )

            if error:
                result["status"] = "sell_failed"
                result["error"] = f"Sell order error: {error}"
            else:
                result["sell_order_id"] = sell_order_id
                result["status"] = "success"

        except Exception as e:
            result["status"] = "exception"
            result["error"] = str(e)

        results.append(result)
        progress_bar.progress((i + 1) / total)
        status_container.empty()

    # Display results
    st.markdown("### 📊 Trade Results")

    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - success_count

    if success_count > 0:
        st.success(f"✅ **{success_count}** trades executed successfully!")
    if failed_count > 0:
        st.error(f"❌ **{failed_count}** trades failed.")

    # Store results in session state for verification
    st.session_state.last_trade_results = results

    for result in results:
        if result["status"] == "success":
            st.markdown(
                f"""
            <div style="background: #d4edda; padding: 15px; border-radius: 10px; margin: 10px 0;">
                <h4 style="margin: 0; color: #155724;">✅ {result["symbol"]} - SUCCESS</h4>
                <p style="margin: 5px 0;">📄 <strong>Contract:</strong> {result.get("trading_symbol", "N/A")}</p>
                <p style="margin: 5px 0;">📊 <strong>LTP:</strong> ₹{result.get("buy_price", 0):,.2f}</p>
                <p style="margin: 5px 0;">💰 <strong>Buy Limit Price:</strong> ₹{result.get("buy_limit_price", 0):,.2f}</p>
                <p style="margin: 5px 0;">🎯 <strong>Sell Target:</strong> ₹{result.get("sell_target", 0):,.2f} (+{profit_target}%)</p>
                <p style="margin: 5px 0;">📋 <strong>Buy Order ID:</strong> {result.get("buy_order_id", "N/A")}</p>
                <p style="margin: 5px 0;">📋 <strong>Sell Order ID:</strong> {result.get("sell_order_id", "N/A")}</p>
            </div>
            """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""
            <div style="background: #f8d7da; padding: 15px; border-radius: 10px; margin: 10px 0;">
                <h4 style="margin: 0; color: #721c24;">❌ {result["symbol"]} - {result["status"].upper()}</h4>
                <p style="margin: 5px 0;">⚠️ <strong>Error:</strong> {result.get("error", "Unknown error")}</p>
            </div>
            """,
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Verification buttons
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📋 Check Order Book", key="check_order_book_btn"):
            show_order_book()

    with col2:
        if st.button("📊 Check Positions", key="check_positions_btn"):
            show_positions()

    with col3:
        if st.button("✅ Done", key="done_trades_btn"):
            st.session_state.show_buy_dialog = False
            st.rerun()


def show_order_book():
    """Display current order book"""
    st.markdown("### 📋 Order Book (Today's Orders)")

    token_manager = TokenManager()
    api = UpstoxAPI(token_manager)

    with st.spinner("Fetching order book..."):
        orders, error = api.get_order_book()

        if error:
            st.error(f"❌ Error fetching orders: {error}")
            return

        if not orders:
            st.info("📭 No orders found for today.")
            return

        # Filter for recent orders (from our trades)
        st.success(f"📋 Found **{len(orders)}** orders")

        for order in orders:
            order_id = order.get("order_id", "N/A")
            trading_symbol = order.get("trading_symbol", "N/A")
            transaction_type = order.get("transaction_type", "N/A")
            order_type = order.get("order_type", "N/A")
            quantity = order.get("quantity", 0)
            price = order.get("price", 0)
            status = order.get("status", "N/A")

            # Color based on status
            if status == "complete":
                bg_color = "#d4edda"
                status_icon = "✅"
            elif status == "open":
                bg_color = "#fff3cd"
                status_icon = "⏳"
            elif status == "cancelled":
                bg_color = "#f8d7da"
                status_icon = "❌"
            else:
                bg_color = "#e2e3e5"
                status_icon = "❓"

            # Color for BUY/SELL
            txn_color = "#28a745" if transaction_type == "BUY" else "#dc3545"

            st.markdown(
                f"""
            <div style="background: {bg_color}; padding: 12px; border-radius: 8px; margin: 8px 0; border-left: 4px solid {txn_color};">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong style="color: {txn_color};">{transaction_type}</strong> | {trading_symbol}
                    </div>
                    <div>
                        {status_icon} <strong>{status.upper()}</strong>
                    </div>
                </div>
                <div style="margin-top: 8px; font-size: 13px; color: #555;">
                    Qty: {quantity} | Price: ₹{price:,.2f} | Type: {order_type} | ID: {order_id}
                </div>
            </div>
            """,
                unsafe_allow_html=True,
            )


def show_positions():
    """Display current positions"""
    st.markdown("### 📊 Current Positions")

    token_manager = TokenManager()
    api = UpstoxAPI(token_manager)

    with st.spinner("Fetching positions..."):
        positions, error = api.get_positions()

        if error:
            st.error(f"❌ Error fetching positions: {error}")
            return

        if not positions:
            st.info("📭 No open positions found.")
            return

        st.success(f"📊 Found **{len(positions)}** positions")

        total_pnl = 0

        for pos in positions:
            trading_symbol = pos.get("trading_symbol", "N/A")
            quantity = pos.get("quantity", 0)
            avg_price = pos.get("average_price", 0)
            ltp = pos.get("last_price", 0)
            pnl = pos.get("pnl", 0)

            total_pnl += pnl

            # Color based on P&L
            if pnl > 0:
                bg_color = "#d4edda"
                pnl_color = "#28a745"
                pnl_icon = "📈"
            elif pnl < 0:
                bg_color = "#f8d7da"
                pnl_color = "#dc3545"
                pnl_icon = "📉"
            else:
                bg_color = "#e2e3e5"
                pnl_color = "#6c757d"
                pnl_icon = "➖"

            pnl_pct = ((ltp - avg_price) / avg_price * 100) if avg_price > 0 else 0

            st.markdown(
                f"""
            <div style="background: {bg_color}; padding: 15px; border-radius: 10px; margin: 10px 0;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong style="font-size: 16px;">{trading_symbol}</strong>
                    </div>
                    <div style="text-align: right;">
                        <span style="font-size: 18px; color: {pnl_color}; font-weight: bold;">
                            {pnl_icon} ₹{pnl:,.2f} ({pnl_pct:+.2f}%)
                        </span>
                    </div>
                </div>
                <div style="margin-top: 10px; display: flex; justify-content: space-between; font-size: 13px; color: #555;">
                    <span>Qty: {quantity}</span>
                    <span>Avg: ₹{avg_price:,.2f}</span>
                    <span>LTP: ₹{ltp:,.2f}</span>
                </div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        # Total P&L
        total_color = "#28a745" if total_pnl >= 0 else "#dc3545"
        st.markdown(
            f"""
        <div style="background: linear-gradient(135deg, #1e3a5f, #2d5a87); padding: 15px; border-radius: 10px; margin-top: 15px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span style="color: white; font-size: 16px;">💰 <strong>Total P&L</strong></span>
                <span style="color: {total_color}; font-size: 22px; font-weight: bold;">₹{total_pnl:,.2f}</span>
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )


# ----------------- Custom CSS -----------------
def inject_custom_css():
    st.markdown(
        """
    <style>
    /* Bullish card styling - Green */
    .bullish-container {
        background-color: #d4edda !important;
        border-left: 4px solid #28a745;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 10px;
    }

    /* Bearish card styling - Red */
    .bearish-container {
        background-color: #f8d7da !important;
        border-left: 4px solid #dc3545;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 10px;
    }

    /* Neutral card styling - White/Light */
    .neutral-container {
        background-color: #ffffff !important;
        border-left: 4px solid #6c757d;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 10px;
        border: 1px solid #dee2e6;
    }

    /* Style metrics labels */
    .metric-label {
        font-size: 12px;
        color: #666;
        margin-bottom: 2px;
    }

    .metric-value {
        font-size: 16px;
        font-weight: bold;
    }

    .positive {
        color: #28a745 !important;
    }

    .negative {
        color: #dc3545 !important;
    }

    /* MACD diff row styling */
    .macd-diff-row {
        display: flex;
        justify-content: space-between;
        gap: 10px;
    }

    .macd-diff-item {
        text-align: center;
        flex: 1;
    }

    /* Column headers styling */
    .column-header-bullish {
        color: #28a745;
    }

    .column-header-bearish {
        color: #dc3545;
    }

    .column-header-neutral {
        color: #6c757d;
    }

    /* Stock metrics container */
    .stock-metrics-green {
        background-color: #d4edda;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #28a745;
        margin: 10px 0;
    }

    .stock-metrics-red {
        background-color: #f8d7da;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #dc3545;
        margin: 10px 0;
    }

    .stock-metrics-white {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #6c757d;
        border: 1px solid #dee2e6;
        margin: 10px 0;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


# ----------------- Stock Card Component -----------------
def render_stock_card(stock_data, card_index, trend_type):
    """Render an expandable stock card matching the original design"""
    symbol = stock_data["symbol"]
    name = stock_data["name"]
    trend = stock_data["trend"]

    # Get timestamp if available
    last_updated = stock_data.get("last_updated", None)

    # Determine if this card should be expanded based on expand_mode
    expand_mode = st.session_state.get("expand_mode", "none")
    should_expand = False
    if expand_mode == "all":
        should_expand = True
    elif expand_mode == "bullish" and trend == "Bullish":
        should_expand = True
    elif expand_mode == "bearish" and trend == "Bearish":
        should_expand = True

    with st.expander(f"**{symbol}** - {name} ({trend})", expanded=should_expand):
        # View Details button - always red/primary color
        if st.button(
            f"🔴 View Details for {symbol}",
            key=f"view_{trend_type}_{card_index}_{symbol}",
            type="primary",
        ):
            st.session_state.selected_symbol = symbol
            st.session_state.selected_stock_data = stock_data
            st.session_state.page = "detail"
            st.rerun()

        # Build the card content based on trend color
        macd_diffs = stock_data.get("macd_diffs_5d", [0, 0, 0, 0, 0])

        # Get low price from stock data
        low_price = stock_data.get("low_price", stock_data["current_price"] * 0.99)

        # Timestamp display
        timestamp_html = ""
        if last_updated:
            timestamp_html = f'<div style="margin-bottom: 8px; font-size: 12px; opacity: 0.8;">🕐 <strong>Last Updated:</strong> {last_updated}</div>'

        if trend == "Bullish":
            # GREEN card
            st.markdown(
                f"""
            <div style="background-color: #28a745; color: white; padding: 15px; border-radius: 8px; margin: 10px 0;">
                {timestamp_html}
                <div style="margin-bottom: 8px;">🔥 <strong>Price:</strong> ₹{stock_data["current_price"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📈 <strong>Today's High:</strong> ₹{stock_data["high_price"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📉 <strong>Today's Low:</strong> ₹{low_price:,.2f}</div>
                <div style="margin-bottom: 8px;">☁️ <strong>Cloud (Senkou B):</strong> ₹{stock_data["senkou_span_b"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist (Today):</strong> {stock_data["macd_hist"]}</div>
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist (Previous):</strong> {stock_data["prev_macd_hist"]}</div>
                <div style="margin-bottom: 8px;">💪 <strong>Intraday Strength (High-Close):</strong></div>
                <div style="margin-bottom: 8px; font-size: 18px;">{stock_data["intraday_strength_pct"]:.4f}%</div>
                <hr style="border-color: rgba(255,255,255,0.3);">
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist. Diff (Last 5 Days):</strong></div>
                <div style="margin-bottom: 3px;"><strong>**Day 1 (T-Y)**:</strong> <span style="color: {"#90EE90" if macd_diffs[0] > 0 else "#FFB6C1"};">●</span> {macd_diffs[0]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 2 (Y-2A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[1] > 0 else "#FFB6C1"};">●</span> {macd_diffs[1]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 3 (2A-3A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[2] > 0 else "#FFB6C1"};">●</span> {macd_diffs[2]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 4 (3A-4A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[3] > 0 else "#FFB6C1"};">●</span> {macd_diffs[3]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 5 (4A-5A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[4] > 0 else "#FFB6C1"};">●</span> {macd_diffs[4]}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
            st.caption(
                "✅ Criteria: Ichimoku (Chikou > 26-ago candle, Price > Cloud A&B 26-ago, Cloud Green 26-ago) AND MACD (Hist or Signal > 0) AND MACD Hist Increasing"
            )

        elif trend == "Bearish":
            # RED card
            st.markdown(
                f"""
            <div style="background-color: #dc3545; color: white; padding: 15px; border-radius: 8px; margin: 10px 0;">
                {timestamp_html}
                <div style="margin-bottom: 8px;">🔥 <strong>Price:</strong> ₹{stock_data["current_price"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📈 <strong>Today's High:</strong> ₹{stock_data["high_price"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📉 <strong>Today's Low:</strong> ₹{low_price:,.2f}</div>
                <div style="margin-bottom: 8px;">☁️ <strong>Cloud (Senkou B):</strong> ₹{stock_data["senkou_span_b"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist (Today):</strong> {stock_data["macd_hist"]}</div>
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist (Previous):</strong> {stock_data["prev_macd_hist"]}</div>
                <div style="margin-bottom: 8px;">💪 <strong>Intraday Strength (High-Close):</strong></div>
                <div style="margin-bottom: 8px; font-size: 18px;">{stock_data["intraday_strength_pct"]:.4f}%</div>
                <hr style="border-color: rgba(255,255,255,0.3);">
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist. Diff (Last 5 Days):</strong></div>
                <div style="margin-bottom: 3px;"><strong>**Day 1 (T-Y)**:</strong> <span style="color: {"#90EE90" if macd_diffs[0] > 0 else "#FFB6C1"};">●</span> {macd_diffs[0]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 2 (Y-2A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[1] > 0 else "#FFB6C1"};">●</span> {macd_diffs[1]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 3 (2A-3A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[2] > 0 else "#FFB6C1"};">●</span> {macd_diffs[2]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 4 (3A-4A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[3] > 0 else "#FFB6C1"};">●</span> {macd_diffs[3]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 5 (4A-5A)**:</strong> <span style="color: {"#90EE90" if macd_diffs[4] > 0 else "#FFB6C1"};">●</span> {macd_diffs[4]}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
            st.caption(
                "🔻 Criteria: Ichimoku (Chikou < 26-ago candle, Price < Cloud A&B 26-ago, Cloud Red 26-ago) AND MACD (Hist or Signal < 0) AND MACD Hist Decreasing"
            )

        else:
            # WHITE/GRAY card for Neutral
            st.markdown(
                f"""
            <div style="background-color: #f8f9fa; color: #333; padding: 15px; border-radius: 8px; margin: 10px 0; border: 1px solid #dee2e6;">
                {timestamp_html.replace("opacity: 0.8", "opacity: 0.6; color: #666")}
                <div style="margin-bottom: 8px;">🔥 <strong>Price:</strong> ₹{stock_data["current_price"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📈 <strong>Today's High:</strong> ₹{stock_data["high_price"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📉 <strong>Today's Low:</strong> ₹{low_price:,.2f}</div>
                <div style="margin-bottom: 8px;">☁️ <strong>Cloud (Senkou B):</strong> ₹{stock_data["senkou_span_b"]:,.2f}</div>
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist (Today):</strong> {stock_data["macd_hist"]}</div>
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist (Previous):</strong> {stock_data["prev_macd_hist"]}</div>
                <div style="margin-bottom: 8px;">💪 <strong>Intraday Strength (High-Close):</strong></div>
                <div style="margin-bottom: 8px; font-size: 18px;">{stock_data["intraday_strength_pct"]:.4f}%</div>
                <hr style="border-color: #dee2e6;">
                <div style="margin-bottom: 8px;">📊 <strong>MACD Hist. Diff (Last 5 Days):</strong></div>
                <div style="margin-bottom: 3px;"><strong>**Day 1 (T-Y)**:</strong> <span style="color: {"#28a745" if macd_diffs[0] > 0 else "#dc3545"};">●</span> {macd_diffs[0]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 2 (Y-2A)**:</strong> <span style="color: {"#28a745" if macd_diffs[1] > 0 else "#dc3545"};">●</span> {macd_diffs[1]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 3 (2A-3A)**:</strong> <span style="color: {"#28a745" if macd_diffs[2] > 0 else "#dc3545"};">●</span> {macd_diffs[2]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 4 (3A-4A)**:</strong> <span style="color: {"#28a745" if macd_diffs[3] > 0 else "#dc3545"};">●</span> {macd_diffs[3]}</div>
                <div style="margin-bottom: 3px;"><strong>**Day 5 (4A-5A)**:</strong> <span style="color: {"#28a745" if macd_diffs[4] > 0 else "#dc3545"};">●</span> {macd_diffs[4]}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
            st.caption(
                "⚪ Mixed signals or fails one of the strict Bullish/Bearish criteria."
            )


# ----------------- PAGE: Authentication -----------------
def auth_page():
    st.title("🔐 Upstox API Authentication")
    st.markdown("---")

    token_manager = TokenManager()
    existing_token = token_manager.get_token()

    # Auto-redirect if token exists
    if existing_token:
        st.success("✅ Valid API token found! Redirecting to screener...")
        st.session_state.authenticated = True
        st.session_state.use_mock_data = False
        st.session_state.page = "screening"
        time.sleep(0.5)
        st.rerun()
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("### 🔗 Upstox Authentication Required")
        st.warning("No valid API token found. Please authenticate to continue.")

        st.markdown("---")

        # New Authentication
        st.markdown("#### 🔗 Option 1: Authenticate with Upstox")
        auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URL}"
        st.markdown(f"1. [Click here to authenticate with Upstox]({auth_url})")
        st.markdown("2. After authorization, copy the code from the URL")
        st.markdown("3. Paste the code below and click Authenticate")

        auth_code = st.text_input("Enter Authorization Code:", key="auth_code_input")
        #if st.button("🔐 Authenticate", width="stretch", type="primary"):
        if st.button("🔐 Authenticate", use_container_width=True, type="primary"):
            if auth_code:
                with st.spinner("Authenticating..."):
                    access_token = token_manager.get_new_token(auth_code)
                    if access_token:
                        st.success("✅ Authentication successful!")
                        st.session_state.authenticated = True
                        st.session_state.use_mock_data = False
                        st.session_state.page = "screening"
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Authentication failed. Please check your code.")
            else:
                st.warning("⚠️ Please enter an authorization code.")

        st.markdown("---")

        # Manual Token Entry
        st.markdown("#### 🔧 Option 2: Manual Token Entry")
        with st.expander("Enter Access Token Manually"):
            manual_token = st.text_input(
                "Access Token:", type="password", key="manual_token_input"
            )
            if st.button("💾 Save Token", key="save_manual_token"):
                if manual_token:
                    token_manager.save_token(manual_token)
                    st.success("✅ Token saved!")
                    st.session_state.authenticated = True
                    st.session_state.use_mock_data = False
                    st.session_state.page = "screening"
                    st.rerun()

    with col2:
        st.markdown("### 📊 Database Status")
        try:
            stock_list = get_stock_list()
            st.success(f"✅ {len(stock_list)} stocks loaded")
        except Exception:
            st.warning("⚠️ Database not initialized")

        st.markdown("### ℹ️ API Info")
        st.code(f"API Key: {API_KEY[:8]}...")
        st.code(f"Redirect: {REDIRECT_URL}")


# ----------------- PAGE: Screening -----------------
def screening_page():
    inject_custom_css()

    # Title with button on right side
    title_col1, title_col2 = st.columns([4, 1])
    with title_col1:
        st.title("🏹 Ichimoku & MACD Stock Screener")
    with title_col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Add spacing to align with title
        if st.button("🛒 Buy This 3 Items", key="buy_3_items_btn", type="primary"):
            st.session_state.show_buy_dialog = True

    # Show Buy Dialog
    if st.session_state.get("show_buy_dialog", False):
        show_buy_dialog()

    st.markdown(
        "Filter stocks using **MACD Histogram** and **Ichimoku Cloud** with **live intraday data**."
    )

    token_manager = TokenManager()
    api = UpstoxAPI(token_manager)

    # Sidebar
    st.sidebar.header("⚙️ Settings")

    if st.session_state.use_mock_data:
        st.sidebar.info("🧪 Demo Mode")
    else:
        if token_manager.get_token():
            st.sidebar.success("✅ API Authenticated")
        else:
            st.sidebar.warning("⚠️ Not Authenticated")

    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Data Source")
    use_live_data = st.sidebar.checkbox(
        "Use Live Intraday Data (V3 API)",
        value=not st.session_state.use_mock_data,
        disabled=st.session_state.use_mock_data,
    )
    intraday_interval = st.sidebar.selectbox(
        "Intraday Interval (minutes)", [1, 30], index=0
    )

    st.sidebar.markdown("---")

    # Auto-refresh toggle
    st.sidebar.subheader("🔄 Auto Refresh")
    auto_refresh = st.sidebar.checkbox(
        "Enable Auto Refresh", value=True, key="auto_refresh_toggle"
    )
    if auto_refresh:
        st.sidebar.info(f"⏱️ Refreshing every {AUTO_REFRESH_INTERVAL} seconds")

    st.sidebar.markdown("---")
    if st.sidebar.button("📋 Reload Stock List", width="stretch"):
        # Force re-insert all stocks from STOCK_LIST
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                # Clear existing stocks and re-insert from STOCK_LIST
                c.execute("DELETE FROM stocks")
                for stock in STOCK_LIST:
                    c.execute(
                        "INSERT OR REPLACE INTO stocks VALUES (?, ?, ?, ?, ?)",
                        (
                            stock["symbol"],
                            stock["name"],
                            stock["isin"],
                            stock["has_options"],
                            datetime.now().strftime("%Y-%m-%d"),
                        ),
                    )
                conn.commit()
            st.sidebar.success(f"✅ Loaded {len(STOCK_LIST)} stocks!")
        except Exception as e:
            st.sidebar.error(f"❌ Error: {e}")
        st.session_state.stock_list_data = []
        st.rerun()

    stock_list = get_stock_list()
    st.sidebar.info(f"📊 Total Stocks: {len(stock_list)}")

    st.sidebar.markdown("---")
    if st.sidebar.button(
        "🚀 Fetch Current Data (All Stocks)", type="primary", width="stretch"
    ):
        st.session_state.force_refresh = True
        st.session_state.stock_list_data = []
        st.rerun()

    # Handle background refresh (from auto-refresh) - update data silently
    if st.session_state.get("background_refresh", False):
        st.session_state.background_refresh = False
        # Silently refresh data in background - no spinner
        results = background_refresh_data(
            stock_list,
            api,
            use_live_data,
            intraday_interval,
            st.session_state.use_mock_data,
        )
        if results:
            st.session_state.stock_list_data = results
            st.session_state.last_refresh_time = datetime.now()

    # Run screening - Only on first load or manual refresh
    elif not st.session_state.stock_list_data or st.session_state.force_refresh:
        st.session_state.force_refresh = False
        # Show loading message
        loading_placeholder = st.empty()
        loading_placeholder.info("🔄 Loading stock data... Please wait.")

        # Use screen_stocks with progress bar for first load
        progress_bar = st.progress(0)
        status_text = st.empty()
        results = screen_stocks(
            stock_list,
            api,
            use_live_data,
            intraday_interval,
            progress_bar,
            status_text,
            st.session_state.use_mock_data,
        )

        st.session_state.stock_list_data = results if results else []
        st.session_state.last_refresh_time = datetime.now()

        # Clear loading elements
        progress_bar.empty()
        status_text.empty()
        loading_placeholder.empty()

    # Show last refresh time
    if st.session_state.last_refresh_time:
        last_refresh_str = st.session_state.last_refresh_time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        st.caption(f"📅 Last Refreshed: {last_refresh_str}")

    # Auto-refresh every 10 seconds (only if we have data)
    if (
        auto_refresh
        and st.session_state.last_refresh_time
        and st.session_state.stock_list_data
    ):
        try:
            from streamlit_autorefresh import st_autorefresh

            # Refresh every AUTO_REFRESH_INTERVAL seconds (10000 ms = 10 seconds)
            count = st_autorefresh(
                interval=AUTO_REFRESH_INTERVAL * 1000,
                limit=None,
                key="data_refresh_timer",
            )
            if count > 0:
                # Trigger background refresh
                st.session_state.background_refresh = True
        except ImportError:
            # Fallback: Check time and trigger rerun (without blocking sleep)
            time_since_refresh = (
                datetime.now() - st.session_state.last_refresh_time
            ).total_seconds()
            if time_since_refresh >= AUTO_REFRESH_INTERVAL:
                st.session_state.background_refresh = True
                st.rerun()

    # Results header
    st.markdown("## Results 🔗")

    # Check if we have data
    if not st.session_state.stock_list_data:
        st.warning(
            "⚠️ No stock data available. Click 'Fetch Current Data' in the sidebar to load stocks."
        )
        st.info(
            "💡 Make sure you are authenticated with Upstox API or enable Demo Mode."
        )
        return

    # Expand/Collapse buttons
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("📂 Expand All", key="expand_all", width="stretch"):
            st.session_state.expand_mode = "all"
            st.rerun()
    with col2:
        if st.button("📁 Collapse All", key="collapse_all", width="stretch"):
            st.session_state.expand_mode = "none"
            st.rerun()
    with col3:
        if st.button("🟢 Expand Bullish Only", key="expand_bullish", width="stretch"):
            st.session_state.expand_mode = "bullish"
            st.rerun()
    with col4:
        if st.button("🔴 Expand Bearish Only", key="expand_bearish", width="stretch"):
            st.session_state.expand_mode = "bearish"
            st.rerun()

    # Categorize stocks
    bullish_stocks = [
        s for s in st.session_state.stock_list_data if s["trend"] == "Bullish"
    ]
    bearish_stocks = [
        s for s in st.session_state.stock_list_data if s["trend"] == "Bearish"
    ]
    neutral_stocks = [
        s for s in st.session_state.stock_list_data if s["trend"] == "Neutral/Mixed"
    ]

    # Sort by intraday strength (High to Low)
    bullish_stocks.sort(key=lambda x: x["intraday_strength_pct"], reverse=False)
    bearish_stocks.sort(key=lambda x: x["intraday_strength_pct"], reverse=False)
    neutral_stocks.sort(key=lambda x: x["intraday_strength_pct"], reverse=False)

    # Three column layout
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"### 🟢 Bullish Signals ({len(bullish_stocks)})")
        st.caption("Sorted by Intraday Strength (High to Low)")
        if bullish_stocks:
            for i, stock in enumerate(bullish_stocks):
                render_stock_card(stock, i, "bullish")
        else:
            st.info("No bullish signals found.")

    with col2:
        st.markdown(f"### 🔴 Bearish Signals ({len(bearish_stocks)})")
        st.caption("Sorted by Intraday Strength (High to Low)")
        if bearish_stocks:
            for i, stock in enumerate(bearish_stocks):
                render_stock_card(stock, i, "bearish")
        else:
            st.info("No bearish signals found.")

    with col3:
        st.markdown(f"### 🟡 Neutral/Mixed ({len(neutral_stocks)})")
        st.caption("Sorted by Intraday Strength (High to Low)")
        if neutral_stocks:
            for i, stock in enumerate(neutral_stocks):
                render_stock_card(stock, i, "neutral")
        else:
            st.info("No neutral signals found.")


# Background refresh function (called at start of screening_page)
def background_refresh_data(
    stock_list, api, use_live_data, intraday_interval, use_mock
):
    """Refresh data in background without showing progress bar"""

    results = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
        future_to_stock = {
            executor.submit(
                fetch_single_stock_data,
                stock,
                api,
                use_live_data,
                intraday_interval,
                use_mock,
            ): stock
            for stock in stock_list
        }
        for future in as_completed(future_to_stock):
            try:
                result = future.result(timeout=10)
                if result:
                    results.append(result)
            except Exception:
                pass
    return results


# ----------------- PAGE: Detail View -----------------
def detail_page():
    symbol = st.session_state.selected_symbol
    stock_data = st.session_state.selected_stock_data

    if not symbol:
        st.warning("No stock selected.")
        if st.button("← Back to Screening"):
            st.session_state.page = "screening"
            st.rerun()
        return

    st.title(f"📈 {symbol} - Detailed Analysis 🔗")

    # Back button
    if st.button("⬅️ Back to Screener"):
        st.session_state.page = "screening"
        st.rerun()

    st.markdown("---")

    # Get data
    if stock_data and "indicators" in stock_data:
        indicators = stock_data["indicators"]
        raw_data = stock_data.get("raw_data", [])
    else:
        if st.session_state.use_mock_data:
            raw_data = generate_mock_historical_data(symbol, days=200)
        else:
            raw_data = get_historical_data(symbol, days=200)
            if not raw_data:
                raw_data = generate_mock_historical_data(symbol, days=200)
        data_asc = raw_data[::-1]
        indicators, _ = calculate_indicators(data_asc)

    if not indicators:
        st.error("Unable to calculate indicators.")
        return

    latest = indicators[0]
    previous = indicators[1] if len(indicators) > 1 else latest

    # MACD Histogram Analysis Section
    st.markdown("### 📊 MACD Histogram Analysis")

    col1, col2, col3, col4 = st.columns(4)

    macd_diff = (
        (latest["macd_hist"] - previous["macd_hist"])
        if latest["macd_hist"] and previous["macd_hist"]
        else 0
    )
    trend_text = "Increasing" if macd_diff > 0 else "Decreasing"
    trend_color = "🟢" if macd_diff > 0 else "🔴"

    with col1:
        st.markdown("**MACD Hist (Today)**")
        st.markdown(f"### {latest['macd_hist']:.4f}" if latest["macd_hist"] else "N/A")
        if macd_diff != 0:
            st.caption(
                f"↑ {macd_diff:.4f}" if macd_diff > 0 else f"↓ {abs(macd_diff):.4f}"
            )

    with col2:
        st.markdown("**MACD Hist (Previous)**")
        st.markdown(
            f"### {previous['macd_hist']:.4f}" if previous["macd_hist"] else "N/A"
        )

    with col3:
        st.markdown("**Trend**")
        st.markdown(f"### {trend_color} {trend_text}")

    with col4:
        st.markdown("### 📡 Live Intraday Data")
        st.markdown("**Interval (minutes)**")
        interval = st.selectbox(
            "Interval",
            [1, 30],
            index=0,
            key="detail_interval",
            label_visibility="collapsed",
        )
        if st.button("🔴 Fetch Live Data", key="fetch_live_detail"):
            token_manager = TokenManager()
            api = UpstoxAPI(token_manager)

            # Check if we have a valid token
            token = token_manager.get_token()

            with st.spinner("Fetching live intraday data..."):
                if token:
                    intraday_data, error_msg = api.get_current_data(
                        symbol, interval_minutes=interval
                    )
                    if intraday_data and len(intraday_data) > 0:
                        st.success(f"✅ Fetched {len(intraday_data)} candles!")
                        # Get current price from latest candle
                        current_price = intraday_data[0]["close"]
                        high_price = max([c["high"] for c in intraday_data])
                        low_price = min([c["low"] for c in intraday_data])
                        fetch_time = datetime.now().strftime("%H:%M:%S")

                        # Display live prices
                        st.markdown(
                            f"""
                        <div style="background-color: #d4edda; padding: 10px; border-radius: 8px; margin-top: 10px;">
                            <div><strong>🔴 Live Price:</strong> ₹{current_price:,.2f}</div>
                            <div><strong>📈 Today's High:</strong> ₹{high_price:,.2f}</div>
                            <div><strong>📉 Today's Low:</strong> ₹{low_price:,.2f}</div>
                        </div>
                        """,
                            unsafe_allow_html=True,
                        )

                        # Store in session for potential use
                        st.session_state[f"live_price_{symbol}"] = {
                            "current": current_price,
                            "high": high_price,
                            "low": low_price,
                            "timestamp": fetch_time,
                        }

                        # AUTO-UPDATE: Update the card data in stock_list_data
                        if st.session_state.stock_list_data:
                            for i, stock in enumerate(st.session_state.stock_list_data):
                                if stock["symbol"] == symbol:
                                    # Update the stock data with live prices
                                    st.session_state.stock_list_data[i][
                                        "current_price"
                                    ] = round(current_price, 2)
                                    st.session_state.stock_list_data[i][
                                        "high_price"
                                    ] = round(high_price, 2)
                                    st.session_state.stock_list_data[i]["low_price"] = (
                                        round(low_price, 2)
                                    )
                                    st.session_state.stock_list_data[i][
                                        "last_updated"
                                    ] = fetch_time

                                    # Also update selected_stock_data
                                    if st.session_state.selected_stock_data:
                                        st.session_state.selected_stock_data[
                                            "current_price"
                                        ] = round(current_price, 2)
                                        st.session_state.selected_stock_data[
                                            "high_price"
                                        ] = round(high_price, 2)
                                        st.session_state.selected_stock_data[
                                            "low_price"
                                        ] = round(low_price, 2)
                                        st.session_state.selected_stock_data[
                                            "last_updated"
                                        ] = fetch_time

                                    st.info(f"✅ Card data updated for {symbol}!")
                                    break
                    else:
                        # Check if it's market hours
                        current_hour = datetime.now().hour
                        current_minute = datetime.now().minute
                        is_market_hours = (
                            (current_hour == 9 and current_minute >= 15)
                            or (current_hour > 9 and current_hour < 15)
                            or (current_hour == 15 and current_minute <= 30)
                        )

                        if not is_market_hours:
                            st.warning(
                                "⏰ **Market is closed.** Live intraday data is only available during market hours (9:15 AM - 3:30 PM)."
                            )
                        else:
                            st.error("❌ Failed to fetch live data")
                            st.warning(
                                f"**Error:** {error_msg if error_msg else 'Unknown error'}"
                            )

                        if stock_data:
                            st.info("📊 Showing last available data:")
                            st.markdown(
                                f"""
                            <div style="background-color: #fff3cd; padding: 15px; border-radius: 8px; margin-top: 10px;">
                                <div style="margin-bottom: 8px;"><strong>💰 Last Price:</strong> ₹{stock_data.get("current_price", "N/A"):,.2f}</div>
                                <div style="margin-bottom: 8px;"><strong>📈 High:</strong> ₹{stock_data.get("high_price", "N/A"):,.2f}</div>
                                <div style="margin-bottom: 8px;"><strong>📉 Low:</strong> ₹{stock_data.get("low_price", "N/A"):,.2f}</div>
                                <div style="font-size: 12px; color: #856404; margin-top: 10px;">
                                    ℹ️ This is the last cached data from the screener.
                                </div>
                            </div>
                            """,
                                unsafe_allow_html=True,
                            )
                else:
                    # No token - show data from screener with note
                    st.info("ℹ️ Using cached data (API token not found)")
                    if stock_data:
                        st.markdown(
                            f"""
                        <div style="background-color: #d1ecf1; padding: 10px; border-radius: 8px; margin-top: 10px;">
                            <div><strong>💰 Price:</strong> ₹{stock_data.get("current_price", "N/A"):,.2f}</div>
                            <div><strong>📈 High:</strong> ₹{stock_data.get("high_price", "N/A"):,.2f}</div>
                            <div><strong>📉 Low:</strong> ₹{stock_data.get("low_price", "N/A"):,.2f}</div>
                        </div>
                        """,
                            unsafe_allow_html=True,
                        )

        # Show cached live price if available
        if f"live_price_{symbol}" in st.session_state:
            cached = st.session_state[f"live_price_{symbol}"]
            st.caption(f"Last fetched at {cached['timestamp']}")

    # MACD Histogram - Last 5 Days Differences
    st.markdown("---")
    st.markdown("### 📈 MACD Histogram - Last 5 Days Differences")

    macd_diffs = []
    for i in range(5):
        if i + 1 < len(indicators):
            curr = indicators[i]["macd_hist"]
            prev = indicators[i + 1]["macd_hist"]
            if curr is not None and prev is not None:
                macd_diffs.append(round(curr - prev, 4))
            else:
                macd_diffs.append(0)
        else:
            macd_diffs.append(0)

    diff_cols = st.columns(5)
    day_labels = ["Day 1 ⓘ", "Day 2 ⓘ", "Day 3 ⓘ", "Day 4 ⓘ", "Day 5 ⓘ"]

    for i, (col, diff, label) in enumerate(zip(diff_cols, macd_diffs, day_labels)):
        with col:
            color = "🟢" if diff > 0 else "🔴"
            st.markdown(f"{color} **{label}**")
            st.markdown(f"## {diff}")

    # MACD Histogram Values Table
    st.markdown("---")
    st.markdown("**MACD Histogram Values (Last 6 Days):**")

    table_data = []
    day_names = ["Today", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5"]
    for i in range(min(6, len(indicators))):
        table_data.append(
            {
                "": i,
                "Day": day_names[i] if i < len(day_names) else f"Day {i}",
                "Date": indicators[i]["date"],
                "MACD Hist": round(indicators[i]["macd_hist"], 4)
                if indicators[i]["macd_hist"]
                else "N/A",
                "Close Price": round(indicators[i]["close"], 2),
            }
        )

    df = pd.DataFrame(table_data)
    st.dataframe(df, width="stretch", hide_index=True)

    # Ichimoku Cloud and MACD Chart
    st.markdown("---")
    st.markdown("### 📊 Ichimoku Cloud and MACD Chart")
    st.markdown(f"**{symbol} - Ichimoku and MACD Analysis**")

    # Create chart using Plotly
    if indicators and len(indicators) > 20:
        chart_data = indicators[:60][::-1]  # Last 60 days, ascending order

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3],
            subplot_titles=("Ichimoku Cloud", "MACD"),
        )

        dates = [d["date"] for d in chart_data]
        closes = [d["close"] for d in chart_data]
        tenkan = [d["tenkan_sen"] for d in chart_data]
        kijun = [d["kijun_sen"] for d in chart_data]
        span_a = [d["senkou_span_a"] for d in chart_data]
        span_b = [d["senkou_span_b"] for d in chart_data]
        macd_hist = [d["macd_hist"] for d in chart_data]
        macd_line = [d["macd"] for d in chart_data]
        signal_line = [d["macd_signal"] for d in chart_data]

        # Price line
        fig.add_trace(
            go.Scatter(
                x=dates, y=closes, name="Close Price", line=dict(color="black", width=2)
            ),
            row=1,
            col=1,
        )

        # Ichimoku lines
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=tenkan,
                name="Tenkan-sen (Conv.)",
                line=dict(color="blue", width=1),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=kijun,
                name="Kijun-sen (Base)",
                line=dict(color="red", width=1),
            ),
            row=1,
            col=1,
        )

        # Cloud
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=span_a,
                name="Senkou Span A",
                line=dict(color="green", width=0.5),
                fill=None,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=span_b,
                name="Kumo (Cloud)",
                line=dict(color="red", width=0.5),
                fill="tonexty",
                fillcolor="rgba(255,0,0,0.1)",
            ),
            row=1,
            col=1,
        )

        # MACD
        colors = ["green" if h and h >= 0 else "red" for h in macd_hist]
        fig.add_trace(
            go.Bar(x=dates, y=macd_hist, name="Histogram", marker_color=colors),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=dates, y=macd_line, name="MACD Line", line=dict(color="blue", width=1)
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=signal_line,
                name="Signal Line",
                line=dict(color="orange", width=1),
            ),
            row=2,
            col=1,
        )

        fig.update_layout(
            height=600,
            showlegend=True,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="MACD", row=2, col=1)

        st.plotly_chart(fig, width="stretch")
    else:
        st.warning("Not enough data to display chart.")

    # Option Chain Section
    st.markdown("---")
    st.markdown("### 📋 Option Contracts")

    token_manager = TokenManager()
    api = UpstoxAPI(token_manager)

    # Token Status Check with detailed info
    token_info = token_manager.get_token_info()
    current_token = token_manager.get_token()

    if current_token and token_info:
        has_refresh = token_info.get("has_refresh_token", False)
        expires_at = token_info.get("expires_at", "Unknown")

        if has_refresh:
            st.success(
                f"✅ API Token found (ends with ...{current_token[-8:]}) | 🔄 Auto-refresh enabled"
            )
        else:
            st.warning(
                "⚠️ Token found but NO refresh token - will need manual re-auth when expired"
            )

        if expires_at and expires_at != "Unknown":
            try:
                expiry_time = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
                time_left = expiry_time - datetime.now()
                if time_left.total_seconds() > 0:
                    hours_left = time_left.total_seconds() / 3600
                    st.caption(
                        f"⏰ Token expires: {expires_at} ({hours_left:.1f} hours left)"
                    )
                else:
                    st.caption(f"⏰ Token expired at: {expires_at}")
            except:
                st.caption(f"⏰ Token expires: {expires_at}")
    else:
        st.warning("⚠️ No API token found")

    # Token validation and update section
    with st.expander("🔑 Token Management", expanded=not current_token):
        st.markdown("**Validate or Update API Token**")

        col_t1, col_t2, col_t3 = st.columns(3)

        with col_t1:
            if st.button("🔄 Validate Token", key="validate_token"):
                if current_token:
                    test_headers = {
                        "Accept": "application/json",
                        "Authorization": f"Bearer {current_token}",
                    }
                    try:
                        test_response = requests.get(
                            USER_PROFILE_URL, headers=test_headers, timeout=10
                        )
                        if test_response.status_code == 200:
                            st.success("✅ Token is valid!")
                            user_data = test_response.json().get("data", {})
                            if user_data:
                                st.info(
                                    f"User: {user_data.get('user_name', 'N/A')} | Email: {user_data.get('email', 'N/A')}"
                                )
                        elif test_response.status_code == 401:
                            st.error(
                                "❌ Token expired! Use 'Refresh Token' or get a new one."
                            )
                        else:
                            st.warning(
                                f"⚠️ Unexpected response: {test_response.status_code}"
                            )
                    except Exception as e:
                        st.error(f"❌ Validation failed: {str(e)}")
                else:
                    st.error("❌ No token to validate")

        with col_t2:
            if st.button("🔃 Refresh Token", key="refresh_token_btn"):
                if token_info and token_info.get("has_refresh_token"):
                    refresh_token = token_info.get("refresh_token")
                    with st.spinner("Refreshing token..."):
                        new_token = token_manager.refresh_token_method(refresh_token)
                        if new_token:
                            st.success(
                                "✅ Token refreshed successfully! Valid for next 24 hours."
                            )
                            st.rerun()
                        else:
                            st.error(
                                "❌ Refresh failed. Refresh token may have expired. Please re-authenticate."
                            )
                else:
                    st.error(
                        "❌ No refresh token available. Please re-authenticate with Upstox."
                    )

        with col_t3:
            if st.button("🗑️ Clear Token", key="clear_token"):
                try:
                    with get_db_connection() as conn:
                        conn.execute("DELETE FROM api_tokens")
                        conn.commit()
                    st.success("✅ Token cleared! Please enter a new token.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed to clear token: {str(e)}")

        st.markdown("---")

        # Info about token lifecycle
        st.info("""
        **📌 Token Lifecycle:**
        - **Access Token**: Expires daily (midnight or ~24 hours)
        - **Refresh Token**: Valid for 15-30 days (auto-renews access token)
        - If refresh token expires, you need to login again with Upstox
        """)

        st.markdown("---")

        # Option 1: Quick Re-authenticate with Authorization Code
        st.markdown("**🔐 Option 1: Re-authenticate with Upstox**")
        auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URL}"
        st.markdown(f"1. [Click here to login to Upstox]({auth_url})")
        st.markdown("2. After login, copy the `code` from the URL")
        st.markdown("3. Paste the code below:")

        auth_code = st.text_input(
            "Authorization Code",
            key="option_auth_code",
            placeholder="Paste code from URL here",
        )

        if st.button("🔓 Get New Token", key="exchange_code", type="primary"):
            if auth_code:
                with st.spinner("Exchanging code for token..."):
                    try:
                        token_data = {
                            "code": auth_code,
                            "client_id": API_KEY,
                            "client_secret": API_SECRET,
                            "redirect_uri": REDIRECT_URL,
                            "grant_type": "authorization_code",
                        }
                        response = requests.post(TOKEN_URL, data=token_data, timeout=15)
                        if response.status_code == 200:
                            access_token = response.json().get("access_token")
                            if access_token:
                                token_manager.save_token(access_token)
                                st.success("✅ New token saved successfully!")
                                st.balloons()
                                st.rerun()
                            else:
                                st.error("❌ No access token in response")
                        else:
                            st.error(f"❌ Failed to get token: {response.text[:200]}")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
            else:
                st.warning("⚠️ Please enter the authorization code")

        st.markdown("---")

        # Option 2: Direct Token Entry
        st.markdown("**📝 Option 2: Enter Access Token Directly**")
        new_token = st.text_input(
            "Access Token",
            type="password",
            key="new_option_token",
            placeholder="Paste your Upstox access token here",
        )

        if st.button("💾 Save Token", key="save_new_token"):
            if new_token and len(new_token) > 20:
                token_manager.save_token(new_token)
                st.success("✅ Token saved successfully!")
                st.rerun()
            else:
                st.error("❌ Please enter a valid token")

    st.markdown("---")

    # Check if stock has options
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(f"View available option contracts for **{symbol}**")

    with col2:
        expiry_date = st.date_input(
            "Filter by Expiry Date (optional)", value=None, key="option_expiry_date"
        )

    # Two buttons: One for contracts list, one for full option chain
    btn_col1, btn_col2 = st.columns(2)

    with btn_col1:
        fetch_contracts = st.button(
            "🔍 Fetch Option Contracts", key="fetch_options", type="secondary"
        )

    with btn_col2:
        fetch_chain = st.button(
            "📊 Fetch Option Chain (OI, Greeks)",
            key="fetch_option_chain",
            type="primary",
        )

    # Fetch Option Chain with full market data
    if fetch_chain:
        if not token_manager.get_token():
            st.error(
                "❌ No API token found. Please add a token in the Token Management section above."
            )
        elif not expiry_date:
            st.warning("⚠️ Please select an **Expiry Date** to fetch Option Chain data.")
        else:
            with st.spinner("Fetching option chain with market data..."):
                expiry_str = expiry_date.strftime("%Y-%m-%d")

                option_chain, spot_price, error_msg = api.get_option_chain_for_stock(
                    symbol, expiry_str
                )

                if option_chain and len(option_chain) > 0:
                    st.success(
                        f"✅ Fetched Option Chain with {len(option_chain)} contracts!"
                    )

                    if spot_price:
                        st.info(f"📍 **Spot Price:** ₹{spot_price:,.2f}")

                    # Separate CE and PE
                    ce_chain = [c for c in option_chain if c["option_type"] == "CE"]
                    pe_chain = [c for c in option_chain if c["option_type"] == "PE"]

                    # Sort by strike price
                    ce_chain.sort(key=lambda x: x["strike_price"])
                    pe_chain.sort(key=lambda x: x["strike_price"])

                    # Display tabs
                    tab1, tab2, tab3, tab4 = st.tabs(
                        [
                            "📊 Full Chain",
                            "🟢 Calls (CE)",
                            "🔴 Puts (PE)",
                            "📈 OI Analysis",
                        ]
                    )

                    with tab1:
                        st.markdown("#### Full Option Chain")

                        # Create combined view
                        df_chain = pd.DataFrame(option_chain)
                        if not df_chain.empty:
                            display_cols = [
                                "strike_price",
                                "option_type",
                                "ltp",
                                "oi",
                                "volume",
                                "iv",
                                "delta",
                                "theta",
                            ]
                            available_cols = [
                                col for col in display_cols if col in df_chain.columns
                            ]

                            # Format the dataframe
                            df_display = df_chain[available_cols].copy()
                            df_display = df_display.rename(
                                columns={
                                    "strike_price": "Strike",
                                    "option_type": "Type",
                                    "ltp": "LTP",
                                    "oi": "OI",
                                    "volume": "Volume",
                                    "iv": "IV%",
                                    "delta": "Delta",
                                    "theta": "Theta",
                                }
                            )

                            st.dataframe(
                                df_display, use_container_width=True, hide_index=True
                            )

                    with tab2:
                        st.markdown("#### 🟢 Call Options (CE)")
                        if ce_chain:
                            df_ce = pd.DataFrame(ce_chain)
                            display_cols = [
                                "strike_price",
                                "ltp",
                                "oi",
                                "oi_change",
                                "volume",
                                "iv",
                                "delta",
                                "theta",
                                "vega",
                            ]
                            available_cols = [
                                col for col in display_cols if col in df_ce.columns
                            ]

                            df_ce_display = df_ce[available_cols].copy()
                            df_ce_display = df_ce_display.rename(
                                columns={
                                    "strike_price": "Strike",
                                    "ltp": "LTP",
                                    "oi": "OI",
                                    "oi_change": "OI Chg",
                                    "volume": "Vol",
                                    "iv": "IV%",
                                    "delta": "Δ",
                                    "theta": "Θ",
                                    "vega": "V",
                                }
                            )

                            # Highlight ITM options
                            if spot_price:
                                itm_strikes = df_ce_display[
                                    df_ce_display["Strike"] < spot_price
                                ]
                                st.caption(
                                    f"ITM Calls (Strike < {spot_price:.0f}): {len(itm_strikes)}"
                                )

                            st.dataframe(
                                df_ce_display, use_container_width=True, hide_index=True
                            )
                        else:
                            st.info("No Call options available")

                    with tab3:
                        st.markdown("#### 🔴 Put Options (PE)")
                        if pe_chain:
                            df_pe = pd.DataFrame(pe_chain)
                            display_cols = [
                                "strike_price",
                                "ltp",
                                "oi",
                                "oi_change",
                                "volume",
                                "iv",
                                "delta",
                                "theta",
                                "vega",
                            ]
                            available_cols = [
                                col for col in display_cols if col in df_pe.columns
                            ]

                            df_pe_display = df_pe[available_cols].copy()
                            df_pe_display = df_pe_display.rename(
                                columns={
                                    "strike_price": "Strike",
                                    "ltp": "LTP",
                                    "oi": "OI",
                                    "oi_change": "OI Chg",
                                    "volume": "Vol",
                                    "iv": "IV%",
                                    "delta": "Δ",
                                    "theta": "Θ",
                                    "vega": "V",
                                }
                            )

                            # Highlight ITM options
                            if spot_price:
                                itm_strikes = df_pe_display[
                                    df_pe_display["Strike"] > spot_price
                                ]
                                st.caption(
                                    f"ITM Puts (Strike > {spot_price:.0f}): {len(itm_strikes)}"
                                )

                            st.dataframe(
                                df_pe_display, use_container_width=True, hide_index=True
                            )
                        else:
                            st.info("No Put options available")

                    with tab4:
                        st.markdown("#### 📈 Open Interest Analysis")

                        if ce_chain and pe_chain:
                            # OI Summary
                            total_ce_oi = sum(c.get("oi", 0) for c in ce_chain)
                            total_pe_oi = sum(c.get("oi", 0) for c in pe_chain)
                            pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0

                            col1, col2, col3, col4 = st.columns(4)
                            col1.metric("Total CE OI", f"{total_ce_oi:,}")
                            col2.metric("Total PE OI", f"{total_pe_oi:,}")
                            col3.metric("PCR (Put/Call)", f"{pcr:.2f}")

                            # PCR Interpretation
                            if pcr > 1.2:
                                col4.metric(
                                    "Sentiment",
                                    "🟢 Bullish",
                                    help="High PCR indicates bullish sentiment",
                                )
                            elif pcr < 0.8:
                                col4.metric(
                                    "Sentiment",
                                    "🔴 Bearish",
                                    help="Low PCR indicates bearish sentiment",
                                )
                            else:
                                col4.metric(
                                    "Sentiment",
                                    "🟡 Neutral",
                                    help="PCR in neutral range",
                                )

                            st.markdown("---")

                            # Max Pain and Key Levels
                            st.markdown("##### 🎯 Key Levels")

                            # Find max OI strikes
                            max_ce_oi = max(ce_chain, key=lambda x: x.get("oi", 0))
                            max_pe_oi = max(pe_chain, key=lambda x: x.get("oi", 0))

                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown(
                                    f"""
                                <div style="background: #d4edda; padding: 15px; border-radius: 10px;">
                                    <h4 style="margin: 0; color: #155724;">🟢 Max CE OI (Resistance)</h4>
                                    <p style="font-size: 24px; font-weight: bold; margin: 10px 0;">₹{max_ce_oi["strike_price"]:,.0f}</p>
                                    <p>OI: {max_ce_oi.get("oi", 0):,}</p>
                                </div>
                                """,
                                    unsafe_allow_html=True,
                                )

                            with col2:
                                st.markdown(
                                    f"""
                                <div style="background: #f8d7da; padding: 15px; border-radius: 10px;">
                                    <h4 style="margin: 0; color: #721c24;">🔴 Max PE OI (Support)</h4>
                                    <p style="font-size: 24px; font-weight: bold; margin: 10px 0;">₹{max_pe_oi["strike_price"]:,.0f}</p>
                                    <p>OI: {max_pe_oi.get("oi", 0):,}</p>
                                </div>
                                """,
                                    unsafe_allow_html=True,
                                )

                            # Top 5 OI strikes
                            st.markdown("---")
                            st.markdown("##### 📊 Top 5 OI Strikes")

                            top_ce = sorted(
                                ce_chain, key=lambda x: x.get("oi", 0), reverse=True
                            )[:5]
                            top_pe = sorted(
                                pe_chain, key=lambda x: x.get("oi", 0), reverse=True
                            )[:5]

                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**Top CE OI (Resistance)**")
                                for i, c in enumerate(top_ce, 1):
                                    st.write(
                                        f"{i}. ₹{c['strike_price']:,.0f} - OI: {c.get('oi', 0):,}"
                                    )

                            with col2:
                                st.markdown("**Top PE OI (Support)**")
                                for i, p in enumerate(top_pe, 1):
                                    st.write(
                                        f"{i}. ₹{p['strike_price']:,.0f} - OI: {p.get('oi', 0):,}"
                                    )
                        else:
                            st.info("Not enough data for OI analysis")

                    # Store in session state
                    st.session_state[f"option_chain_{symbol}"] = option_chain
                    st.session_state[f"spot_price_{symbol}"] = spot_price

                else:
                    st.error("❌ Failed to fetch option chain")
                    st.warning(
                        f"**Error:** {error_msg if error_msg else 'Unknown error'}"
                    )

    # Fetch Option Contracts (basic list)
    if fetch_contracts:
        if not token_manager.get_token():
            st.error(
                "❌ No API token found. Please add a token in the Token Management section above."
            )
        else:
            with st.spinner("Fetching option contracts..."):
                # Convert date to string if provided
                expiry_str = expiry_date.strftime("%Y-%m-%d") if expiry_date else None

                # Try to get option contracts
                option_contracts, error_msg, instrument_key = (
                    api.get_option_contracts_for_stock(symbol, expiry_str)
                )

                if option_contracts and len(option_contracts) > 0:
                    st.success(f"✅ Found {len(option_contracts)} option contracts!")

                    # Store in session state
                    st.session_state[f"options_{symbol}"] = option_contracts

                    # Separate CE and PE
                    ce_contracts = [
                        c for c in option_contracts if c.get("instrument_type") == "CE"
                    ]
                    pe_contracts = [
                        c for c in option_contracts if c.get("instrument_type") == "PE"
                    ]

                    # Get unique expiry dates
                    expiry_dates = sorted(
                        list(set([c.get("expiry") for c in option_contracts]))
                    )

                    st.markdown(
                        f"**Available Expiries:** {', '.join(expiry_dates[:5])}{'...' if len(expiry_dates) > 5 else ''}"
                    )

                    # Display in tabs
                    tab1, tab2, tab3 = st.tabs(
                        [
                            "📊 All Contracts",
                            "🟢 Call Options (CE)",
                            "🔴 Put Options (PE)",
                        ]
                    )

                    with tab1:
                        # Create DataFrame for all contracts
                        df_all = pd.DataFrame(option_contracts)
                        if not df_all.empty:
                            display_cols = [
                                "trading_symbol",
                                "strike_price",
                                "instrument_type",
                                "expiry",
                                "lot_size",
                            ]
                            available_cols = [
                                col for col in display_cols if col in df_all.columns
                            ]
                            st.dataframe(
                                df_all[available_cols].head(50),
                                width="stretch",
                                hide_index=True,
                            )
                            st.caption(
                                f"Showing first 50 of {len(option_contracts)} contracts"
                            )

                    with tab2:
                        if ce_contracts:
                            df_ce = pd.DataFrame(ce_contracts)
                            display_cols = [
                                "trading_symbol",
                                "strike_price",
                                "expiry",
                                "lot_size",
                            ]
                            available_cols = [
                                col for col in display_cols if col in df_ce.columns
                            ]
                            st.dataframe(
                                df_ce[available_cols].head(30),
                                width="stretch",
                                hide_index=True,
                            )
                            st.caption(f"Total CE contracts: {len(ce_contracts)}")
                        else:
                            st.info("No Call Options found")

                    with tab3:
                        if pe_contracts:
                            df_pe = pd.DataFrame(pe_contracts)
                            display_cols = [
                                "trading_symbol",
                                "strike_price",
                                "expiry",
                                "lot_size",
                            ]
                            available_cols = [
                                col for col in display_cols if col in df_pe.columns
                            ]
                            st.dataframe(
                                df_pe[available_cols].head(30),
                                width="stretch",
                                hide_index=True,
                            )
                            st.caption(f"Total PE contracts: {len(pe_contracts)}")
                        else:
                            st.info("No Put Options found")

                    # Summary stats
                    st.markdown("---")
                    st.markdown("#### 📈 Option Chain Summary")

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Total Contracts", len(option_contracts))
                    col2.metric("Call Options (CE)", len(ce_contracts))
                    col3.metric("Put Options (PE)", len(pe_contracts))
                    col4.metric("Expiry Dates", len(expiry_dates))

                    # Strike price range
                    if option_contracts:
                        strikes = [
                            c.get("strike_price", 0)
                            for c in option_contracts
                            if c.get("strike_price")
                        ]
                        if strikes:
                            st.markdown(
                                f"**Strike Range:** ₹{min(strikes):,.0f} - ₹{max(strikes):,.0f}"
                            )
                else:
                    # Show detailed error
                    st.error(f"❌ Failed to fetch option contracts for {symbol}")

                    # Show error details
                    st.warning(f"""
**Error Details:**
- **Error:** {error_msg if error_msg else "Unknown error"}
- **Symbol:** {symbol}
- **Instrument Key:** {instrument_key if instrument_key else "N/A"}

**Possible Solutions:**
1. Click on "Token Management" above to validate or update your token
2. Verify that {symbol} is in the F&O segment
3. Try clearing and re-entering your access token
                    """)

    # Show cached options if available
    if f"options_{symbol}" in st.session_state and not st.session_state.get(
        "options_just_fetched"
    ):
        st.info(
            f"💡 Previously fetched {len(st.session_state[f'options_{symbol}'])} option contracts. Click 'Fetch Option Contracts' to refresh."
        )


# ----------------- Main -----------------
def main():
    if not init_db():
        st.error("❌ Database initialization failed")
        st.stop()
        return

    # Check and reset ISIN daily at 8 AM, re-populate at 9:15 AM
    isin_status = check_and_reset_daily_isin()
    if isin_status == "reset":
        st.toast("🔄 Daily ISIN reset completed (8:00 AM)", icon="🕐")
    elif isin_status == "populated":
        st.toast("✅ ISIN data populated for market open", icon="📈")

    if st.session_state.page == "auth" or not st.session_state.authenticated:
        auth_page()
    elif st.session_state.page == "screening":
        screening_page()
    elif st.session_state.page == "detail":
        detail_page()
    else:
        auth_page()


if __name__ == "__main__":
    main()
