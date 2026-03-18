"""
================================================================================
  PROFESSIONAL ALGORITHMIC TRADING BOT FOR OPTIONS (UPSTOX API)
  Single-file • Async • NIFTY 50 / BANKNIFTY • Intraday Options Buying
================================================================================

ARCHITECTURE: asyncio + aiohttp (REST), websockets (live stream). Single file.

MARKET DATA: Historical candles (5m, 15m) via REST; concurrent fetch.

INDICATORS: EMA(20), VWAP, ATR(14). pandas/numpy.

MULTI-TIMEFRAME: 5m & 15m trend (UPTREND: price > EMA and price > VWAP;
  DOWNTREND: price < EMA and price < VWAP; RANGE otherwise). Trade only if both agree.

FILTERS: ATR above rolling average.

LIQUIDITY SWEEP: Rolling 20 high/low; bearish trap (break above, close below);
  bullish trap (break below, close above). Traps convert to signals.

ORDER FLOW: Buy/sell volume estimate → imbalance = (buy-sell)/(buy+sell).
  imbalance > 0.3 → BUY_CALL; imbalance < -0.3 → BUY_PUT.

OPTION CHAIN: Support = strike with highest Put OI; Resistance = highest Call OI.

STRIKE: ATM; NIFTY 50 pt steps, BANKNIFTY 100 pt steps.

POSITION SIZING: Risk 1%% of capital; position_size = risk_amount / stop_distance;
  round to lot size. ATR stop: initial stop = entry − 1.5×ATR; trailing = highest − 1.5×ATR.

LIVE PRICE: WebSocket stream + REST LTP fallback for trailing stop.

RISK: Daily loss limit ₹3000; optional max trades/day. Auto square-off 3:15 PM.

DISCORD: Alerts for signal, order placed, stop loss hit, trade closed, daily loss.

TRADE JOURNAL: CSV (timestamp, signal, strike, entry_price, exit_price, pnl).
"""

import os
import sys

# Force UTF-8 encoding for Windows terminals to display emojis correctly
if sys.platform.startswith('win'):
    # Standard output reconfiguration
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

import csv
import time
import json
import logging
import asyncio
import aiohttp
import requests
import pandas as pd
import numpy as np
import traceback
from datetime import datetime, date, timedelta, timezone

# =============================================================================
# CONFIGURATION (EDIT HERE)
# =============================================================================
UPSTOX_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1NUJBOVgiLCJqdGkiOiI2OWJhMWUyZjYzNmQ0YzFiYjVlNjdhYmEiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3MzgwNTEwMywiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzczODcxMjAwfQ.KPQXocZ-AyOaXN7CBiKOU4SZ-W-iSixzyPYyjVmWmBc"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1412386951474057299/Jgft_nxzGxcfWOhoLbSWMde-_bwapvqx8l3VQGQwEoR7_8n4b9Q9zN242kMoXsVbLdvG"

INSTRUMENT = "NSE_INDEX|Nifty 50"  # Or "NSE_INDEX|Nifty Bank"
IS_BANKNIFTY = "BANK" in INSTRUMENT.upper()
LOT_SIZE = 15 if IS_BANKNIFTY else 65
STRIKE_STEP = 100 if IS_BANKNIFTY else 50  # NIFTY 50 pt, BANKNIFTY 100 pt

# --- RISK & POSITION SIZING ---
CAPITAL = 100000           # Account balance for risk sizing
RISK_PCT = 1.0             # Risk per trade = 1% of CAPITAL
MAX_DAILY_LOSS = 3000      # Stop trading if daily loss exceeds this (₹)
MAX_TRADES_PER_DAY = None  # Optional cap (e.g. 5); None = no limit
DELTA_APPROX = 0.5         # Delta for ATM options (moves ~50% of spot)

# --- EXECUTION ---
EXECUTE_TRADE = False      # False = Paper trading; True = Live
SQUARE_OFF_TIME = "15:15"  # Auto close all positions at 3:15 PM

# --- LOGGING & DIRECTORIES ---
SESSION_START_TIME = datetime.now().strftime("%Y-%m-%d_%I%M%S_%p")
TERMINAL_LOGS_DIR = "terminal_logs"
TRADE_LOGS_DIR = "trade_logs"
os.makedirs(TERMINAL_LOGS_DIR, exist_ok=True)
os.makedirs(TRADE_LOGS_DIR, exist_ok=True)

TRADES_CSV = os.path.join(TRADE_LOGS_DIR, f"{SESSION_START_TIME}_trades_log.csv")
LOG_FILE = os.path.join(TERMINAL_LOGS_DIR, f"{SESSION_START_TIME}_algo_trader.log")

# Configure logging with explicit UTF-8 StreamHandler for Windows
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[stream_handler, file_handler])
log = logging.getLogger("AlgoTrader")

# =============================================================================
# POSITION TRACKING & RISK STATE
# =============================================================================
class State:
    daily_pnl = 0.0
    trades_today = 0
    in_trade = False
    stop_trading = False
    
    # Active Position
    pos_side = None
    pos_strike = None
    pos_opt_type = None
    pos_entry = 0.0
    pos_sl_premium = 0.0
    pos_tp_premium = 0.0
    pos_stop = 0.0
    pos_stop_distance = 0.0
    pos_qty = 0
    pos_highest = 0.0
    pos_lowest = 0.0
    pos_tp_spot = 0.0
    pos_ins_key = None

    pos_breakeven_locked = False
    pos_entry_spot = 0.0
    
    @classmethod
    def reset(cls):
        """Clear current position only; daily_pnl/trades_today/stop_trading persist for risk."""
        cls.in_trade = False
        cls.pos_side = None
        cls.pos_strike = None
        cls.pos_opt_type = None
        cls.pos_entry = 0.0
        cls.pos_sl_premium = 0.0
        cls.pos_tp_premium = 0.0
        cls.pos_breakeven_locked = False
        cls.pos_entry_spot = 0.0
        cls.pos_stop = 0.0
        cls.pos_stop_distance = 0.0
        cls.pos_qty = 0
        cls.pos_highest = 0.0
        cls.pos_lowest = 0.0
        cls.pos_ins_key = None
        cls.pos_tp_spot = 0.0

# =============================================================================
# TRADE JOURNAL (CSV)
# =============================================================================
if not os.path.exists(TRADES_CSV):
    with open(TRADES_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["timestamp", "signal", "strike", "entry_price", "exit_price", "pnl"])

# =============================================================================
# DISCORD ALERTS
# =============================================================================
async def send_discord(title, description, color=0x3498DB):
    if not DISCORD_WEBHOOK_URL or "YOUR_" in DISCORD_WEBHOOK_URL: return
    embed = {"title": title, "description": description, "color": color, "timestamp": datetime.now(timezone.utc).isoformat()}
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=5)
    except Exception as e:
        log.error(f"Discord error: {e}")

# =============================================================================
# UPSTOX REST API CLIENT (Market Data, Option Chain, Orders)
# =============================================================================
class UpstoxClient:
    BASE_URL = "https://api.upstox.com/v2"

    @staticmethod
    def _headers():
        return {
            "Authorization": f"Bearer {UPSTOX_TOKEN}",
            "Accept": "application/json"
        }

    @classmethod
    async def get(cls, session, endpoint, params=None):
        url = f"{cls.BASE_URL}{endpoint}"
        try:
            async with session.get(url, headers=cls._headers(), params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data")
                return None
        except Exception as e:
            log.error(f"API API GET Error ({endpoint}): {e}")
            return None

    @classmethod
    async def post(cls, session, endpoint, payload=None):
        url = f"{cls.BASE_URL}{endpoint}"
        headers = cls._headers()
        headers["Content-Type"] = "application/json"
        try:
            async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data")
                return None
        except Exception as e:
            log.error(f"API POST Error ({endpoint}): {e}")
            return None

    @classmethod
    async def get_candles(cls, session, instrument_key, interval="5minute", days=4):
        # We MUST fetch historical data even for "intraday only" trading.
        # Why? A 20-period moving average on a 15-minute timeframe requires 300 minutes (5 hours) of data.
        # If we only pull today's data, the 15m indicators wouldn't activate until 2:15 PM!
        
        to_date = date.today().strftime("%Y-%m-%d")
        from_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        hist_endpoint = f"/historical-candle/{instrument_key}/1minute/{to_date}/{from_date}"
        intra_endpoint = f"/historical-candle/intraday/{instrument_key}/1minute"
        
        # Concurrent API execution
        hist_task = cls.get(session, hist_endpoint)
        intra_task = cls.get(session, intra_endpoint)
        
        hist_data, intra_data = await asyncio.gather(hist_task, intra_task)
        
        all_candles = []
        if hist_data and "candles" in hist_data:
            all_candles.extend(hist_data["candles"])
        if intra_data and "candles" in intra_data:
            all_candles.extend(intra_data["candles"])
            
        if not all_candles:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_candles, columns=["datetime", "open", "high", "low", "close", "volume", "oi"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        
        # Nifty Options volume fixes to avoid zero divides
        df["volume"] = df["volume"].replace(0, 1)
        
        df = df.drop_duplicates(subset=["datetime"])
        df = df.sort_values("datetime").reset_index(drop=True)
        
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])
            
        df.set_index("datetime", inplace=True)
        
        # Resample logic
        rule = "5min" if interval == "5minute" else "15min"
        
        df_resampled = df.resample(rule).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        df_resampled.reset_index(inplace=True)
        return df_resampled

    @classmethod
    async def get_option_chain(cls, session, instrument_key):
        # Dynamically fetch the nearest available expiry date to avoid holiday misalignments
        contract_url = "/option/contract"
        contracts = await cls.get(session, contract_url, {"instrument_key": instrument_key})
        
        if not contracts:
            return None, None, "N/A"
            
        expiries = sorted(list(set([c.get("expiry") for c in contracts if c.get("expiry")])))
        if not expiries:
            return None, None, "N/A"
            
        nearest_expiry = expiries[0]
        
        # Fetch live spot to filter near-ATM strikes only
        spot_res = await cls.get(session, "/market-quote/ltp", {"instrument_key": instrument_key})
        spot = 0
        if spot_res:
            for v in spot_res.values():
                if isinstance(v, dict) and "last_price" in v:
                    spot = float(v["last_price"])
                    break
        
        res = await cls.get(session, "/option/chain", {"instrument_key": instrument_key, "expiry_date": nearest_expiry})
        if not res: return None, None, nearest_expiry
        
        max_pe_oi = 0
        max_ce_oi = 0
        support = None
        resistance = None
        
        for strike_data in res:
            strike = strike_data.get("strike_price")
            if not strike:
                continue
            
            # Only look at strikes within +/- 3 strikes of current spot (Strict Intraday Zone)
            range_limit = STRIKE_STEP * 3
            if spot > 0 and abs(strike - spot) > range_limit:
                continue
            
            ce_oi = strike_data.get("call_options", {}).get("market_data", {}).get("oi", 0) or 0
            pe_oi = strike_data.get("put_options", {}).get("market_data", {}).get("oi", 0) or 0
            
            if ce_oi > max_ce_oi:
                max_ce_oi = ce_oi
                resistance = strike
            if pe_oi > max_pe_oi:
                max_pe_oi = pe_oi
                support = strike
                
        return support, resistance, nearest_expiry, res

    @classmethod
    async def get_option_premium(cls, session, instrument_key):
        """
        Fetch current option premium (LTP) via quotes API.
        Primary: /market-quote/quotes
        Fallback: /market-quote/ltp
        Both parsed like Day's Open strategy: robust over last_price/ltp/last_traded_price.
        Returns float or None.
        """
        if not instrument_key:
            return None

        async def _extract_premium(endpoint: str):
            data = await cls.get(session, endpoint, {"instrument_key": instrument_key})
            if not data:
                return None

            # Shape 1: dict of instruments -> dict(ltp/last_price/last_traded_price)
            if isinstance(data, dict):
                for _, v in data.items():
                    if isinstance(v, dict):
                        premium = v.get("last_price") or v.get("ltp") or v.get("last_traded_price")
                        if premium is not None:
                            try:
                                return float(premium)
                            except (TypeError, ValueError):
                                continue

            # Shape 2: list of dicts
            if isinstance(data, list):
                for v in data:
                    if isinstance(v, dict):
                        premium = v.get("last_price") or v.get("ltp") or v.get("last_traded_price")
                        if premium is not None:
                            try:
                                return float(premium)
                            except (TypeError, ValueError):
                                continue

            return None

        # Try quotes first
        prem = await _extract_premium("/market-quote/quotes")
        if prem is not None:
            return prem

        # Fallback to LTP if quotes fails
        prem = await _extract_premium("/market-quote/ltp")
        if prem is not None:
            return prem

        return None


# =============================================================================
# INDICATORS (EMA 20, VWAP, ATR 14, Avg Volume 20, Imbalance, Liquidity Sweep)
# =============================================================================
def calc_indicators(df):
    if df.empty: return df
    
    # EMA 20
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    
    # VWAP
    tp = (df["high"] + df["low"] + df["close"]) / 3
    df["date"] = df["datetime"].dt.date
    df["tp_vol"] = tp * df["volume"]
    df["cum_vol"] = df.groupby("date")["volume"].cumsum()
    df["cum_tp_vol"] = df.groupby("date")["tp_vol"].cumsum()
    df["vwap"] = df["cum_tp_vol"] / df["cum_vol"]
    
    # ATR 14
    df["tr"] = np.maximum(df["high"] - df["low"], 
                 np.maximum(abs(df["high"] - df["close"].shift(1)), 
                            abs(df["low"] - df["close"].shift(1))))
    # ATR(14): Wilder's smoothing (RMA) to match TradingView; alpha = 1/14
    df["atr_14"] = df["tr"].ewm(alpha=1/14, adjust=False).mean()
    df["atr_rolling_mean"] = df["atr_14"].rolling(20).mean()
    
    # Buy/Sell Volume Imbalance
    # Estimate: If close > open -> more buy vol. If close < open -> more sell vol.
    df["is_bull"] = df["close"] >= df["open"]
    df["buy_vol"] = np.where(df["is_bull"], df["volume"] * 0.7, df["volume"] * 0.3)
    df["sell_vol"] = np.where(df["is_bull"], df["volume"] * 0.3, df["volume"] * 0.7)
    df["imbalance"] = (df["buy_vol"] - df["sell_vol"]) / (df["buy_vol"] + df["sell_vol"] + 1e-9)
    
    # Rolling 20 High / Low for Liquidity Sweep
    df["roll_high_20"] = df["high"].rolling(20).max().shift(1)
    df["roll_low_20"] = df["low"].rolling(20).min().shift(1)
    
    # Liquidity Sweeps
    # Bearish Trap: Price breaks above rolling 20 high but closes below
    df["bearish_trap"] = (df["high"] > df["roll_high_20"]) & (df["close"] < df["roll_high_20"])
    
    # Bullish Trap: Price breaks below rolling 20 low but closes above
    df["bullish_trap"] = (df["low"] < df["roll_low_20"]) & (df["close"] > df["roll_low_20"])
    
    return df

# =============================================================================
# TERMINAL DASHBOARD
# =============================================================================
def print_market_snapshot(spot, df_5m, df_15m, signal, support, resistance):
    if df_5m.empty or df_15m.empty: return
    
    c5 = df_5m.iloc[-1]
    c15 = df_15m.iloc[-1]
    
    # Check 15m Trend
    is_15m_up = c15["close"] > c15["ema_20"] and c15["close"] > c15["vwap"]
    is_15m_dn = c15["close"] < c15["ema_20"] and c15["close"] < c15["vwap"]
    
    # Check 5m Trend
    is_5m_up = c5["close"] > c5["ema_20"] and c5["close"] > c5["vwap"]
    is_5m_dn = c5["close"] < c5["ema_20"] and c5["close"] < c5["vwap"]
    
    # Filters (volume spike now removed from logic; still show volume/ATR/imbalance)
    atr_expand = c5["atr_14"] > c5["atr_rolling_mean"]
    imbalance = c5["imbalance"]
    
    now = datetime.now().strftime("%I:%M:%S %p")
    mode = "🔴 LIVE" if EXECUTE_TRADE else "📝 PAPER"
    
    snapshot = f"""{'=' * 85}
⚡ ALGO TRADER | {now} | {mode}
{'=' * 85}
📊 MARKET SNAPSHOT:
  Spot Price:    {spot:8.2f}    |  VWAP(5m):   {c5['vwap']:8.2f}
  15m Trend:     {'BULLISH ✅' if is_15m_up else ('BEARISH ❌' if is_15m_dn else 'NEUTRAL ➖')}  |  5m Trend:   {'BULLISH ✅' if is_5m_up else ('BEARISH ❌' if is_5m_dn else 'NEUTRAL ➖')}
  OI Support:    {support or 'N/A':<10}  |  OI Resist:  {resistance or 'N/A':<10}
  ATR(14, 5m):   {c5['atr_14']:8.2f}     |  ATR Mean(20): {c5['atr_rolling_mean']:8.2f}
  Imbalance:     {c5['imbalance']:8.3f}

🔍 CONDITION EVALUATION (All ✅ required for trade):
  CALL (CE): {'✅' if is_15m_up else '❌'} 15m Up | {'✅' if is_5m_up else '❌'} 5m Up | {'✅' if atr_expand else '❌'} ATR Exp | {'✅' if imbalance > 0.3 else '❌'} Imbal > 0.3
  PUT (PE) : {'✅' if is_15m_dn else '❌'} 15m Dn | {'✅' if is_5m_dn else '❌'} 5m Dn | {'✅' if atr_expand else '❌'} ATR Exp | {'✅' if imbalance < -0.3 else '❌'} Imbal < -0.3

🎯 STATE: {signal if signal else '⏸ WAITING FOR CONDITIONS'}
{'=' * 85}"""
    # Print snapshot
    print(snapshot)

def print_trade_dashboard(spot, premium, entry, tp, sl, pnl, strike, opt_type):
    """Prints a clean, single-line update for continuous P&L logging."""
    now = datetime.now().strftime("%I:%M:%S %p")
    pnl_symbol = "🟢" if pnl >= 0 else "🔴"
    
    line = (
        f"⏰ [{now}] {pnl_symbol} P&L: ₹{pnl:.2f} | "
        f"LTP: ₹{premium:.2f} | Entry: ₹{entry:.2f} | "
        f"Strike: {str(strike)}{opt_type} | SL: ₹{sl:.2f} | TP: ₹{tp:.2f}"
    )
    print(line, flush=True)

def log_signal_alert(signal, strike, buy_price, sl_premium, tp_premium, spot, support, resistance, expiry):
    """Logs a compact signal alert box."""
    now = datetime.now().strftime("%I:%M:%S %p")
    box = f"╔{'═'*78}╗\n" \
          f"║ 🚀 SIGNAL: {signal:<14} | STR: {strike:<12} | EXP: {expiry:<8} ║\n" \
          f"║ SPOT: {spot:<16.2f} | BUY: ₹{buy_price:<10.2f} | LTP: ₹{buy_price:<10.2f} ║\n" \
          f"║ 🎯 TP: ₹{tp_premium:<11.2f} | 🛑 SL: ₹{sl_premium:<11.2f} | SUP/RES: {support or 'N/A'}/{resistance or 'N/A'} ║\n" \
          f"╚{'═'*78}╝"
    log.info(box)

# =============================================================================
# TRADE EXECUTION (Paper / Live)
# =============================================================================
async def place_order(session, instrument_token, qty, side="BUY"):
    if not EXECUTE_TRADE:
        log.info(f"PAPER TRADE: {side} {qty} {instrument_token}")
        return True

    payload = {
        "quantity": qty,
        "product": "I",
        "validity": "DAY",
        "price": 0,
        "instrument_token": instrument_token,
        "order_type": "MARKET",
        "transaction_type": side,
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False,
    }
    res = await UpstoxClient.post(session, "/order/place", payload)
    if res:
        log.info(f"LIVE TRADE: {side} {qty} {instrument_token} placed successfully. OrderID: {res.get('order_id')}")
        return True
    return False


# =============================================================================
# TRAILING STOP (REST LTP fallback when WebSocket Protobuf not decoded)
# =============================================================================
async def ltp_loop(session):
    while True:
        if State.in_trade and State.pos_ins_key:
            res = await UpstoxClient.get(session, "/market-quote/ltp", {"instrument_key": INSTRUMENT})
            if res:
                spot_ltp = res.get(INSTRUMENT.replace("|", ":"), {}).get("last_price", 0)
                if spot_ltp > 0:
                    if State.pos_side == "BUY_CALL":
                        # Profit Locking: If price moves 1R into profit, lock Break-Even
                        if not State.pos_breakeven_locked and spot_ltp >= (State.pos_entry_spot + State.pos_stop_distance):
                            State.pos_breakeven_locked = True
                            State.pos_stop = State.pos_entry_spot
                            log.info(f"Smart Profit Locking: Stop moved to Break-Even (Entry: {State.pos_entry_spot})")
                        
                        State.pos_highest = max(State.pos_highest, spot_ltp)
                        # Trail stop only if not locked at BE or if trail is higher than entry
                        new_stop = State.pos_highest - State.pos_stop_distance
                        State.pos_stop = max(State.pos_stop, new_stop)
                        
                        if spot_ltp <= State.pos_stop:
                            log.info(f"STOP LOSS HIT at Spot {spot_ltp}! Exiting position.")
                            await close_position(session, "SL_HIT")
                            
                    elif State.pos_side == "BUY_PUT":
                        # Profit Locking: If price moves 1R into profit, lock Break-Even
                        if not State.pos_breakeven_locked and spot_ltp <= (State.pos_entry_spot - State.pos_stop_distance):
                            State.pos_breakeven_locked = True
                            State.pos_stop = State.pos_entry_spot
                            log.info(f"Smart Profit Locking: Stop moved to Break-Even (Entry: {State.pos_entry_spot})")
                            
                        State.pos_lowest = min(State.pos_lowest, spot_ltp) if State.pos_lowest > 0 else spot_ltp
                        # Trail stop only if not locked at BE or if trail is lower than entry
                        new_stop = State.pos_lowest + State.pos_stop_distance
                        if State.pos_stop == 0: State.pos_stop = new_stop
                        State.pos_stop = min(State.pos_stop, new_stop)
                        
                        if spot_ltp >= State.pos_stop:
                            log.info(f"STOP LOSS HIT at Spot {spot_ltp}! Exiting position.")
                            await close_position(session, "SL_HIT")
        await asyncio.sleep(1)

# =============================================================================
# CLOSE POSITION & RISK MANAGEMENT (Daily loss limit, CSV log)
# =============================================================================
async def close_position(session, reason="SQUARE_OFF"):
    if not State.in_trade: return
    
    # Get option exit premium (same as Day's Open strategy: quotes API)
    exit_price = await UpstoxClient.get_option_premium(session, State.pos_ins_key)
    if exit_price is None:
        exit_price = State.pos_entry

    # Place SELL order
    await place_order(session, State.pos_ins_key, State.pos_qty, "SELL")
    
    # PnL: For any BUY position (Call or Put), profit = (exit - entry) * qty
    pnl = (exit_price - State.pos_entry) * State.pos_qty
    State.daily_pnl += pnl
    State.trades_today += 1
    
    log.info(f"Closed {State.pos_side} {State.pos_strike}{State.pos_opt_type} at {exit_price}. P&L: {pnl:.2f} | Daily P&L: {State.daily_pnl:.2f}. Reason: {reason}")
    
    status_emoji = "🟢" if pnl >= 0 else "🔴"
    if reason == "SL_HIT":
        msg = f"{status_emoji} **STOP LOSS HIT**\n" \
              f"━━━━━━━━━━━━━━━━━━━━━━━━\n" \
              f"🏷️ **Side:** {State.pos_side}\n" \
              f"🎯 **Strike:** {State.pos_strike}{State.pos_opt_type}\n" \
              f"🚪 **Exit Price:** ₹{exit_price:.2f}\n" \
              f"💰 **Trade P&L:** ₹{pnl:.2f}\n" \
              f"📈 **Daily P&L:** ₹{State.daily_pnl:.2f}\n" \
              f"━━━━━━━━━━━━━━━━━━━━━━━━"
        await send_discord("Stop Loss Hit", msg, color=0xFF6600)
    else:
        msg = f"{status_emoji} **TRADE CLOSED**\n" \
              f"━━━━━━━━━━━━━━━━━━━━━━━━\n" \
              f"🏷️ **Side:** {State.pos_side}\n" \
              f"🎯 **Strike:** {State.pos_strike}{State.pos_opt_type}\n" \
              f"🚪 **Exit Price:** ₹{exit_price:.2f}\n" \
              f"💰 **Trade P&L:** ₹{pnl:.2f}\n" \
              f"📈 **Daily P&L:** ₹{State.daily_pnl:.2f}\n" \
              f"📝 **Reason:** {reason}\n" \
              f"━━━━━━━━━━━━━━━━━━━━━━━━"
        await send_discord("Trade Closed", msg, color=0x00FF00)
    
    # Log CSV with formatted timestamp for spreadsheet compatibility
    with open(TRADES_CSV, "a", newline="") as f:
        formatted_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        csv.writer(f).writerow([formatted_now, State.pos_side, State.pos_strike, State.pos_entry, exit_price, pnl])
        
    State.reset()
    
    if State.daily_pnl <= -MAX_DAILY_LOSS:
        log.warning(f"MAX DAILY LOSS EXCEEDED (₹{State.daily_pnl:.2f}). Stopping bot for the day.")
        State.stop_trading = True
        await send_discord("Daily Loss Limit Reached", f"Daily P&L: ₹{State.daily_pnl:.2f}\nTrading stopped.", color=0xFF0000)

# =============================================================================
# MAIN LOOP (Multi-timeframe, filters, signals, auto square-off)
# =============================================================================
async def main_loop():
    log.info("Starting Algo Trader Initialization...")
    waiting_line_count = 0
    async with aiohttp.ClientSession() as session:
        
        # Start LTP Loop in background
        asyncio.create_task(ltp_loop(session))
        
        while True:
            now = datetime.now()
            if State.stop_trading:
                break
                
            # Auto Square-Off Time Check
            h, m = map(int, SQUARE_OFF_TIME.split(":"))
            if now.hour == h and now.minute >= m and State.in_trade:
                log.info("Auto Square Off Time Triggered.")
                await close_position(session, "SQUARE_OFF")
                break

            # Market close guard: stop after 15:30
            if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                break # Market closed

            # Ensure we only ENTER new trades between 09:30 and 14:45 for pure intraday.
            # Outside this window we still evaluate conditions, but do not enter.
            is_valid_entry_time = (
                (now.hour > 9 or (now.hour == 9 and now.minute >= 30))
                and (now.hour < 14 or (now.hour == 14 and now.minute < 45))
            )

            try:
                # 1. Fetch 5m & 15m Concurrently
                t1 = UpstoxClient.get_candles(session, INSTRUMENT, "5minute")
                t2 = UpstoxClient.get_candles(session, INSTRUMENT, "15minute")
                df_5m, df_15m = await asyncio.gather(t1, t2)
                
                # Fetch Option Chain independently for dashboard regardless of candle failures
                support, resistance, expiry, chain_data = await UpstoxClient.get_option_chain(session, INSTRUMENT)
                
                # Fetch live spot
                spot_res = await UpstoxClient.get(session, "/market-quote/ltp", {"instrument_key": INSTRUMENT})
                spot = spot_res.get(INSTRUMENT.replace("|", ":"), {}).get("last_price", 0) if spot_res else 0
                
                if df_5m.empty or df_15m.empty or len(df_5m) < 20 or len(df_15m) < 20:
                    if not State.in_trade:
                        # Output loading dashboard
                        now = datetime.now().strftime("%I:%M:%S %p")
                        mode = "🔴 LIVE" if EXECUTE_TRADE else "📝 PAPER"
                        snapshot = f"""{'=' * 85}
⚡ ALGO TRADER | {now} | {mode}
{'=' * 85}
📊 MARKET SNAPSHOT:
  Spot Price:    {spot:8.2f}    |  Status:     WAITING FOR MORE HISTORICAL CANDLES...

  OI Support:    {support or 'N/A':<10}  |  OI Resist:  {resistance or 'N/A':<10}

🎯 STATE: ⏸ WAITING FOR CANDLE DATA (Needs 20+ Bars)
{'=' * 85}"""
                        print(snapshot)
                    await asyncio.sleep(5)
                    continue
                    
                df_5m = calc_indicators(df_5m)
                df_15m = calc_indicators(df_15m)
                
                c5 = df_5m.iloc[-1]
                c15 = df_15m.iloc[-1]
                
                # Check 15m Trend
                is_15m_up = c15["close"] > c15["ema_20"] and c15["close"] > c15["vwap"]
                is_15m_dn = c15["close"] < c15["ema_20"] and c15["close"] < c15["vwap"]
                
                # Check 5m Trend
                is_5m_up = c5["close"] > c5["ema_20"] and c5["close"] > c5["vwap"]
                is_5m_dn = c5["close"] < c5["ema_20"] and c5["close"] < c5["vwap"]
                
                # Filters
                atr_expand = c5["atr_14"] > c5["atr_rolling_mean"]
                bull_trap = c5["bullish_trap"]
                bear_trap = c5["bearish_trap"]
                
                imbalance = c5["imbalance"]
                
                signal = None

                # -----------------------------------------------------------------
                # OUTSIDE HOURS INFO BANNER (e.g. after 15:30, before 09:20)
                # -----------------------------------------------------------------
                if not is_valid_entry_time and not State.in_trade:
                    print(
                        "\n⏸ Outside entry hours (09:30–14:45). "
                        f"Time now: {now.strftime('%I:%M:%S %p')}. "
                        "No new trades will be taken today.",
                        flush=True,
                    )
                
                # Buy Call Condition (volume spike removed from entry filter)
                if is_valid_entry_time and is_15m_up and is_5m_up and atr_expand:
                    if imbalance > 0.3 or bull_trap:
                        signal = "BUY_CALL"
                        
                # Buy Put Condition
                if is_valid_entry_time and is_15m_dn and is_5m_dn and atr_expand:
                    if imbalance < -0.3 or bear_trap:
                        signal = "BUY_PUT"

                spot = c5["close"] if spot == 0 else spot
                
                # Outside entry window alert: conditions may be met but no new trades are allowed
                if signal and not State.in_trade and not is_valid_entry_time:
                    log.info(
                        f"Signal {signal} detected at {now.strftime('%H:%M')} "
                        f"but outside entry window (09:30–14:45). No trade taken."
                    )
                    print(
                        f"\n⚠ Signal {signal} detected but outside entry hours "
                        f"(09:30–14:45). Waiting for next session.",
                        flush=True,
                    )
                    # For the dashboard, treat this loop as 'waiting' (no active signal)
                    signal = None

                # Print Status
                if not State.in_trade:
                    waiting_line_count += 1
                    if waiting_line_count >= 10:
                        os.system('cls' if os.name == 'nt' else 'clear')
                        waiting_line_count = 0
                    print_market_snapshot(spot, df_5m, df_15m, signal, support, resistance)
                    # Show banner only after 15:30 (entry window end) and before square-off
                    after_entry_window = (now.hour > 15) or (now.hour == 15 and now.minute > 30)
                    if after_entry_window and now.strftime("%H:%M") < SQUARE_OFF_TIME:
                        print(
                            f"\n⏸ Outside entry hours (09:30–14:45). "
                            f"Time now: {now.strftime('%I:%M:%S %p')}. "
                            "No new trades will be taken today.",
                            flush=True,
                        )
                else:
                    # Option premium from quotes API (Day's Open strategy logic)
                    opt_premium = await UpstoxClient.get_option_premium(session, State.pos_ins_key)
                    opt_ltp = opt_premium if opt_premium is not None else State.pos_entry
                    # For any BUY, P&L = (LTP - Entry) * Qty
                    pnl = (opt_ltp - State.pos_entry) * State.pos_qty

                    # Premium-based SL/TP exits (Buying strategy: Premium RISE = Profit, Premium FALL = Loss)
                    if opt_ltp <= State.pos_sl_premium:
                        await close_position(session, "SL_HIT_PREMIUM")
                    elif opt_ltp >= State.pos_tp_premium:
                        await close_position(session, "TP_HIT_PREMIUM")
                    
                    # Clean Dashboard View (Only if still in trade after exit check)
                    if State.in_trade:
                        print_trade_dashboard(
                            spot, opt_ltp, State.pos_entry, 
                            State.pos_tp_premium, State.pos_sl_premium, 
                            pnl, State.pos_strike, State.pos_opt_type
                        )

                if signal and not State.in_trade:
                    # Optional: max trades per day
                    if MAX_TRADES_PER_DAY is not None and State.trades_today >= MAX_TRADES_PER_DAY:
                        log.info(f"Max trades per day ({MAX_TRADES_PER_DAY}) reached. Skipping signal.")
                    else:
                        strike = round(spot / STRIKE_STEP) * STRIKE_STEP
                        opt_type = "CE" if signal == "BUY_CALL" else "PE"
                        
                        # Format standard Upstox Expiry
                        if expiry:
                            try:
                                date_obj = datetime.strptime(expiry, "%Y-%m-%d")
                                exp_fmt = date_obj.strftime("%d%b%y").upper()
                                name = "BANKNIFTY" if IS_BANKNIFTY else "NIFTY"
                                instrument_key = f"NSE_FO|{name}{exp_fmt}{strike}{opt_type}"
                            except Exception:
                                instrument_key = f"NSE_FO|{INSTRUMENT.split('|')[1].replace(' ','').upper()}{strike}{opt_type}"
                        else:
                            instrument_key = f"NSE_FO|{INSTRUMENT.split('|')[1].replace(' ','').upper()}{strike}{opt_type}"
                        
                        # Position sizing: Fixed at 1 Lot (65 qty)
                        stop_distance = 1.5 * c5["atr_14"]
                        qty = LOT_SIZE
                        
                        # Premium-based SL/TP (Fixed 1 Lot) - Precise resolution from chain_data
                        opt_ltp = None
                        if chain_data:
                            for strike_data in chain_data:
                                if strike_data.get("strike_price") == strike:
                                    option_entry_data = strike_data.get("call_options" if signal == "BUY_CALL" else "put_options", {})
                                    instrument_key = option_entry_data.get("instrument_key")
                                    market_data = option_entry_data.get("market_data", {})
                                    opt_ltp = market_data.get("ltp") or market_data.get("last_price")
                                    break
                                    
                        if opt_ltp is None:
                            opt_ltp = await UpstoxClient.get_option_premium(session, instrument_key)
                        
                        if opt_ltp is None:
                            opt_ltp = float(spot)  # fallback
                            
                        # Option Premium Risk (Delta-Adjusted)
                        # Since we trade ATM, premium moves roughly 50% of spot movement
                        # For all BOUGHT options (Calls & Puts), we want premium to RISE.
                        base_risk = stop_distance * DELTA_APPROX
                        risk_per_unit = min(base_risk, opt_ltp * 0.7)
                        
                        sl_premium = opt_ltp - risk_per_unit
                        tp_premium = opt_ltp + 1.5 * risk_per_unit
                        
                        # Dynamic OI-Based Target (Adjust TP based on Support/Resistance)
                        if signal == "BUY_CALL" and resistance and float(resistance) > spot:
                            res_spot_diff = float(resistance) - spot
                            tp_premium = min(tp_premium, opt_ltp + (res_spot_diff * DELTA_APPROX))
                            log.info(f"Dynamic TP adjusted by OI Resistance: {resistance}")
                        elif signal == "BUY_PUT" and support and float(support) < spot:
                            sup_spot_diff = spot - float(support)
                            tp_premium = min(tp_premium, opt_ltp + (sup_spot_diff * DELTA_APPROX))
                            log.info(f"Dynamic TP adjusted by OI Support: {support}")

                        # Log professional signal alert
                        log_signal_alert(signal, strike, opt_ltp, sl_premium, tp_premium, spot, support, resistance, expiry)
                        
                        signal_msg = f"🚀 **SIGNAL GENERATED: {signal}**\n" \
                                     f"━━━━━━━━━━━━━━━━━━━━━━━━\n" \
                                     f"📍 **Spot Price:** {spot:.2f}\n" \
                                     f"🎯 **Target Strike:** {strike} {opt_type}\n" \
                                     f"🛡️ **Support (PE OI):** {support}\n" \
                                     f"🛑 **Resistance (CE OI):** {resistance}\n" \
                                     f"📅 **Expiry:** {expiry}\n" \
                                     f"━━━━━━━━━━━━━━━━━━━━━━━━"
                        await send_discord("Signal Generated", signal_msg, color=0x9B59B6)
                        
                        # Position tracking
                        State.pos_side = signal
                        State.pos_strike = strike
                        State.pos_opt_type = opt_type
                        State.pos_entry = opt_ltp
                        State.pos_entry_spot = spot
                        State.pos_stop_distance = stop_distance
                        State.pos_stop = spot - stop_distance if signal == "BUY_CALL" else spot + stop_distance
                        State.pos_qty = qty
                        State.pos_highest = spot
                        State.pos_lowest = spot
                        State.pos_sl_premium = sl_premium
                        State.pos_tp_premium = tp_premium
                        State.pos_tp_spot = spot + 1.5 * stop_distance if signal == "BUY_CALL" else spot - 1.5 * stop_distance
                        State.pos_ins_key = instrument_key
                        State.in_trade = True
                        
                        await place_order(session, State.pos_ins_key, qty, "BUY")
                        
                        order_msg = f"💰 **ORDER PLACED SUCCESSFULLY**\n" \
                                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n" \
                                    f"🏷️ **Signal:** {signal}\n" \
                                    f"🎯 **Strike:** {strike} {opt_type}\n" \
                                    f"📦 **Quantity:** {qty}\n" \
                                    f"📍 **Spot at Entry:** {spot:.2f}\n" \
                                    f"💎 **Entry Premium:** ₹{opt_ltp:.2f}\n" \
                                    f"🛑 **SL Premium:** ₹{sl_premium:.2f}\n" \
                                    f"🏆 **TP Premium:** ₹{tp_premium:.2f}\n" \
                                    f"━━━━━━━━━━━━━━━━━━━━━━━━"
                        await send_discord("Order Placed", order_msg, color=0x3498DB)

            except Exception as e:
                log.error(f"Error in main loop: {traceback.format_exc()}")
                
            # Dynamic sleep: 1s during trade for fast P&L monitoring, 10s otherwise
            sleep_time = 1 if State.in_trade else 10
            await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        log.info("Bot stopped manually.")
