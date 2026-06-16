"""
================================================================
  QUOTEX + FOREX CANDLE COLLECTOR BOT (Final - 20 Days Retention)
  Owner: GHULAM MUJTABA  |  Contact: @BINARYSUPPORT
================================================================
  Endpoints:
  GET /get-candles?symbol=SYMBOL&limit=100   (OTC auto-detect)
  GET /pairs                                 (pair stats)
  Collector runs every 6 hours (auto)
  Retention: 20 days (old data auto-deleted)
================================================================
"""

import os
import time
import sqlite3
import logging
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
DB_PATH        = os.getenv("DB_PATH", "candles.db")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "6"))   # hours
RETENTION_DAYS = 20   # <--- YAHAN CHANGE KAR DIYA (8 se 20)
OWNER          = "GHULAM MUJTABA"
CONTACT        = "@BINARYSUPPORT"
UTC5           = timezone(timedelta(hours=5))

# ---------- API ENDPOINTS ----------
QUOTEX_API = "https://xcharts.live/api/market/quotex/"
FOREX_API  = "https://xcharts.live/api/market/forex/"

UPSTREAM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":    "https://xcharts.live/",
    "Accept":     "application/json",
}

# ---------- PAIRS LISTS ----------
QUOTEX_PAIRS = [
    "AUDCAD-OTCq", "AUDCHF-OTCq", "AUDJPY-OTCq", "AUDNZD-OTCq", "AUDUSD-OTCq",
    "AMERICAN EXPRESS-OTCq", "BOI-OTCq", "BRLUSD-OTCq", "BTCUSD-OTCq",
    "CADCHF-OTCq", "CADJPY-OTCq", "CHFJPY-OTCq",
    "EURAUD-OTCq", "EURCAD-OTCq", "EURCHF-OTCq", "EURGBP-OTCq", "EURJPY-OTCq",
    "EURNZD-OTCq", "EURSGD-OTCq", "EURUSD-OTCq",
    "FACEBOOK-OTCq",
    "GBPAUD-OTCq", "GBPCAD-OTCq", "GBPCHF-OTCq", "GBPJPY-OTCq", "GBPNZD-OTCq",
    "GBPUSD-OTCq",
    "INTEL-OTCq",
    "JOHNSON & JOHNSON-OTCq",
    "MCDONALDS-OTCq", "MICROSOFT-OTCq",
    "NZDCAD-OTCq", "NZDCHF-OTCq", "NZDJPY-OTCq", "NZDUSD-OTCq",
    "PIZFER-OTCq",
    "USDARS-OTCq", "USDBDT-OTCq", "USDCAD-OTCq", "USDCHF-OTCq", "USDCOP-OTCq",
    "USDDZD-OTCq", "USDEGP-OTCq", "USDIDR-OTCq", "USDINR-OTCq", "USDJPY-OTCq",
    "USDMXN-OTCq", "USDPKR-OTCq", "USDZAR-OTCq"
]

FOREX_PAIRS = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDUSD",
    "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "EURUSD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "GBPUSD",
    "NZDUSD", "USDCAD", "USDCHF", "USDJPY"
]

TOTAL_PAIRS = len(QUOTEX_PAIRS) + len(FOREX_PAIRS)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("candle-bot")

# ---------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS candles(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT 'M1',
            open_time INTEGER NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER,
            created_at INTEGER DEFAULT (strftime('%s','now')),
            UNIQUE(pair, timeframe, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_candles_pair_tf_time
            ON candles(pair, timeframe, open_time DESC);

        CREATE TABLE IF NOT EXISTS fetch_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT, message TEXT, pair TEXT, status TEXT,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        );
        """)
    log.info("DB initialized at %s", DB_PATH)

# ---------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------
def calc_volume(o, h, l, c, t):
    rng  = abs(h - l)
    body = abs(c - o)
    mid  = (h + l) / 2 or 1
    base = max((rng/mid)*100000 + (body/mid)*50000, 1)
    noise = ((t % 97)/97)*0.4 + 0.8
    return int(round(base * noise))

def to_utc5(ts):
    return datetime.fromtimestamp(ts, tz=UTC5).strftime("%Y-%m-%d %H:%M:%S")

def enrich(c):
    o,h,l,cl = c["open"], c["high"], c["low"], c["close"]
    return {
        "time": c["time"],
        "readable_time": to_utc5(c["time"]),
        "open": o, "high": h, "low": l, "close": cl,
        "volume": calc_volume(o,h,l,cl,c["time"]),
        "color": "green" if cl >= o else "red",
        "direction": "UP" if cl >= o else "DOWN",
    }

# ---------------------------------------------------------------
# COLLECTOR (Supports both Quotex and Forex)
# ---------------------------------------------------------------
def fetch_pair(pair, market='quotex'):
    if market == 'forex':
        try:
            r = requests.get(
                FOREX_API,
                params={"symbol": pair, "interval": "1m", "limit": 600},
                headers=UPSTREAM_HEADERS, timeout=10,
            )
            if not r.ok: return [], ""
            data = r.json()
            raw = data if isinstance(data, list) else data.get("candles") or data.get("data") or []
            raw = [c for c in raw if isinstance(c, dict) and "time" in c and "open" in c]
            return raw, pair
        except Exception as e:
            log.warning("Forex fetch %s failed: %s", pair, e)
            return [], ""

    # ---------- Quotex logic (with variants) ----------
    variants = [
        pair,
        pair.replace("-OTCq","_OTCq"),
        pair.replace("-OTCq","-OTC"),
        pair.replace("-OTCq","_OTC"),
    ]
    for v in variants:
        try:
            r = requests.get(
                QUOTEX_API,
                params={"symbol": v, "interval":"1m", "limit": 600},
                headers=UPSTREAM_HEADERS, timeout=10,
            )
            if not r.ok: continue
            data = r.json()
            raw = data if isinstance(data, list) else data.get("candles") or data.get("data") or []
            raw = [c for c in raw if isinstance(c, dict) and "time" in c and "open" in c]
            if raw: return raw, v
        except Exception as e:
            log.warning("Quotex fetch %s (%s) failed: %s", pair, v, e)
    return [], ""

def collect_all():
    log.info("=== Collection cycle started (Total %d pairs) ===", TOTAL_PAIRS)
    
    # 20 din purani candles delete karo
    cutoff = int(time.time()) - RETENTION_DAYS * 86400
    with db() as c:
        c.execute("DELETE FROM candles WHERE open_time < ?", (cutoff,))
        # Fetch logs sirf 3 din rakhte hain (yeh waise hi rahne do)
        c.execute("DELETE FROM fetch_logs WHERE created_at < ?",
                  (int(time.time()) - 3*86400,))

    total_ins = 0

    # 1. Fetch Quotex Pairs
    for pair in QUOTEX_PAIRS:
        candles, variant = fetch_pair(pair, market='quotex')
        if not candles:
            with db() as c:
                c.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(?,?,?,?)",
                          ("warn", f"no data for {pair}", pair, "fail"))
            time.sleep(0.5)
            continue

        rows = [(pair,"M1", c["time"], c["open"], c["high"], c["low"], c["close"],
                 calc_volume(c["open"],c["high"],c["low"],c["close"],c["time"]))
                for c in candles]
        with db() as conn:
            conn.executemany("""INSERT OR IGNORE INTO candles
                (pair,timeframe,open_time,open,high,low,close,volume)
                VALUES (?,?,?,?,?,?,?,?)""", rows)
            conn.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(?,?,?,?)",
                         ("info", f"collected {len(rows)} ({variant})", pair, "ok"))
        total_ins += len(rows)
        log.info("  [Q] %-20s  %4d candles", pair, len(rows))
        time.sleep(1.5)

    # 2. Fetch Forex Pairs
    for pair in FOREX_PAIRS:
        candles, variant = fetch_pair(pair, market='forex')
        if not candles:
            with db() as c:
                c.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(?,?,?,?)",
                          ("warn", f"no data for {pair}", pair, "fail"))
            time.sleep(0.5)
            continue

        rows = [(pair,"M1", c["time"], c["open"], c["high"], c["low"], c["close"],
                 calc_volume(c["open"],c["high"],c["low"],c["close"],c["time"]))
                for c in candles]
        with db() as conn:
            conn.executemany("""INSERT OR IGNORE INTO candles
                (pair,timeframe,open_time,open,high,low,close,volume)
                VALUES (?,?,?,?,?,?,?,?)""", rows)
            conn.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(?,?,?,?)",
                         ("info", f"collected {len(rows)}", pair, "ok"))
        total_ins += len(rows)
        log.info("  [F] %-20s  %4d candles", pair, len(rows))
        time.sleep(1.5)

    log.info("=== Done. Total inserted/ignored = %d ===", total_ins)
    return total_ins

# ---------------------------------------------------------------
# FLASK APP
# ---------------------------------------------------------------
app = Flask(__name__)

def brand(payload, status=200):
    payload = {**payload, "OWNER": OWNER, "CONTACT": CONTACT,
               "server_time_utc5": to_utc5(int(time.time()))}
    return jsonify(payload), status


@app.route("/")
def health():
    return brand({"status":"running","message":"candles collector online"})
    
# ---------- ENDPOINT 1: Get candles (with OTC auto-detect) ----------
@app.route("/get-candles")
def get_candles():
    symbol   = request.args.get("symbol","").upper()
    interval = request.args.get("interval","1m")
    lim      = int(request.args.get("limit", "100"))

    if not symbol:
        return brand({"error":"symbol required"}, 400)

    # OTC detection: agar -OTC hai aur -OTCq nahi, toh q add karo
    actual_symbol = symbol
    if symbol.endswith("-OTC") and not symbol.endswith("-OTCq"):
        actual_symbol = symbol + "q"

    with db() as c:
        rows = c.execute("""SELECT open_time,open,high,low,close,volume FROM candles
            WHERE pair=? AND timeframe='M1' ORDER BY open_time DESC LIMIT ?""",
            (actual_symbol, lim)).fetchall()
    rows = list(reversed([dict(r) for r in rows]))
    out  = [enrich({"time":r["open_time"], **{k:r[k] for k in ("open","high","low","close")}}) for r in rows]
    return brand({
        "symbol": symbol,
        "actual_symbol": actual_symbol,
        "interval": interval,
        "count": len(out),
        "candles": out
    })

# ---------- ENDPOINT 2: Pair stats (candle count + last fetch) ----------
@app.route("/pairs")
def pairs_stats():
    with db() as c:
        # Total candles per pair
        counts = c.execute("""
            SELECT pair, COUNT(*) as count
            FROM candles
            GROUP BY pair
            ORDER BY pair
        """).fetchall()
        
        # Last successful fetch time per pair (from fetch_logs)
        last_fetch = c.execute("""
            SELECT pair, MAX(created_at) as last_ts
            FROM fetch_logs
            WHERE status='ok'
            GROUP BY pair
        """).fetchall()
    
    # Merge data
    stats = {}
    for row in counts:
        stats[row["pair"]] = {"candles": row["count"], "last_fetch_utc5": None}
    for row in last_fetch:
        if row["pair"] in stats:
            stats[row["pair"]]["last_fetch_utc5"] = to_utc5(row["last_ts"])
        else:
            stats[row["pair"]] = {"candles": 0, "last_fetch_utc5": to_utc5(row["last_ts"])}
    
    # Include all pairs from our lists for completeness
    all_pairs = set(QUOTEX_PAIRS + FOREX_PAIRS)
    for p in all_pairs:
        if p not in stats:
            stats[p] = {"candles": 0, "last_fetch_utc5": None}
    
    # Convert to list for response
    result = [{"pair": p, **stats[p]} for p in sorted(stats.keys())]
    
    return brand({
        "total_pairs": len(result),
        "pairs": result
    })

# ---------------------------------------------------------------
# SCHEDULER
# ---------------------------------------------------------------
def start_scheduler():
    sch = BackgroundScheduler(timezone="UTC")
    sch.add_job(collect_all, "interval", hours=FETCH_INTERVAL,
                next_run_time=datetime.utcnow() + timedelta(seconds=10),
                id="collect", max_instances=1, coalesce=True)
    sch.start()
    log.info("Scheduler started: every %dh", FETCH_INTERVAL)

# ---------------------------------------------------------------
# BOOT
# ---------------------------------------------------------------
init_db()
start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)