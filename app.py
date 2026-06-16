"""
================================================================
  QUOTEX + FOREX CANDLE COLLECTOR BOT (PostgreSQL - Render Ready)
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
import logging
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
from psycopg2 import sql
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
DB_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/candles_db")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "6"))   # hours
RETENTION_DAYS = 20
OWNER = "GHULAM MUJTABA"
CONTACT = "@BINARYSUPPORT"
UTC5 = timezone(timedelta(hours=5))

# ---------- API ENDPOINTS ----------
QUOTEX_API = "https://xcharts.live/api/market/quotex/"
FOREX_API = "https://xcharts.live/api/market/forex/"

UPSTREAM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://xcharts.live/",
    "Accept": "application/json",
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
# DATABASE (PostgreSQL)
# ---------------------------------------------------------------
def get_db():
    """Get PostgreSQL connection for Render"""
    conn = psycopg2.connect(DB_URL, sslmode='require')
    conn.autocommit = False
    return conn

def init_db():
    """Create tables if they don't exist"""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS candles(
            id SERIAL PRIMARY KEY,
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT 'M1',
            open_time BIGINT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER,
            created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
            UNIQUE(pair, timeframe, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_candles_pair_tf_time 
            ON candles(pair, timeframe, open_time DESC);
        
        CREATE TABLE IF NOT EXISTS fetch_logs(
            id SERIAL PRIMARY KEY,
            level TEXT, message TEXT, pair TEXT, status TEXT,
            created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
        );
        """)
        conn.commit()
        log.info("PostgreSQL initialized successfully")
    except Exception as e:
        log.error("Database init error: %s", e)
        conn.rollback()
    finally:
        conn.close()

# ---------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------
def calc_volume(o, h, l, c, t):
    rng = abs(h - l)
    body = abs(c - o)
    mid = (h + l) / 2 or 1
    base = max((rng/mid)*100000 + (body/mid)*50000, 1)
    noise = ((t % 97)/97)*0.4 + 0.8
    return int(round(base * noise))

def to_utc5(ts):
    return datetime.fromtimestamp(ts, tz=UTC5).strftime("%Y-%m-%d %H:%M:%S")

def enrich(c):
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    return {
        "time": c["time"],
        "readable_time": to_utc5(c["time"]),
        "open": o, "high": h, "low": l, "close": cl,
        "volume": calc_volume(o, h, l, cl, c["time"]),
        "color": "green" if cl >= o else "red",
        "direction": "UP" if cl >= o else "DOWN",
    }

# ---------------------------------------------------------------
# COLLECTOR
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

    # Quotex logic with variants
    variants = [
        pair,
        pair.replace("-OTCq", "_OTCq"),
        pair.replace("-OTCq", "-OTC"),
        pair.replace("-OTCq", "_OTC"),
    ]
    for v in variants:
        try:
            r = requests.get(
                QUOTEX_API,
                params={"symbol": v, "interval": "1m", "limit": 600},
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
    
    # Delete old candles (20 days) and old logs (3 days)
    cutoff = int(time.time()) - RETENTION_DAYS * 86400
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM candles WHERE open_time < %s", (cutoff,))
        cur.execute("DELETE FROM fetch_logs WHERE created_at < %s", (int(time.time()) - 3*86400,))
        conn.commit()
    except Exception as e:
        log.error("Delete old data error: %s", e)
        conn.rollback()
    finally:
        conn.close()

    total_ins = 0

    # ---------- Quotex Pairs ----------
    for pair in QUOTEX_PAIRS:
        candles, variant = fetch_pair(pair, market='quotex')
        if not candles:
            conn = get_db()
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(%s,%s,%s,%s)",
                           ("warn", f"no data for {pair}", pair, "fail"))
                conn.commit()
            except Exception as e:
                log.error("Log insert error: %s", e)
                conn.rollback()
            finally:
                conn.close()
            time.sleep(0.5)
            continue

        conn = get_db()
        try:
            cur = conn.cursor()
            for c in candles:
                volume = calc_volume(c["open"], c["high"], c["low"], c["close"], c["time"])
                cur.execute("""
                    INSERT INTO candles(pair,timeframe,open_time,open,high,low,close,volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pair, timeframe, open_time) DO NOTHING
                """, (pair, "M1", c["time"], c["open"], c["high"], c["low"], c["close"], volume))
            
            cur.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(%s,%s,%s,%s)",
                       ("info", f"collected {len(candles)} ({variant})", pair, "ok"))
            conn.commit()
            total_ins += len(candles)
            log.info("  [Q] %-20s  %4d candles", pair, len(candles))
        except Exception as e:
            log.error("Insert error for %s: %s", pair, e)
            conn.rollback()
        finally:
            conn.close()
        time.sleep(1.5)

    # ---------- Forex Pairs ----------
    for pair in FOREX_PAIRS:
        candles, variant = fetch_pair(pair, market='forex')
        if not candles:
            conn = get_db()
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(%s,%s,%s,%s)",
                           ("warn", f"no data for {pair}", pair, "fail"))
                conn.commit()
            except Exception as e:
                log.error("Log insert error: %s", e)
                conn.rollback()
            finally:
                conn.close()
            time.sleep(0.5)
            continue

        conn = get_db()
        try:
            cur = conn.cursor()
            for c in candles:
                volume = calc_volume(c["open"], c["high"], c["low"], c["close"], c["time"])
                cur.execute("""
                    INSERT INTO candles(pair,timeframe,open_time,open,high,low,close,volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pair, timeframe, open_time) DO NOTHING
                """, (pair, "M1", c["time"], c["open"], c["high"], c["low"], c["close"], volume))
            
            cur.execute("INSERT INTO fetch_logs(level,message,pair,status) VALUES(%s,%s,%s,%s)",
                       ("info", f"collected {len(candles)}", pair, "ok"))
            conn.commit()
            total_ins += len(candles)
            log.info("  [F] %-20s  %4d candles", pair, len(candles))
        except Exception as e:
            log.error("Insert error for %s: %s", pair, e)
            conn.rollback()
        finally:
            conn.close()
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

@app.route("/get-candles")
def get_candles():
    symbol = request.args.get("symbol", "").upper()
    interval = request.args.get("interval", "1m")
    lim = int(request.args.get("limit", "100"))

    if not symbol:
        return brand({"error": "symbol required"}, 400)

    actual_symbol = symbol
    if symbol.endswith("-OTC") and not symbol.endswith("-OTCq"):
        actual_symbol = symbol + "q"

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT open_time, open, high, low, close, volume 
            FROM candles 
            WHERE pair=%s AND timeframe='M1' 
            ORDER BY open_time DESC 
            LIMIT %s
        """, (actual_symbol, lim))
        rows = cur.fetchall()
    except Exception as e:
        log.error("Query error: %s", e)
        return brand({"error": str(e)}, 500)
    finally:
        conn.close()

    # Convert tuples to dicts
    candles_out = []
    for row in reversed(rows):
        r = {"time": row[0], "open": row[1], "high": row[2], "low": row[3], "close": row[4]}
        candles_out.append(enrich(r))

    return brand({
        "symbol": symbol,
        "actual_symbol": actual_symbol,
        "interval": interval,
        "count": len(candles_out),
        "candles": candles_out
    })

@app.route("/pairs")
def pairs_stats():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT pair, COUNT(*) as count
            FROM candles
            GROUP BY pair
            ORDER BY pair
        """)
        counts = cur.fetchall()
        
        cur.execute("""
            SELECT pair, MAX(created_at) as last_ts
            FROM fetch_logs
            WHERE status='ok'
            GROUP BY pair
        """)
        last_fetch = cur.fetchall()
    except Exception as e:
        log.error("Query error: %s", e)
        return brand({"error": str(e)}, 500)
    finally:
        conn.close()

    stats = {}
    for row in counts:
        stats[row[0]] = {"candles": row[1], "last_fetch_utc5": None}
    for row in last_fetch:
        if row[0] in stats:
            stats[row[0]]["last_fetch_utc5"] = to_utc5(row[1])
        else:
            stats[row[0]] = {"candles": 0, "last_fetch_utc5": to_utc5(row[1])}

    all_pairs = set(QUOTEX_PAIRS + FOREX_PAIRS)
    for p in all_pairs:
        if p not in stats:
            stats[p] = {"candles": 0, "last_fetch_utc5": None}

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
if __name__ == "__main__":
    init_db()
    start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=False)
else:
    # Render uses Gunicorn - initialize DB and scheduler on import
    init_db()
    start_scheduler()
