"""
ThetaTracker Pro — ThetaData bridge service.

WHY THIS EXISTS. The journal's backend is Node. ThetaData ships a Python library and no Node
SDK, and their gRPC endpoint would otherwise have to be hand-rolled from protobuf. So this is
a small Python service that speaks ThetaData on one side and plain JSON on the other. The Node
backend calls it over Railway's private network.

It deliberately does NOT run Theta Terminal. Their Python library connects straight to a hosted
endpoint (mdds-*.thetadata.us:443 over TLS), so there is no Java, no self-updating JAR, and no
local process to babysit through a redeploy.

Environment variables (Railway, never in code):
    THETADATA_API_KEY        from the ThetaData user portal
    SERVICE_SHARED_SECRET    any long random string; the Node backend sends it as X-TTP-Key
    ALLOW_UNAUTHENTICATED    optional, "1" to disable the shared-secret check (local only)
    THETADATA_FEED           optional, "market_value" (default) or "realtime". See FEED below.
"""

import datetime
import logging
import os
import threading
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

log = logging.getLogger("thetadata-bridge")
logging.basicConfig(level=logging.INFO)

PACIFIC = ZoneInfo("America/Los_Angeles")
API_KEY = os.getenv("THETADATA_API_KEY", "").strip()
SHARED_SECRET = os.getenv("SERVICE_SHARED_SECRET", "").strip()
ALLOW_OPEN = os.getenv("ALLOW_UNAUTHENTICATED", "").strip() in ("1", "true", "True")

# ── FEED: Market Value vs real-time ──────────────────────────────────────────
# Market Value is NOT an account setting and NOT a different API key. ThetaData support
# (Anthony, Sep 21 2026) confirmed that it is separate endpoints plus a request parameter,
# and must be asked for explicitly on every call:
#   index spot     index_snapshot_market_value   -> market_price   (not index_snapshot_price)
#   greeks / IV    same call, use_market_value=True (the library default is False)
#   bid / ask      option_snapshot_market_value  -> market_bid/ask (not option_snapshot_quote)
#   expirations    reference data, no Market Value variant exists
# Real-time NBBO shown to subscribers is OPRA redistribution ($1,500/mo + $1.25/user). Market
# Value is what our pricing assumes, so it is the DEFAULT, and realtime has to be chosen
# deliberately. Greeks responses do not echo use_market_value back, so the only record of
# which feed produced a number is the "feed" field this service stamps on every response.
FEED = os.getenv("THETADATA_FEED", "market_value").strip().lower().replace("-", "_")
if FEED not in ("market_value", "realtime"):
    raise RuntimeError("THETADATA_FEED must be 'market_value' or 'realtime', got %r" % FEED)
USE_MV = FEED == "market_value"

app = FastAPI(title="ThetaData bridge", docs_url=None, redoc_url=None, openapi_url=None)


# ── client ───────────────────────────────────────────────────────────────────
# Authenticating costs a round trip, so the client is built once and reused. The library is
# synchronous, so every endpoint below is a plain `def` — FastAPI then runs it in a worker
# thread instead of blocking the event loop, which an `async def` around blocking gRPC would.
_client = None
_client_lock = threading.Lock()


def client():
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        if not API_KEY:
            raise HTTPException(503, "THETADATA_API_KEY is not set on this service.")
        try:
            from thetadata import ThetaClient
        except Exception as e:                      # pragma: no cover
            # thetadata 1.0.10 imports dotenv without declaring python-dotenv as a dependency,
            # so a clean install fails here. requirements.txt pins it explicitly.
            raise HTTPException(503, "thetadata import failed: %s" % e)
        try:
            _client = ThetaClient(api_key=API_KEY, dataframe_type="pandas")
        except Exception as e:
            raise HTTPException(502, "ThetaData authentication failed: %s" % e)
        log.info("ThetaData client authenticated (feed=%s)", FEED)
    return _client


def require_key(x_ttp_key: Optional[str]):
    if ALLOW_OPEN:
        return
    if not SHARED_SECRET:
        raise HTTPException(503, "SERVICE_SHARED_SECRET is not set on this service.")
    if not x_ttp_key or x_ttp_key != SHARED_SECRET:
        raise HTTPException(401, "Bad or missing X-TTP-Key.")


# ── frame helpers, carried over from the capture script ──────────────────────
# These were arrived at against live data and each one exists because a naive version was wrong.

# A column named bid_size is not the bid. Substring matching alone reported sizes as prices.
COL_NOISE = ("size", "exchange", "condition", "count", "sequence", "flag", "time", "ms_of_day")
PRICE_NAMES = ("market_price", "price", "last", "value", "close", "mid", "spot", "index_price")
SYMBOL_NAMES = ("symbol", "root", "sym", "ticker", "underlying")
TIME_NAMES = ("timestamp", "time", "datetime", "quote_time")


def rows_of(df) -> List[Dict[str, Any]]:
    try:
        return df.to_dict("records")
    except Exception:
        try:
            return list(df.iter_rows(named=True))
        except Exception:
            return []


def col(d: Dict[str, Any], *names):
    """Exact column name wins; a substring match is accepted only when the name carries no
    size/exchange/condition noise."""
    low = {str(k).lower(): k for k in d}
    for n in names:
        if n in low:
            return d[low[n]]
    for n in names:
        for kl, k in low.items():
            if n in kl and not any(bad in kl for bad in COL_NOISE):
                return d[k]
    return None


def to_pacific(v):
    try:
        ts = v.tz_convert(PACIFIC) if hasattr(v, "tz_convert") else v.astimezone(PACIFIC)
        return ts.isoformat()
    except Exception:
        return None


def jsonable(v):
    """gRPC frames carry numpy scalars and pandas timestamps; neither serialises directly."""
    if v is None:
        return None
    if isinstance(v, (bool, int, float, str)):
        return v
    for attr in ("item", "isoformat"):
        if hasattr(v, attr):
            try:
                return getattr(v, attr)()
            except Exception:
                pass
    return str(v)


def chain_scale(rows: List[Dict[str, Any]], spot: Optional[float]) -> float:
    """Strikes arrive either plainly (766) or in thousandths (766000). Infer from the data."""
    vals = []
    for d in rows:
        try:
            vals.append(float(col(d, "strike")))
        except (TypeError, ValueError):
            pass
    if not vals or not spot:
        return 1.0
    mid = sorted(vals)[len(vals) // 2]
    return 1000.0 if abs(mid / 1000.0 - spot) < abs(mid - spot) else 1.0


RIGHT_WORDS = {"P": ("p", "put"), "C": ("c", "call")}


def right_matches(raw, want: str) -> bool:
    s = str(raw).strip().lower()
    return s in RIGHT_WORDS[want]


def with_root_fallback(fn, symbol: str, **kw):
    """SPX monthlies (third Friday) sit under root SPX; every other SPX expiration sits under
    SPXW. Verified live: SPX 2026-10-09 returns no data while SPX 2026-09-18 and 2026-11-20 do.
    Try the plain root, then the weekly one, and report which answered."""
    last = None
    for root in (symbol, symbol + "W"):
        try:
            df = fn(symbol=root, **kw)
            rows = rows_of(df)
            if rows:
                return rows, root, None
        except Exception as e:
            last = e
    return [], None, last


# ── routes ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health(x_ttp_key: Optional[str] = Header(None, alias="X-TTP-Key")):
    """Is the service able to reach ThetaData, and what is this account entitled to?
    The indices entitlement is the CGIF answer and comes straight from the auth response."""
    require_key(x_ttp_key)
    out = {
        "ok": True,
        "apiKeySet": bool(API_KEY),
        "sharedSecretSet": bool(SHARED_SECRET),
        "checkedAt": datetime.datetime.now(PACIFIC).isoformat(),
        "feed": FEED,
    }
    try:
        c = client()
        out["subscriptions"] = {
            "indices": getattr(c, "index_subscription", None),
            "options": getattr(c, "options_subscription", None),
            "stocks": getattr(c, "stock_subscription", None),
        }
        out["authenticated"] = True
        if not out["subscriptions"]["indices"]:
            out["ok"] = False
            out["note"] = "No indices entitlement — SPX/XSP index values will not be available."
    except HTTPException as e:
        out["ok"] = False
        out["authenticated"] = False
        out["note"] = e.detail
    return out


@app.get("/spot")
def spot(symbols: str = Query("SPX,XSP"),
         x_ttp_key: Optional[str] = Header(None, alias="X-TTP-Key")):
    """Live index spot. The `dataTimestamp` is the market's own print time, NOT when this was
    called — measured 51 minutes apart after the close. Anything comparing against a broker
    screen must use dataTimestamp, or it is comparing two different moments."""
    require_key(x_ttp_key)
    wanted = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not wanted:
        raise HTTPException(400, "No symbols given.")
    called = datetime.datetime.now(PACIFIC)
    try:
        c = client()
        snap = c.index_snapshot_market_value(wanted) if USE_MV else c.index_snapshot_price(wanted)
    except HTTPException:
        # Our own errors carry the right status and the right explanation already. Without this
        # re-raise, the broad handler below relabels a missing API key (503, our configuration)
        # as an upstream failure (502, ThetaData's fault) — and sends you debugging the wrong end.
        raise
    except Exception as e:
        raise HTTPException(502, "%s failed: %s" % (
            "index_snapshot_market_value" if USE_MV else "index_snapshot_price", e))

    prices, stamps = {}, {}
    for d in rows_of(snap):
        sym = col(d, *SYMBOL_NAMES)
        if sym is None or str(sym).upper() not in wanted:
            continue
        sym = str(sym).upper()
        px = col(d, *PRICE_NAMES)
        if isinstance(px, (int, float)):
            prices[sym] = float(px)
        t = col(d, *TIME_NAMES)
        if t is not None:
            stamps[sym] = to_pacific(t)

    newest = max([s for s in stamps.values() if s], default=None)
    lag = None
    if newest:
        try:
            lag = (called - datetime.datetime.fromisoformat(newest)).total_seconds()
        except Exception:
            lag = None

    out = {"ok": bool(prices), "feed": FEED, "prices": prices, "timestamps": stamps,
           "dataTimestamp": newest, "calledAt": called.isoformat(), "feedLagSeconds": lag}
    # XSP is SPX/10, separately calculated. Each feed disagrees with itself by ~0.02 XSP points,
    # which is the noise floor of independent index calculation — report it, don't hide it.
    # On Market Value each index also carries its own random $0.01-0.05 offset, so expect this
    # to sit higher (up to ~0.05) than the ~0.002 seen on the real-time feed on day 1.
    if "SPX" in prices and "XSP" in prices:
        out["xspVsSpxOver10"] = round(abs(prices["XSP"] - prices["SPX"] / 10.0), 4)
    if not prices:
        raise HTTPException(502, "No prices parsed from the snapshot.")
    return out


@app.get("/greeks")
def greeks(symbol: str,
           expiration: str,
           strike: float,
           right: str = Query("P", pattern="^[PCpc]$"),
           x_ttp_key: Optional[str] = Header(None, alias="X-TTP-Key")):
    """Greeks for ONE contract. Pulls the whole chain and matches locally, because ThetaData's
    strike and right wire formats are undocumented and the nearest LISTED strike is often not
    the one asked for — XSP Nov 20 runs 5-point increments, so 766 and 804 do not exist there."""
    require_key(x_ttp_key)
    right = right.upper()
    sym = symbol.strip().upper()

    sp = None
    try:
        s = spot(symbols=sym if sym in ("SPX", "XSP") else "SPX", x_ttp_key=x_ttp_key)
        sp = s["prices"].get(sym) or (s["prices"].get("SPX", 0) / 10.0 if sym == "XSP" else None)
    except Exception:
        pass

    rows, root, err = with_root_fallback(
        client().option_snapshot_greeks_first_order, sym,
        expiration=expiration, strike="*", right="both", use_market_value=USE_MV)
    if not rows:
        raise HTTPException(502, "No chain for %s %s: %s" % (sym, expiration, err))

    scale = chain_scale(rows, sp)
    best, best_k, best_gap = None, None, None
    for d in rows:
        s_raw, r_raw = col(d, "strike"), col(d, "right")
        if s_raw is None or (r_raw is not None and not right_matches(r_raw, right)):
            continue
        try:
            k = float(s_raw) / scale
        except (TypeError, ValueError):
            continue
        gap = abs(k - strike)
        if best_gap is None or gap < best_gap:
            best, best_k, best_gap = d, k, gap
    if best is None:
        raise HTTPException(404, "No %s contract found on %s %s." % (right, sym, expiration))

    iv = col(best, "implied_vol", "implied_volatility", "implied", "iv")
    return {
        "ok": True, "feed": FEED, "symbol": sym, "rootUsed": root, "expiration": expiration,
        "right": right, "strikeRequested": strike, "strikeUsed": best_k,
        "strikeSnapped": abs(best_k - strike) > 0.001,
        "underlyingSpot": sp,
        "delta": jsonable(col(best, "delta")),
        "theta": jsonable(col(best, "theta")),
        "vega": jsonable(col(best, "vega")),
        "gamma": jsonable(col(best, "gamma")),
        # ThetaData returns IV as a fraction (0.1448); brokers display a percent (14.48%).
        "impliedVolFraction": jsonable(iv),
        "impliedVolPercent": (float(iv) * 100) if isinstance(iv, (int, float)) and iv < 5 else jsonable(iv),
        "dataTimestamp": to_pacific(col(best, *TIME_NAMES)),
    }


@app.get("/chain")
def chain(symbol: str,
          expiration: str,
          quotes: bool = Query(False),
          x_ttp_key: Optional[str] = Header(None, alias="X-TTP-Key")):
    """A whole expiration in one response — every strike, both rights, greeks attached, and
    optionally bid/ask. This is what makes the Roll Choices Engine feasible: it walks a
    strike-by-expiration grid, and one request per expiration is cheap where one per strike
    would not be. Measured live: 1,166 rows for SPX 2026-09-18 in a single call."""
    require_key(x_ttp_key)
    sym = symbol.strip().upper()

    g_rows, root, err = with_root_fallback(
        client().option_snapshot_greeks_first_order, sym,
        expiration=expiration, strike="*", right="both", use_market_value=USE_MV)
    if not g_rows:
        raise HTTPException(502, "No chain for %s %s: %s" % (sym, expiration, err))

    # Strikes arrive plainly (766) or in thousandths (766000), undocumented either way.
    # /greeks has always inferred this from the data; /chain did not, and returned whatever
    # the feed sent. A consumer walking a strike grid would then match nothing and see an
    # empty chain rather than an error. Same inference, same helper, one source of truth.
    sp = None
    try:
        sq = spot(symbols=sym if sym in ("SPX", "XSP") else "SPX", x_ttp_key=x_ttp_key)
        sp = sq["prices"].get(sym) or (
            sq["prices"].get("SPX", 0) / 10.0 if sym == "XSP" else None)
    except Exception:
        pass
    scale = chain_scale(g_rows, sp)

    q_index = {}
    if quotes:
        q_rows, _, _ = with_root_fallback(
            client().option_snapshot_market_value if USE_MV else client().option_snapshot_quote, sym,
            expiration=expiration, strike="*", right="both")
        for d in q_rows:
            k, r = col(d, "strike"), col(d, "right")
            if k is not None:
                q_index[(str(k), str(r).upper()[:1])] = d

    out = []
    for d in g_rows:
        k, r = col(d, "strike"), col(d, "right")
        iv = col(d, "implied_vol", "implied_volatility", "implied", "iv")
        try:
            k_scaled = float(k) / scale if k is not None else None
        except (TypeError, ValueError):
            k_scaled = None
        row = {
            "strike": jsonable(k_scaled),
            "right": (str(r).upper()[:1] if r is not None else None),
            "delta": jsonable(col(d, "delta")),
            "theta": jsonable(col(d, "theta")),
            "vega": jsonable(col(d, "vega")),
            "gamma": jsonable(col(d, "gamma")),
            "impliedVolFraction": jsonable(iv),
        }
        if quotes:
            q = q_index.get((str(k), (str(r).upper()[:1] if r is not None else None)))
            if q:
                bid = jsonable(col(q, "market_bid", "bid"))
                ask = jsonable(col(q, "market_ask", "ask"))
                # CROSSED-QUOTE GUARD. Market Value nudges one side by a tick, and on a market only
                # a tick wide that can push the bid past the ask (Sep 22 capture: XSP 777P 0DTE,
                # bid 1.05 / ask 1.04). The mid is still right; the pair is not. A bid above the
                # ask is never a real market, so show both as the mid and say so.
                if isinstance(bid, (int, float)) and isinstance(ask, (int, float)) and bid > ask:
                    mid = round((bid + ask) / 2.0, 4)
                    row["quoteUncrossed"] = {"bid": bid, "ask": ask}
                    bid = ask = mid
                row["bid"] = bid
                row["ask"] = ask
        out.append(row)

    return {"ok": True, "feed": FEED, "symbol": sym, "rootUsed": root, "expiration": expiration,
            "count": len(out), "withQuotes": quotes, "strikeScale": scale,
            "spotUsedForScale": sp, "contracts": out}


def norm_expiration(raw):
    """Expirations come back as a date, as 20260918, or as '2026-09-18' depending on the
    call. Normalise to an ISO date string, or None if it is none of those."""
    if raw is None:
        return None
    if isinstance(raw, datetime.datetime):
        return raw.date().isoformat()
    if isinstance(raw, datetime.date):
        return raw.isoformat()
    t = str(raw).strip()
    if not t:
        return None
    digits = t.replace("-", "").replace("/", "")
    if len(digits) == 8 and digits.isdigit():
        try:
            return datetime.date(int(digits[0:4]), int(digits[4:6]), int(digits[6:8])).isoformat()
        except ValueError:
            return None
    try:
        return datetime.date.fromisoformat(t[:10]).isoformat()
    except ValueError:
        return None


@app.get("/expirations")
def expirations(symbol: str,
                withinDays: int = Query(120),
                x_ttp_key: Optional[str] = Header(None, alias="X-TTP-Key")):
    """Which expirations actually exist for this underlying, and how many days out each is.

    The Roll Choices Engine walks a strike-by-expiration surface out to a duration ceiling.
    Without this it would have to guess dates and pay for every miss against a metered feed.

    BOTH ROOTS are queried and unioned: SPX monthlies (third Friday) sit under SPX and every
    other SPX expiration sits under SPXW, so querying one root alone returns a partial
    calendar that looks complete."""
    require_key(x_ttp_key)
    sym = symbol.strip().upper()
    today = datetime.datetime.now(PACIFIC).date()

    found = {}
    errs = []
    for root in (sym, sym + "W"):
        try:
            rows = rows_of(client().option_list_expirations(root))
        except Exception as e:
            errs.append("%s: %s" % (root, e))
            continue
        for d in rows:
            iso = norm_expiration(col(d, "expiration", "expiry", "date", "exp"))
            if not iso:
                continue
            found.setdefault(iso, set()).add(root)

    if not found:
        raise HTTPException(502, "No expirations for %s (%s)" % (sym, "; ".join(errs) or "no rows"))

    out = []
    for iso in sorted(found):
        dte = (datetime.date.fromisoformat(iso) - today).days
        if dte < 0 or dte > withinDays:
            continue
        out.append({"expiration": iso, "dte": dte, "roots": sorted(found[iso])})

    return {"ok": True, "symbol": sym, "asOf": today.isoformat(),
            "withinDays": withinDays, "count": len(out), "expirations": out}


@app.exception_handler(HTTPException)
def http_error(_request, exc: HTTPException):
    """Errors come back as JSON with the same `ok:false` shape the Node backend already uses,
    so one code path handles every failure rather than two."""
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "error": exc.detail})
