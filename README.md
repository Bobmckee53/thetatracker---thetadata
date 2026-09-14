# ThetaData bridge service

A small Python service that speaks ThetaData on one side and plain JSON on the other, so the
Node backend can use a feed that only ships a Python library.

**No Theta Terminal, no Java.** ThetaData's Python library connects straight to a hosted
endpoint (`mdds-*.thetadata.us:443` over TLS), so there is nothing to install, nothing to keep
running, and nothing to warm up after a redeploy. This is the reason the deployment stopped
being a risk.

## Deploying it on Railway

It goes in its **own Railway service**, never the same container as the Node backend. The
backend redeploys on every push; a data bridge in that container would restart every time.

1. Create a new GitHub repo — `thetatracker-thetadata` or similar — and put these four files
   in it: `main.py`, `requirements.txt`, `Procfile`, `.gitignore`.
2. In the existing Railway **project**, add a **New Service → GitHub Repo** and point it at
   that repo. Same project means both services share Railway's private network.
3. Set two variables on the new service:

   | Variable | Value |
   |---|---|
   | `THETADATA_API_KEY` | the key from the ThetaData user portal |
   | `SERVICE_SHARED_SECRET` | any long random string — generate with `openssl rand -hex 32` |

4. Set `SERVICE_SHARED_SECRET` to the **same value** on the Node backend service, so it can
   authenticate to this one.
5. Railway detects Python from `requirements.txt` and reads the start command from `Procfile`.

## Checking it works

Railway gives the service a private hostname inside the project. From the Node backend:

```
GET http://<service>.railway.internal:$PORT/health
X-TTP-Key: <SERVICE_SHARED_SECRET>
```

A healthy answer reports the three entitlements. `indices` must be non-empty — that is the
CGIF answer, and it comes from ThetaData's auth response rather than a data call.

## Endpoints

All require the `X-TTP-Key` header. All errors return `{"ok": false, "error": "..."}` with a
status that says **which end is broken**: `503` means this service is misconfigured, `502`
means ThetaData failed, `401` means the shared secret is wrong.

| Endpoint | Purpose |
|---|---|
| `GET /health` | reachability and entitlements |
| `GET /spot?symbols=SPX,XSP` | live index spot, with the feed's own timestamp |
| `GET /greeks?symbol=&expiration=&strike=&right=` | one contract |
| `GET /chain?symbol=&expiration=&quotes=true` | a whole expiration, every strike, both rights |

## Things learned the hard way, now encoded here

**`dataTimestamp` is not `calledAt`.** The feed reports when the market last printed, not when
you asked. Measured 51 minutes apart after the close. Any comparison against a broker screen
must use `dataTimestamp`, or it compares two different moments — which manufactures a
discrepancy out of a working feed, or hides a real one.

**SPX has two roots.** Monthlies (third Friday) are under `SPX`; everything else is under
`SPXW`. Verified live: `SPX 2026-10-09` returns nothing while `SPX 2026-09-18` and
`SPX 2026-11-20` return full chains. Every call here tries the plain root then the weekly one,
and reports which answered as `rootUsed`.

**Strikes arrive plainly or in thousandths**, undocumented either way. `chain_scale()` infers
which from the data against a known spot rather than assuming.

**A column named `bid_size` is not the bid.** Matching column names by substring alone reported
sizes as prices — a bid of 20.00 against an ask of 10.00, which is impossible and is what gave
it away. `col()` demands an exact name first and refuses anything carrying size, exchange or
condition in its name.

**Strike increments widen further out in time.** XSP Nov 20 lists ~232 strikes against Sep 18's
~370, so 766 and 804 simply do not exist there. `/greeks` snaps to the nearest listed strike
and tells you it did, via `strikeUsed` and `strikeSnapped`. Comparing against an unsnapped
strike means comparing a different contract.

**IV is a fraction, not a percent.** ThetaData returns `0.1448`; brokers display `14.48%`. Both
forms are returned so nothing has to guess.

**`thetadata` 1.0.10 imports `dotenv` without declaring it.** A clean `pip install thetadata`
succeeds and then fails on import. `requirements.txt` pins `python-dotenv` explicitly.

**Endpoints are `def`, not `async def`.** The ThetaData library is synchronous, so FastAPI runs
these in a worker thread. An `async def` wrapping blocking gRPC would stall the event loop for
every other request.
