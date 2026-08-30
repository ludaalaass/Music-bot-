"""
ShrutiAPI Gateway — Multi-Key Rotator
Proxies to api.shrutibots.site with automatic key rotation.
"""

import asyncio
import json
import os
import time
import uuid
from typing import Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─── Config ───────────────────────────────────────────────────────────────────
UPSTREAM_BASE        = os.getenv("UPSTREAM_BASE", "https://api.shrutibots.site")
REQUESTS_PER_KEY     = int(os.getenv("REQUESTS_PER_KEY", "100"))
RESET_HOURS          = int(os.getenv("RESET_HOURS", "24"))
DATA_FILE            = "keys_data.json"
LOG_MAX              = 500
CACHE_MAX            = 200


# ─── KeyRecord ────────────────────────────────────────────────────────────────
class KeyRecord:
    def __init__(self, key: str, label: str = ""):
        self.id                    = str(uuid.uuid4())[:8]
        self.key                   = key
        self.label                 = label or f"Key-{self.id}"
        self.requests_used         = 0
        self.requests_limit        = REQUESTS_PER_KEY
        self.added_at              = time.time()
        self.reset_at              = time.time() + RESET_HOURS * 3600
        self.is_active             = True
        self.last_used: Optional[float] = None
        self.total_lifetime_requests    = 0
        self.errors                = 0

    def to_dict(self) -> dict:
        remaining = max(0, self.requests_limit - self.requests_used)
        return {
            "id":                       self.id,
            "key":                      self.key,
            "label":                    self.label,
            "requests_used":            self.requests_used,
            "requests_limit":           self.requests_limit,
            "requests_remaining":       remaining,
            "added_at":                 self.added_at,
            "reset_at":                 self.reset_at,
            "is_active":                self.is_active,
            "last_used":                self.last_used,
            "total_lifetime_requests":  self.total_lifetime_requests,
            "errors":                   self.errors,
            "percent_used":             round((self.requests_used / self.requests_limit) * 100, 1),
        }

    def restore(self, d: dict) -> "KeyRecord":
        for field in ("id","key","label","requests_used","requests_limit",
                      "added_at","reset_at","is_active","last_used",
                      "total_lifetime_requests","errors"):
            if field in d:
                setattr(self, field, d[field])
        return self


# ─── GatewayState ─────────────────────────────────────────────────────────────
class GatewayState:
    def __init__(self):
        self.keys: List[KeyRecord]   = []
        self.lock                    = asyncio.Lock()
        self.total_proxied           = 0
        self.total_errors            = 0
        self.started_at              = time.time()
        self.request_log: List[dict] = []
        self._rr_idx                 = 0

    # ── persistence ───────────────────────────────────────────────────────────
    def save(self):
        try:
            with open(DATA_FILE, "w") as f:
                json.dump({
                    "keys":           [k.to_dict() for k in self.keys],
                    "total_proxied":  self.total_proxied,
                    "total_errors":   self.total_errors,
                    "started_at":     self.started_at,
                }, f, indent=2)
        except Exception as e:
            print(f"[save error] {e}")

    def load(self):
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE) as f:
                data = json.load(f)
            self.keys = [KeyRecord(d["key"]).restore(d) for d in data.get("keys", [])]
            self.total_proxied = data.get("total_proxied", 0)
            self.total_errors  = data.get("total_errors", 0)
            self.started_at    = data.get("started_at", time.time())
            print(f"[startup] {len(self.keys)} keys loaded")
        except Exception as e:
            print(f"[load error] {e}")

    # ── auto-reset ────────────────────────────────────────────────────────────
    def _tick_resets(self):
        now = time.time()
        for k in self.keys:
            if k.reset_at <= now and k.requests_used > 0:
                k.requests_used = 0
                k.reset_at      = now + RESET_HOURS * 3600
                k.is_active     = True

    # ── round-robin key pick ──────────────────────────────────────────────────
    def pick_key(self) -> Optional[KeyRecord]:
        self._tick_resets()
        pool = [k for k in self.keys if k.is_active and k.requests_used < k.requests_limit]
        if not pool:
            return None
        idx = self._rr_idx % len(pool)
        self._rr_idx = (idx + 1) % max(len(pool), 1)
        return pool[idx]

    # ── log ───────────────────────────────────────────────────────────────────
    def append_log(self, key_id: str, endpoint: str, status: int, ms: float):
        self.request_log.append({"ts": time.time(), "key_id": key_id,
                                  "endpoint": endpoint, "status": status, "latency_ms": round(ms, 1)})
        if len(self.request_log) > LOG_MAX:
            self.request_log = self.request_log[-LOG_MAX:]

    # ── stats ─────────────────────────────────────────────────────────────────
    def stats(self) -> dict:
        self._tick_resets()
        now     = time.time()
        active  = [k for k in self.keys if k.is_active and k.requests_used < k.requests_limit]
        exhaust = [k for k in self.keys if k.requests_used >= k.requests_limit]
        rpm     = sum(1 for r in self.request_log if now - r["ts"] < 60)
        resets  = sorted(k.reset_at for k in self.keys if k.requests_used > 0)
        return {
            "total_keys":              len(self.keys),
            "active_keys":             len(active),
            "exhausted_keys":          len(exhaust),
            "total_proxied":           self.total_proxied,
            "total_errors":            self.total_errors,
            "total_remaining_requests": sum(max(0, k.requests_limit - k.requests_used) for k in self.keys),
            "requests_last_minute":     rpm,
            "uptime_seconds":          int(now - self.started_at),
            "next_reset_at":           resets[0] if resets else None,
            "current_key_id":          active[0].id if active else None,
        }


state = GatewayState()

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="ShrutiGateway", version="2.0.0", docs_url="/api/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def on_startup():
    state.load()


@app.on_event("shutdown")
async def on_shutdown():
    state.save()


# ─── Static + Root ────────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse("static/index.html")


# ─── Pydantic models ──────────────────────────────────────────────────────────
class AddKeyReq(BaseModel):
    key: str
    label: Optional[str] = ""

class BulkAddReq(BaseModel):
    keys: List[str]
    label_prefix: Optional[str] = "Key"


# ─── Admin: Keys ──────────────────────────────────────────────────────────────
@app.get("/admin/keys")
async def list_keys():
    state._tick_resets()
    return {"keys": [k.to_dict() for k in state.keys]}


@app.post("/admin/keys")
async def add_key(req: AddKeyReq):
    async with state.lock:
        if any(k.key == req.key.strip() for k in state.keys):
            raise HTTPException(400, "Key already exists")
        kr = KeyRecord(req.key.strip(), req.label or "")
        state.keys.append(kr)
        state.save()
    return {"message": "Added", "key": kr.to_dict()}


@app.post("/admin/keys/bulk")
async def bulk_add(req: BulkAddReq):
    added, skipped = [], []
    async with state.lock:
        existing = {k.key for k in state.keys}
        for i, raw in enumerate(req.keys):
            key = raw.strip()
            if not key or key in existing:
                skipped.append(key)
                continue
            kr = KeyRecord(key, f"{req.label_prefix}-{len(state.keys)+1}")
            state.keys.append(kr)
            existing.add(key)
            added.append(kr.to_dict())
        state.save()
    return {"added": len(added), "skipped": len(skipped), "keys": added}


@app.delete("/admin/keys/{key_id}")
async def delete_key(key_id: str):
    async with state.lock:
        before = len(state.keys)
        state.keys = [k for k in state.keys if k.id != key_id]
        if len(state.keys) == before:
            raise HTTPException(404, "Key not found")
        state.save()
    return {"message": "Deleted"}


@app.patch("/admin/keys/{key_id}/toggle")
async def toggle_key(key_id: str):
    async with state.lock:
        for k in state.keys:
            if k.id == key_id:
                k.is_active = not k.is_active
                state.save()
                return {"key": k.to_dict()}
    raise HTTPException(404, "Key not found")


@app.post("/admin/keys/{key_id}/reset")
async def reset_key(key_id: str):
    async with state.lock:
        for k in state.keys:
            if k.id == key_id:
                k.requests_used = 0
                k.reset_at      = time.time() + RESET_HOURS * 3600
                k.is_active     = True
                state.save()
                return {"key": k.to_dict()}
    raise HTTPException(404, "Key not found")


@app.post("/admin/keys/reset-all")
async def reset_all():
    async with state.lock:
        now = time.time()
        for k in state.keys:
            k.requests_used = 0
            k.reset_at      = now + RESET_HOURS * 3600
            k.is_active     = True
        state.save()
    return {"message": f"Reset {len(state.keys)} keys"}


# ─── Admin: Analytics ─────────────────────────────────────────────────────────
@app.get("/admin/stats")
async def get_stats():
    return state.stats()


@app.get("/admin/logs")
async def get_logs(limit: int = 200):
    logs = list(reversed(state.request_log[-limit:]))
    return {"logs": logs, "total": len(state.request_log)}


@app.get("/admin/health")
async def health():
    key = state.pick_key()
    return {"status": "ok" if key else "no_keys", "timestamp": time.time()}


# ─── Proxy ────────────────────────────────────────────────────────────────────
@app.api_route("/proxy/{path:path}", methods=["GET", "POST", "HEAD"])
async def proxy(path: str, request: Request):
    t0 = time.time()

    async with state.lock:
        kr = state.pick_key()
        if not kr:
            raise HTTPException(503, "No API keys available. Add keys in admin panel.")
        params = dict(request.query_params)
        params["api_key"] = kr.key
        kr.requests_used        += 1
        kr.total_lifetime_requests += 1
        kr.last_used             = time.time()
        state.total_proxied     += 1
        kid = kr.id

    url = f"{UPSTREAM_BASE}/{path}"

    try:
        async with aiohttp.ClientSession() as session:
            body    = await request.body() if request.method == "POST" else None
            headers = {k: v for k, v in request.headers.items()
                       if k.lower() not in ("host", "content-length", "transfer-encoding")}

            async with session.request(
                request.method, url,
                params=params, data=body, headers=headers,
                timeout=aiohttp.ClientTimeout(total=180),
            ) as resp:
                ms = (time.time() - t0) * 1000
                state.append_log(kid, f"/{path}", resp.status, ms)

                # upstream rate-limited — force exhaust this key
                if resp.status == 429:
                    async with state.lock:
                        for k in state.keys:
                            if k.id == kid:
                                k.requests_used = k.requests_limit
                                k.errors       += 1
                    state.total_errors += 1
                    state.save()
                    raise HTTPException(503, "Key exhausted (429). Rotated. Retry request.")

                ct = resp.headers.get("Content-Type", "application/octet-stream")

                # stream binary
                if any(x in ct for x in ("audio", "video", "octet-stream")):
                    async def _stream():
                        async for chunk in resp.content.iter_chunked(65536):
                            yield chunk
                    state.save()
                    return StreamingResponse(_stream(), status_code=resp.status, media_type=ct)

                body_bytes = await resp.read()
                state.save()
                return Response(content=body_bytes, status_code=resp.status, media_type=ct)

    except HTTPException:
        raise
    except Exception as e:
        async with state.lock:
            for k in state.keys:
                if k.id == kid:
                    k.errors += 1
        state.total_errors += 1
        state.save()
        raise HTTPException(502, f"Upstream error: {e}")


# ─── Entry ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
