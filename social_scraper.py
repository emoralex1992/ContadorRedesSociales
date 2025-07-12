# social_scraper.py – 15‑jul‑2025 (robusto 24×7)
"""
Scraper de seguidores para TikTok, Instagram y YouTube basado en Playwright.

Mejoras clave
-------------
*   **Sesión persistente de Instagram** → carpeta `ig_userdata` para no volver a
    logarse salvo que la sesión caduque.
*   **Reintentos exponenciales + jitter** en cualquier llamada con red/UI.
*   **Paralelismo** configurable (`CONCURRENCY`) mediante semáforo.
*   **Medición de tiempos** de cada cuenta y del ciclo completo.
*   **Logs rotativos** en `./logs/scraper.log` y consola.
*   **Compatibilidad Windows/Linux**: la captura de señales sólo se registra
    cuando la plataforma lo soporta.

Requisitos
~~~~~~~~~~
```bash
python -m venv .venv && .venv\Scripts\activate      # Windows
pip install -r requirements.txt
playwright install chromium
```
Para congelar las dependencias instaladas:
```bash
pip freeze > requirements.txt
```
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import logging.handlers
import os
import random
import re
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, List

import certifi
import requests
from pymongo import MongoClient
from playwright.async_api import (
    BrowserContext,
    Page,
    TimeoutError,  # noqa: N811
    async_playwright,
)

# ──────────────────────────  Config  ──────────────────────────
try:
    import importlib

    _cfg = importlib.import_module("config")
except ModuleNotFoundError as exc:
    raise SystemExit(f"⚠️  No se pudo importar config.py: {exc}") from exc

#  Credenciales obligatorias que ya existen en tu config.py
MONGODB_USER: str = _cfg.MONGODB_USER
MONGODB_PASSWORD: str = _cfg.MONGODB_PASSWORD
MONGODB_CLUSTER: str = _cfg.MONGODB_CLUSTER
MONGODB_DB_NAME: str = _cfg.MONGODB_DB_NAME
IG_USER: str = _cfg.IG_USER
IG_PASS: str = _cfg.IG_PASS
YOUTUBE_API_KEY: str = _cfg.YOUTUBE_API_KEY

#  Parámetros opcionales con valores por defecto
HEADLESS: bool = bool(getattr(_cfg, "HEADLESS", True))
CONCURRENCY: int = int(getattr(_cfg, "CONCURRENCY", 4))
RETRIES: int = int(getattr(_cfg, "RETRIES", 3))
LOOP_EVERY: int = int(getattr(_cfg, "LOOP_EVERY", 60))  # segundos entre ciclos

MONGODB_URI: str = getattr(
    _cfg,
    "MONGODB_URI",
    f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}/?retryWrites=true&w=majority",
)

# ─────────────────────────  Logging  ─────────────────────────
LOG_DIR = Path.cwd() / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            LOG_DIR / "scraper.log", maxBytes=1 << 20, backupCount=5, encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("scraper")

# ─────────────────────────  Helpers  ─────────────────────────
UA_STR = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA_STR, "Cache-Control": "no-cache"}

_DIGIT_RE = re.compile(r"([\d.,]+)\s*([kmb]?)", re.I)
_SUFFIX: dict[str, Decimal] = {
    "": Decimal(1),
    "k": Decimal(1_000),
    "m": Decimal(1_000_000),
    "b": Decimal(1_000_000_000),
}

def digits(txt: str) -> int | None:
    """Convierte «1.2M», «30 seguidores» → int."""
    txt = txt.replace("\u202f", " ").lower()
    m = _DIGIT_RE.search(txt)
    if not m:
        return None
    num_text = m.group(1).replace(".", "").replace(",", ".")
    num = Decimal(num_text)
    factor = _SUFFIX[m.group(2)]
    return int(num * factor)


def retry_async(times: int = 3, base: float = 1.6):
    """Decorador de reintentos exponenciales + jitter."""

    def decorator(fn: Callable[..., Awaitable[Any]]):
        async def wrapper(*args, **kwargs):  # type: ignore[override]
            for attempt in range(1, times + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    if attempt == times:
                        logger.error("❌ %s agotó %s intentos – %s", fn.__name__, times, exc)
                        return None
                    wait = base ** attempt + random.uniform(0, 1)
                    logger.warning(
                        "⟳ Reintento %s/%s %s en %.1fs (%s)", attempt, times, fn.__name__, wait, exc
                    )
                    await asyncio.sleep(wait)

        return wrapper

    return decorator


# ──────────────────────────  Mongo  ─────────────────────────
client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
COL = client[MONGODB_DB_NAME]["social_accounts"]

# ───────────────────────── TikTok ──────────────────────────
@retry_async(times=RETRIES)
async def _tk_html(user: str) -> int | None:
    html = requests.get(f"https://www.tiktok.com/@{user}", headers=HEADERS, timeout=10).text
    m = re.search(r'data-e2e="followers-count"[^>]*>([^<]+)<', html)
    if m:
        return digits(m.group(1))
    m = re.search(r'"followerCount":\s*(\d+)', html)
    return int(m.group(1)) if m else None


@retry_async(times=RETRIES)
async def _tk_api(user: str) -> int | None:
    url = f"https://www.tiktok.com/api/user/detail/?uniqueId={user}"
    j = requests.get(url, headers=HEADERS, timeout=10).json()
    return j.get("userInfo", {}).get("stats", {}).get("followerCount")


async def tiktok_followers(user: str) -> int | None:
    return await _tk_html(user) or await _tk_api(user)


# ───────────────────────── YouTube ─────────────────────────
YT_S = (
    "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={h}&key="
    + YOUTUBE_API_KEY
)
YT_C = (
    "https://www.googleapis.com/youtube/v3/channels?part=statistics&id={cid}&key="
    + YOUTUBE_API_KEY
)


@retry_async(times=RETRIES)
async def yt_channel_id(handle: str) -> str | None:
    j = requests.get(YT_S.format(h=handle), timeout=10).json()
    items = j.get("items")
    return items[0]["snippet"]["channelId"] if items else None


@retry_async(times=RETRIES)
async def yt_subscribers(cid: str) -> int | None:
    j = requests.get(YT_C.format(cid=cid), timeout=10).json()
    items = j.get("items")
    return int(items[0]["statistics"]["subscriberCount"]) if items else None


# ───────────────────── Instagram (Playwright) ───────────────────
USER_DATA = Path.cwd() / "ig_userdata"
FOLLOWERS_LOC = "a[href$='followers/'] span[title]"


async def _accept_cookies(page: Page) -> None:
    with contextlib.suppress(TimeoutError):
        await page.locator("text=/^(Aceptar todas|Accept all)/i").click(timeout=5_000)
        logger.debug("🍪 Cookies aceptadas")


async def _login_if_needed(page: Page) -> None:
    if await page.is_visible("input[name='username']"):
        logger.info("🔑 Rellenando credenciales IG…")
        await page.fill("input[name='username']", IG_USER)
        await page.fill("input[name='password']", IG_PASS)
        await page.press("input[name='password']", "Enter")
        with contextlib.suppress(TimeoutError):
            await page.wait_for_selector("text=/Guardar información|Save info/i", timeout=15_000)
            await page.click("text=/Ahora no|Not now/i", timeout=5_000)


def _ig_retry(fn: Callable[..., Awaitable[Any]]):
    """Retry específico para acciones del browser con cierre de página"""

    async def wrapper(page: Page, *args, **kwargs):  # type: ignore[override]
        for attempt in range(1, RETRIES + 1):
            try:
                return await fn(page, *args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                if attempt == RETRIES:
                    logger.error("❌ IG agotó %s intentos – %s", RETRIES, exc)
                    return None
                wait = 1.4 ** attempt + random.uniform(0, 1)
                logger.warning("⟳ IG retry %s/%s en %.1fs (%s)", attempt, RETRIES, wait, exc)
                await asyncio.sleep(wait)

    return wrapper


@_ig_retry
async def _ensure_profile(page: Page, user: str) -> None:
    await page.goto(f"https://www.instagram.com/{user}/", timeout=0)
    await _accept_cookies(page)
    await _login_if_needed(page)
    await page.wait_for_selector(FOLLOWERS_LOC, timeout=15_000)


@_ig_retry
async def instagram_followers(page: Page, user: str) -> int | None:
    await _ensure_profile(page, user)
    txt = await page.locator(FOLLOWERS_LOC).inner_text()
    return digits(txt)


# ─────────────────────────── Main Loop ───────────────────────────
async def gather_followers(ctx: BrowserContext, doc: dict) -> None:
    """Procesa una sola cuenta de Mongo y actualiza sus stats."""
    start = time.perf_counter()
    upd: dict[str, Any] = {}
    log_parts: List[str] = []

    email = doc.get("email", "sin_email")

    # TikTok
    if tk := doc.get("tiktok_id"):
        tk_followers = await tiktok_followers(tk)
        upd["tiktok_stats"] = {
            "followers": tk_followers,
            "updated_at": datetime.now(timezone.utc),
        }
        log_parts.append(f"TikTok:@{tk} → {tk_followers}")

    # Instagram (necesita página)
    if ig := doc.get("instagram_id"):
        page = await ctx.new_page()
        ig_followers = await instagram_followers(page, ig)
        await page.close()
        upd["instagram_stats"] = {
            "followers": ig_followers,
            "updated_at": datetime.now(timezone.utc),
        }
        log_parts.append(f"Instagram:@{ig} → {ig_followers}")

    # YouTube
    if yh := doc.get("youtube_id"):
        cid = doc.get("youtube_channel_id") or await yt_channel_id(yh)
        subs = await yt_subscribers(cid) if cid else None
        if subs is not None:
            upd["youtube_channel_id"] = cid
            upd["youtube_stats"] = {
                "subscribers": subs,
                "updated_at": datetime.now(timezone.utc),
            }
        log_parts.append(f"YouTube:@{yh} → {subs}")

    upd["last_updated"] = datetime.now(timezone.utc)
    COL.update_one({"_id": doc["_id"]}, {"$set": upd})

    elapsed = time.perf_counter() - start
    logger.info("✅ %s actualizado en %.2fs | %s", email, elapsed, " | ".join(log_parts))


async def main() -> None:
    async with async_playwright() as pw:
        ig_ctx = await pw.chromium.launch_persistent_context(
            USER_DATA,
            headless=HEADLESS,
            locale="en-US",
            user_agent=UA_STR,
            args=["--lang=en-US,en"],
        )

        # Registro de señales (solo en plataformas que lo soporten)
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda sig=sig: asyncio.create_task(ig_ctx.close()))
        except NotImplementedError:
            pass

        sem = asyncio.Semaphore(CONCURRENCY)

        while True:
            cycle_start = time.perf_counter()
            tasks = []

            async def worker(document: dict) -> None:
                async with sem:
                    await gather_followers(ig_ctx, document)

            for document in COL.find({"verified": True}):
                tasks.append(asyncio.create_task(worker(document)))

            await asyncio.gather(*tasks)
            cycle_elapsed = time.perf_counter() - cycle_start
            logger.info(
                "🔄 Ciclo completo en %.2fs – próxima pasada en %ss", cycle_elapsed, LOOP_EVERY
            )
            await asyncio.sleep(max(0, LOOP_EVERY - cycle_elapsed))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Interrumpido por el usuario")
