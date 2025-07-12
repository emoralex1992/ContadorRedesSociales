# ── social_scraper.py – 15-jul-2025 (v4.2) ─────────────────────────────
import asyncio, re, time, certifi, requests, pathlib
from datetime import datetime, timezone
from pymongo import MongoClient
from playwright.async_api import async_playwright, TimeoutError, Error as PWError

from config import (
    MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER, MONGODB_DB_NAME,
    YOUTUBE_API_KEY, IG_USER, IG_PASS
)

UA_STR           = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0 Safari/537.36")
IG_PREVIEW_USER  = "engi_academy"
USER_DATA_DIR    = "ig_session"
pathlib.Path(USER_DATA_DIR).mkdir(exist_ok=True)

# ─────────── Mongo ───────────
mongo = MongoClient(
    f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}"
    "/?retryWrites=true&w=majority",
    tls=True, tlsCAFile=certifi.where()
)
COL = mongo[MONGODB_DB_NAME]["social_accounts"]

# ─────────── helpers ───────────
def digits(txt: str) -> int | None:
    txt = txt.replace("\u202f", " ")
    m = re.search(r"([\d\.,]+)", txt)
    return None if not m else int(float(m.group(1).replace(".", "").replace(",", ".")))

async def accept_cookies(page):
    for lbl in ("Aceptar todas", "Allow essential and optional cookies",
                "Permitir todas las cookies", "Accept all"):
        btn = page.locator(f"button:has-text('{lbl}')")
        if await btn.is_visible():
            await btn.click()
            print("   🍪  Cookies aceptadas")
            break

# ─────────── Instagram ───────────
async def ig_do_login(page):
    await accept_cookies(page)
    if not await page.locator('input[name="username"]').is_visible():
        return
    print("   🔑  Rellenando credenciales IG…")
    await page.fill('input[name="username"]', IG_USER)
    await page.fill('input[name="password"]', IG_PASS)
    await page.click('button[type="submit"]')
    try:
        await page.wait_for_selector('nav[role="navigation"]', timeout=20_000)
        print("   ✅  Login completado")
    except TimeoutError:
        print("   ⚠️  Timeout tras enviar credenciales")

async def ig_ensure_logged(page):
    if page.url.startswith("https://www.instagram.com/accounts/login"):
        await ig_do_login(page)

async def ig_followers(page, user: str, retries=3) -> int | None:
    profile_url = f"https://www.instagram.com/{user}/"
    for n in range(1, retries + 1):
        try:
            await page.goto(profile_url, timeout=0)
            await page.wait_for_load_state('domcontentloaded')
        except PWError:
            pass
        await ig_ensure_logged(page)
        await accept_cookies(page)

        if await page.locator("text=Hay un problema").is_visible():
            print(f"⟳  Reintento {n}/{retries} – página con error")
            btn = page.locator("text=Volver a cargar la página")
            if await btn.is_visible():
                await btn.click()
            else:
                await page.reload()
            continue

        span = page.locator('a[href$="followers/"] span[title]').first
        try:
            await span.wait_for(state="visible", timeout=10_000)
            raw = await span.get_attribute("title")
            return digits(raw)
        except TimeoutError:
            print(f"⟳  Reintento {n}/{retries} – followers no visibles")
            await page.reload()
    return None

async def open_visible_profile(play):
    print(f"👀  Abriendo ventana visible con el perfil @{IG_PREVIEW_USER}…")
    ctx = await play.chromium.launch_persistent_context(
        USER_DATA_DIR,
        headless=False,
        viewport={"width": 1280, "height": 900},
        user_agent=UA_STR,
        args=["--lang=en-US,en", "--disable-blink-features=AutomationControlled"],
    )
    pg = await ctx.new_page()
    await ig_ensure_logged(pg)
    ok = await ig_followers(pg, IG_PREVIEW_USER)
    if ok is not None:
        print(f"✅  Perfil visible cargado (followers = {ok})")
    else:
        print("❌  No se pudo dejar el perfil operativo; de todos modos continúa.")
    print("🪟  Deja esta ventana abierta; el loop corre en segundo plano.\n")
    return ctx, pg

# ─────────── TikTok ───────────
def tk_html(user):
    try:
        html = requests.get(f"https://www.tiktok.com/@{user}",
                            headers={"User-Agent": UA_STR, "Cache-Control": "no-cache"},
                            timeout=10).text
        m = re.search(r'data-e2e="followers-count"[^>]*>([^<]+)<', html)
        if m:
            return digits(m.group(1))
        m = re.search(r'"followerCount":\s*(\d+)', html)
        return int(m.group(1)) if m else None
    except Exception:
        return None

async def tk_pw(page, user):
    try:
        await page.goto(f"https://www.tiktok.com/@{user}", timeout=0)
        await page.wait_for_selector('[data-e2e="followers-count"]', timeout=10_000)
        txt = await page.locator('[data-e2e="followers-count"]').inner_text()
        return digits(txt)
    except Exception:
        return None

# ─────────── YouTube ───────────
YT_S = ("https://www.googleapis.com/youtube/v3/search?"
        "part=snippet&type=channel&q={h}&key={k}")
YT_C = ("https://www.googleapis.com/youtube/v3/channels?"
        "part=statistics&id={cid}&key={k}")

def yt_channel(handle):
    try:
        return requests.get(YT_S.format(h=handle, k=YOUTUBE_API_KEY), timeout=10)\
                       .json()["items"][0]["snippet"]["channelId"]
    except Exception:
        return None

def yt_subs(cid):
    try:
        return int(requests.get(YT_C.format(cid=cid, k=YOUTUBE_API_KEY), timeout=10)
                   .json()["items"][0]["statistics"]["subscriberCount"])
    except Exception:
        return None

# ─────────── loop principal ───────────
async def main_loop():
    async with async_playwright() as pw:
        vis_ctx, _ = await open_visible_profile(pw)
        ig_page = await vis_ctx.new_page()

        tk_br  = await pw.chromium.launch(headless=True)
        tk_ctx = await tk_br.new_context(user_agent=UA_STR)
        tk_pg  = await tk_ctx.new_page()

        while True:
            loop_start = time.time()
            print(f"[{datetime.now():%H:%M:%S}] ⏳  Actualizando…")

            for doc in COL.find({"verified": True}):
                t_acc = time.time()
                upd, log = {}, []
                email = doc.get("email", "sin_email")

                # TikTok
                if tk := doc.get("tiktok_id"):
                    f = tk_html(tk) or await tk_pw(tk_pg, tk)
                    upd["tiktok_stats"] = {"followers": f,
                                           "updated_at": datetime.now(timezone.utc)}
                    log.append(f"• TikTok  @{tk:<20} {f}")

                # Instagram
                if ig := doc.get("instagram_id"):
                    f = await ig_followers(ig_page, ig)
                    upd["instagram_stats"] = {"followers": f,
                                              "updated_at": datetime.now(timezone.utc)}
                    log.append(f"• Instagram @{ig:<20} {f}")

                # YouTube
                if yh := doc.get("youtube_id"):
                    cid  = doc.get("youtube_channel_id") or yt_channel(yh)
                    subs = yt_subs(cid) if cid else None
                    if subs is not None:
                        upd["youtube_channel_id"] = cid
                        upd["youtube_stats"] = {"subscribers": subs,
                                                "updated_at": datetime.now(timezone.utc)}
                    log.append(f"• YouTube  {yh:<22} {subs}")

                upd["last_updated"] = datetime.now(timezone.utc)
                COL.update_one({"_id": doc["_id"]}, {"$set": upd})

                print(f"   ✅ {email} actualizado "
                      f"⏱ {time.time() - t_acc:.2f}s")
                for l in log:
                    print("      " + l)
                print()

            cycle = time.time() - loop_start
            wait  = max(0, 60 - cycle)
            print(f"⏳  Siguiente pase en {wait:.1f}s "
                  f"(ciclo: {cycle:.2f}s)\n")
            await asyncio.sleep(wait)

if __name__ == "__main__":
    asyncio.run(main_loop())
