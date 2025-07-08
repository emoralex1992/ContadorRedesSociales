"""
social_scraper.py – versión estable 24-jun-2025

• TikTok
      1) HTML requests (data-e2e="followers-count")
      2) Fallback Playwright headless
• Instagram
      1) Endpoint web_profile_info (requiere cookies)
      2) Fallback meta-description (Playwright + cookies)
      → login inicial (visible) solo si no existe ig_state.json
• YouTube — API oficial
• Bucle — cada 5 min descontando la duración real

Instalación:
    pip install pymongo requests playwright==1.44.0
    playwright install chromium
Añade IG_USER, IG_PASS en config.py
"""

import asyncio, json, os, re, time, requests
from datetime import datetime, timezone
from pymongo import MongoClient
from playwright.async_api import async_playwright, TimeoutError
from config import (
    MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER,
    MONGODB_DB_NAME, YOUTUBE_API_KEY,
    IG_USER, IG_PASS
)

# ───────── Utilidades ─────────
def parse_abbrev(txt: str) -> int | None:
    """
    '162.2M' → 162 200 000 ; '134K' → 134 000 ; '12,345' → 12345
    """
    m = re.match(r"([\d.,]+)\s*([KkMmBb]?)", txt.strip())
    if not m:
        return None
    num_str, suf = m.groups()
    num = float(num_str.replace(',', ''))
    mul = {'':1, 'k':1_000, 'm':1_000_000, 'b':1_000_000_000}[suf.lower()]
    return int(num * mul)

# ───────── MongoDB ─────────
URI = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}/?retryWrites=true&w=majority"
COL = MongoClient(URI)[MONGODB_DB_NAME]["social_accounts"]

# ───────── YouTube helpers ─────────
YT_S = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={h}&key={k}"
YT_C = "https://www.googleapis.com/youtube/v3/channels?part=statistics&id={cid}&key={k}"
def yt_channel(handle):
    try:
        js = requests.get(YT_S.format(h=handle, k=YOUTUBE_API_KEY), timeout=10).json()
        return js["items"][0]["snippet"]["channelId"]
    except Exception:
        return None
def yt_subs(cid):
    try:
        js = requests.get(YT_C.format(cid=cid, k=YOUTUBE_API_KEY), timeout=10).json()
        return int(js["items"][0]["statistics"]["subscriberCount"])
    except Exception:
        return None

# ───────── TikTok helpers ─────────
HEAD = {"User-Agent": "Mozilla/5.0"}
def tk_followers_html(user):
    try:
        html = requests.get(f"https://www.tiktok.com/@{user}", headers=HEAD, timeout=10).text
        m = re.search(r'data-e2e="followers-count"[^>]*>([^<]+)<', html)
        return parse_abbrev(m.group(1)) if m else None
    except Exception:
        return None

async def tk_followers_pw(pw, user):
    browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    page = await browser.new_page()
    try:
        await page.goto(f"https://www.tiktok.com/@{user}", timeout=0)
        await page.wait_for_selector('[data-e2e="followers-count"]', timeout=10000)
        txt = await page.locator('[data-e2e="followers-count"]').inner_text()
        return parse_abbrev(txt)
    except Exception:
        return None
    finally:
        await browser.close()

# ───────── Instagram helpers ─────────
STATE = "ig_state.json"

async def ensure_ig_state(pw):
    if os.path.exists(STATE):
        return
    br = await pw.chromium.launch(headless=False)
    ctx = await br.new_context()
    pg = await ctx.new_page()
    await pg.goto("https://www.instagram.com/accounts/login/")
    try:
        await pg.locator("text=Permitir todas las cookies").click(timeout=5000)
    except TimeoutError:
        pass
    await pg.fill("input[name='username']", IG_USER)
    await pg.fill("input[name='password']", IG_PASS)
    await pg.locator("button[type='submit']").click()
    await pg.wait_for_url(re.compile(r"instagram.com/(accounts/onetap/.*|.+)"), timeout=30000)
    for txt in ("Ahora no", "Not now"):
        try:
            await pg.locator(f"text={txt}").click(timeout=4000)
        except TimeoutError:
            pass
    await ctx.storage_state(path=STATE)
    await br.close()

def ig_session():
    if not os.path.exists(STATE):
        return None
    st = json.load(open(STATE))
    s = requests.Session()
    for c in st["cookies"]:
        s.cookies.set(c["name"], c["value"], domain=c.get("domain"))
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def ig_followers_api(username, ses):
    try:
        js = ses.get(
            f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}",
            timeout=10
        ).json()
        return int(js["data"]["user"]["edge_followed_by"]["count"])
    except Exception:
        return None

async def ig_followers_pw(pw, user):
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(storage_state=STATE)
    pg = await ctx.new_page()
    await pg.goto(f"https://www.instagram.com/{user}/", timeout=0)
    await pg.wait_for_timeout(3000)
    html = await pg.content()
    await browser.close()
    meta = re.search(r'content="([\d.,]+)\s*([KkMmBb]?)\s*Followers', html)
    return parse_abbrev(''.join(meta.groups())) if meta else None

# ───────── Bucle principal ─────────
async def main_loop():
    async with async_playwright() as pw:
        await ensure_ig_state(pw)
        ses = ig_session()
        while True:
            t0 = time.time()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⏳  Actualizando…")

            for doc in COL.find({"verified": True}):
                upd = {}
                email = doc.get("email", "sin_email")

                # TikTok
                if tk := doc.get("tiktok_id"):
                    fol = tk_followers_html(tk) or await tk_followers_pw(pw, tk)
                    if fol is not None:
                        upd["tiktok_stats"] = {"followers": fol,
                                               "updated_at": datetime.now(timezone.utc)}

                # Instagram
                if ig := doc.get("instagram_id"):
                    followers = ig_followers_api(ig, ses) if ses else None
                    if followers is None:
                        followers = await ig_followers_pw(pw, ig)
                    if followers is not None:
                        upd["instagram_stats"] = {"followers": followers,
                                                  "updated_at": datetime.now(timezone.utc)}

                # YouTube
                if yh := doc.get("youtube_id"):
                    cid = doc.get("youtube_channel_id") or yt_channel(yh)
                    if cid:
                        upd["youtube_channel_id"] = cid
                        subs = yt_subs(cid)
                        if subs is not None:
                            upd["youtube_stats"] = {"subscribers": subs,
                                                    "updated_at": datetime.now(timezone.utc)}

                if upd:
                    upd["last_updated"] = datetime.now(timezone.utc)
                    COL.update_one({"_id": doc["_id"]}, {"$set": upd})
                    print(f"   ✅ {email} actualizado")

            dur = time.time() - t0
            wait = max(0, 60 - dur)
            m, s = divmod(int(dur), 60)
            print(f"⌛  Duración {m} min {s} s — próxima en {int(wait)} s\n")
            await asyncio.sleep(wait)

if __name__ == "__main__":
    import sys, re
    if not IG_USER or not IG_PASS:
        sys.exit("⚠️  Añade IG_USER e IG_PASS en config.py")
    asyncio.run(main_loop())
