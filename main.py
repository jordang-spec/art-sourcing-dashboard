import os, uuid, re, asyncio
from datetime import datetime
from typing import Optional, List

import httpx
from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from playwright.async_api import async_playwright

# ── Database ──────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./artworks.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Artwork(Base):
    __tablename__ = "artworks"
    id          = Column(String, primary_key=True)
    artist_name = Column(String, index=True)
    title       = Column(String)
    year        = Column(String)
    medium      = Column(String)
    dimensions  = Column(String)
    price       = Column(String)
    seller_name = Column(String)
    source_url  = Column(String)
    source_name = Column(String)
    image_url   = Column(String)
    is_auction  = Column(Boolean, default=False)
    sale_type   = Column(String)
    scraped_at  = Column(DateTime, default=datetime.utcnow)
    source_id   = Column(String)


class ScrapeStatus(Base):
    __tablename__ = "scrape_status"
    id       = Column(String, primary_key=True, default=lambda: "singleton")
    status   = Column(String, default="idle")
    last_run = Column(DateTime)
    message  = Column(Text)


Base.metadata.create_all(bind=engine)

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID  = "app8GJgnPtP23cObV"
AIRTABLE_TABLE_ID = "tblClPSzECL3rpbd0"

EXCLUDED_MEDIUMS = [
    "print", "lithograph", "etching", "screenprint", "silkscreen",
    "woodcut", "aquatint", "engraving", "giclée", "giclee", "poster",
    "photograph", "photography", "drawing", "work on paper", "paper",
    "pastel", "pencil", "charcoal", "watercolor", "gouache", "collage",
]

AUCTION_HOUSES = [
    "christie", "sotheby", "phillips", "bonhams", "heritage", "wright",
    "rago", "swann", "doyle", "bukowskis", "ketterer", "artcurial",
    "aguttes", "seoul auction",
]

FALLBACK_ARTISTS = [
    "Banksy", "Yayoi Kusama", "Gerhard Richter", "Andy Warhol", "Keith Haring",
    "Joan Mitchell", "Willem de Kooning", "Christopher Wool", "Agnes Martin",
    "Josef Albers", "Lucio Fontana", "Alexander Calder", "Zao Wou-Ki",
    "Cecily Brown", "George Condo", "Barkley L. Hendricks", "Helen Frankenthaler",
    "Henry Taylor", "Bridget Riley", "Amy Sillman", "Lynette Yiadom-Boakye",
    "Barbara Kruger", "Ruth Asawa", "Carmen Herrera", "Kazuo Shiraga",
    "Alighiero Boetti", "Sam Gilliam", "Tracey Emin", "Jacqueline Humphries",
    "Ernie Barnes", "Lynne Mapp Drexler", "Simone Leigh", "Grace Hartigan",
    "Christine Ay Tjoe", "Edward Ruscha",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def valid_medium(medium: str) -> bool:
    if not medium:
        return True
    m = medium.lower()
    return not any(ex in m for ex in EXCLUDED_MEDIUMS)


def is_auction_house(seller: str) -> bool:
    if not seller:
        return False
    return any(h in seller.lower() for h in AUCTION_HOUSES)


def slug(name: str) -> str:
    s = re.sub(r"[^\w\s-]", "", name.lower().strip())
    return re.sub(r"\s+", "-", s)


def make_row(artist, title, year, medium, dims, price, seller, src_url, src_name, img, is_auc):
    return {
        "artist_name": artist,
        "title":       (title or "Untitled").strip(),
        "year":        year or "",
        "medium":      (medium or "").strip(),
        "dimensions":  dims or "",
        "price":       (price or "").strip(),
        "seller_name": seller or "",
        "source_url":  src_url or "",
        "source_name": src_name,
        "image_url":   img or "",
        "is_auction":  is_auc,
        "sale_type":   "Auction" if is_auc else "Private",
        "source_id":   f"{src_name}_{artist}_{(title or '')[:40]}".replace(" ", "_"),
    }

# ── Airtable ──────────────────────────────────────────────────────────────────
async def get_target_artists() -> List[str]:
    if not AIRTABLE_TOKEN:
        return FALLBACK_ARTISTS
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    params  = [
        ("fields[]", "fldCHDjPpp3beIo3y"),   # Artist Full Name Proper
        ("fields[]", "fldkSytEvHkMWDKJb"),    # Next Launch
        ("fields[]", "fldDRxI5mX3NhNzjc"),    # Artist Market Buy Rating
        ("filterByFormula", "{Next Launch} = 'Immediate'"),
    ]
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            records = (await c.get(url, headers=headers, params=params)).json().get("records", [])
        artists = []
        for rec in records:
            f = rec.get("fields", {})
            if "Buy" not in str(f.get("fldDRxI5mX3NhNzjc", "")):
                continue
            name = f.get("fldCHDjPpp3beIo3y", "")
            if isinstance(name, list):
                name = name[0] if name else ""
            if str(name).strip():
                artists.append(str(name).strip())
        return artists or FALLBACK_ARTISTS
    except Exception as e:
        print(f"Airtable error: {e}")
        return FALLBACK_ARTISTS

# ── Artsy API ─────────────────────────────────────────────────────────────────
ARTSY_GQL   = "https://metaphysics.artsy.net/v2"
ARTSY_QUERY = """
query($slug: String!) {
  artist(id: $slug) {
    filterArtworksConnection(forSale: true, first: 30) {
      edges {
        node {
          internalID title date medium
          dimensions { in cm }
          saleMessage
          partner { name }
          image { url(version: "large") }
          href
          sale { isAuction }
        }
      }
    }
  }
}
"""

async def scrape_artsy(artists: List[str]) -> List[dict]:
    results = []
    async with httpx.AsyncClient(timeout=30) as c:
        for name in artists:
            try:
                resp        = await c.post(ARTSY_GQL, json={"query": ARTSY_QUERY, "variables": {"slug": slug(name)}}, headers={"Content-Type": "application/json"})
                artist_data = (resp.json().get("data") or {}).get("artist") or {}
                edges       = (artist_data.get("filterArtworksConnection") or {}).get("edges", [])
                for edge in edges:
                    n      = edge.get("node", {})
                    med    = n.get("medium") or ""
                    if not valid_medium(med):
                        continue
                    seller = ((n.get("partner") or {}).get("name") or "")
                    is_auc = bool(((n.get("sale") or {}).get("isAuction"))) or is_auction_house(seller)
                    dims   = n.get("dimensions") or {}
                    results.append(make_row(
                        name, n.get("title"), n.get("date"), med,
                        dims.get("in") or dims.get("cm"),
                        n.get("saleMessage"), seller,
                        "https://artsy.net" + (n.get("href") or ""),
                        "Artsy",
                        (n.get("image") or {}).get("url"),
                        is_auc,
                    ))
                await asyncio.sleep(0.3)
            except Exception as e:
                print(f"Artsy [{name}]: {e}")
    return results

# ── Playwright scrapers ───────────────────────────────────────────────────────
async def scrape_with_playwright(artists: List[str]) -> List[dict]:
    results = []
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
            page    = await (await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )).new_page()
            for name in artists:
                for fn, delay in [(_artnet, 1.5), (_seesaw, 1.5), (_christies, 2.0), (_sothebys, 2.0), (_phillips, 1.5)]:
                    results += await fn(name, page)
                    await asyncio.sleep(delay)
            await browser.close()
    except Exception as e:
        print(f"Playwright error: {e}")
    return results


async def _cards(page, url, sel, limit=15):
    try:
        await page.goto(url, wait_until="networkidle", timeout=20000)
        await page.wait_for_timeout(1500)
        return (await page.query_selector_all(sel))[:limit]
    except Exception:
        return []


async def _text(el, sel):
    try:
        c = await el.query_selector(sel)
        return (await c.inner_text()).strip() if c else ""
    except Exception:
        return ""


async def _attr(el, sel, attr):
    try:
        c = await el.query_selector(sel)
        return (await c.get_attribute(attr) or "").strip() if c else ""
    except Exception:
        return ""


async def _artnet(name: str, page) -> List[dict]:
    results = []
    for card in await _cards(page, f"https://www.artnet.com/artists/{slug(name)}/works-for-sale/", "[class*='artwork-card'],[class*='GridItem'],[class*='ArtworkCard']"):
        t = await _text(card, "h2,h3,[class*='title']") or "Untitled"
        m = await _text(card, "[class*='medium']")
        if not valid_medium(m): continue
        p = await _text(card, "[class*='price']")
        img = await _attr(card, "img", "src")
        href = await _attr(card, "a", "href")
        results.append(make_row(name, t, "", m, "", p, "Artnet", href if href.startswith("http") else f"https://www.artnet.com{href}", "Artnet", img, False))
    return results


async def _seesaw(name: str, page) -> List[dict]:
    results = []
    for card in await _cards(page, f"https://www.seesaw.website/works?q={name.replace(' ','+')}&for_sale=true", "[class*='WorkCard'],[class*='work-card'],article"):
        t = await _text(card, "h2,h3,[class*='title']") or "Untitled"
        m = await _text(card, "[class*='medium']")
        if not valid_medium(m): continue
        p = await _text(card, "[class*='price']")
        img = await _attr(card, "img", "src")
        href = await _attr(card, "a", "href")
        results.append(make_row(name, t, "", m, "", p, "Seesaw", href if href.startswith("http") else f"https://www.seesaw.website{href}", "Seesaw", img, False))
    return results


async def _christies(name: str, page) -> List[dict]:
    results = []
    for card in await _cards(page, f"https://www.christies.com/en/search?entry={name.replace(' ','+')}&tab=upcoming_lots", ".chr-lot-tile,[class*='lot-tile']", 10):
        t = await _text(card, "[class*='title'],h3") or "Untitled"
        p = await _text(card, "[class*='estimate']")
        img = await _attr(card, "img", "src")
        href = await _attr(card, "a", "href")
        results.append(make_row(name, t, "", "", "", p, "Christie's", href if href.startswith("http") else f"https://www.christies.com{href}", "Christie's", img, True))
    return results


async def _sothebys(name: str, page) -> List[dict]:
    results = []
    for card in await _cards(page, f"https://www.sothebys.com/en/search#/?q={name.replace(' ','%20')}&tab=lots", "[data-testid='search-result-item'],[class*='SearchResult']", 10):
        t = await _text(card, "[class*='title'],h3,h2") or "Untitled"
        p = await _text(card, "[class*='estimate'],[class*='price']")
        img = await _attr(card, "img", "src")
        href = await _attr(card, "a", "href")
        results.append(make_row(name, t, "", "", "", p, "Sotheby's", href if href.startswith("http") else f"https://www.sothebys.com{href}", "Sotheby's", img, True))
    return results


async def _phillips(name: str, page) -> List[dict]:
    results = []
    for card in await _cards(page, f"https://www.phillips.com/search#/?search_query={name.replace(' ','+')}&tab=lots", "[class*='lot-'],[class*='Lot']", 10):
        t = await _text(card, "[class*='title'],h3") or "Untitled"
        p = await _text(card, "[class*='estimate']")
        img = await _attr(card, "img", "src")
        href = await _attr(card, "a", "href")
        results.append(make_row(name, t, "", "", "", p, "Phillips", href if href.startswith("http") else f"https://www.phillips.com{href}", "Phillips", img, True))
    return results

# ── Scrape job ────────────────────────────────────────────────────────────────
_lock = asyncio.Lock()


def _set_status(db, status, message):
    s = db.query(ScrapeStatus).first()
    if not s:
        s = ScrapeStatus(id="singleton")
        db.add(s)
    s.status, s.last_run, s.message = status, datetime.utcnow(), message
    db.commit()


async def run_scrape():
    if _lock.locked():
        return
    async with _lock:
        db = SessionLocal()
        try:
            _set_status(db, "running", "Fetching artists from Airtable…")
            artists = await get_target_artists()
            _set_status(db, "running", f"Scraping {len(artists)} artists across all sources…")

            all_results = await scrape_artsy(artists) + await scrape_with_playwright(artists) # type: ignore

            db.query(Artwork).delete()
            seen, saved = set(), 0
            for item in all_results:
                if item["source_id"] in seen:
                    continue
                seen.add(item["source_id"])
                db.add(Artwork(id=str(uuid.uuid4()), **item))
                saved += 1
            db.commit()
            _set_status(db, "done", f"Found {saved} works across {len(artists)} artists")
        except Exception as e:
            _set_status(db, "error", str(e))
        finally:
            db.close()

# ── API ───────────────────────────────────────────────────────────────────────
app = FastAPI()


def serialize(r: Artwork) -> dict:
    return {
        "id": r.id, "artist_name": r.artist_name, "title": r.title,
        "year": r.year, "medium": r.medium, "dimensions": r.dimensions,
        "price": r.price, "seller_name": r.seller_name, "source_url": r.source_url,
        "source_name": r.source_name, "image_url": r.image_url,
        "is_auction": r.is_auction, "sale_type": r.sale_type,
        "scraped_at": r.scraped_at.isoformat() if r.scraped_at else None,
    }


@app.get("/api/artworks")
def get_artworks(artist: Optional[str] = None, sale_type: Optional[str] = None, source: Optional[str] = None):
    db = SessionLocal()
    try:
        q = db.query(Artwork)
        if artist:    q = q.filter(Artwork.artist_name.ilike(f"%{artist}%"))
        if sale_type and sale_type != "All": q = q.filter(Artwork.sale_type == sale_type)
        if source:    q = q.filter(Artwork.source_name == source)
        return [serialize(r) for r in q.order_by(Artwork.scraped_at.desc()).all()]
    finally:
        db.close()


@app.post("/api/scrape")
async def trigger_scrape(bg: BackgroundTasks):
    if _lock.locked():
        return {"status": "already_running"}
    bg.add_task(run_scrape)
    return {"status": "started"}


@app.get("/api/scrape/status")
def scrape_status():
    db = SessionLocal()
    try:
        s = db.query(ScrapeStatus).first()
        if not s:
            return {"status": "idle", "last_run": None, "message": "Click Refresh Data to start"}
        return {"status": s.status, "last_run": s.last_run.isoformat() if s.last_run else None, "message": s.message}
    finally:
        db.close()


# Must be last — serves the frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")
