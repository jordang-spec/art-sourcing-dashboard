import os, uuid, re, asyncio
from datetime import datetime
from typing import Optional, List

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker

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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

EXCLUDED_MEDIUMS = [
    "print", "lithograph", "etching", "screenprint", "silkscreen",
    "woodcut", "aquatint", "engraving", "giclée", "giclee", "poster",
    "photograph", "photography", "drawing", "work on paper", "paper",
    "pastel", "pencil", "charcoal", "watercolor", "gouache", "collage",
]

AUCTION_HOUSES = [
    "christie", "sotheby", "phillips", "bonhams", "heritage", "wright",
    "rago", "swann", "doyle", "bukowskis", "ketterer", "artcurial", "aguttes",
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
        ("fields[]", "fldCHDjPpp3beIo3y"),
        ("fields[]", "fldkSytEvHkMWDKJb"),
        ("fields[]", "fldDRxI5mX3NhNzjc"),
        ("filterByFormula", "{Next Launch} = 'Immediate'"),
    ]
    try:
        async with httpx.AsyncClient(timeout=15) as c:
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

# ── Artsy — concurrent GraphQL ────────────────────────────────────────────────
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

async def _artsy_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            resp        = await client.post(ARTSY_GQL, json={"query": ARTSY_QUERY, "variables": {"slug": slug(name)}}, headers={"Content-Type": "application/json"})
            artist_data = (resp.json().get("data") or {}).get("artist") or {}
            edges       = (artist_data.get("filterArtworksConnection") or {}).get("edges", [])
            results     = []
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
                    "Artsy", (n.get("image") or {}).get("url"), is_auc,
                ))
            return results
        except Exception as e:
            print(f"Artsy [{name}]: {e}")
            return []

async def scrape_artsy(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(10)  # 10 concurrent requests
    async with httpx.AsyncClient(timeout=15) as client:
        tasks   = [_artsy_one(client, sem, name) for name in artists]
        batches = await asyncio.gather(*tasks)
    return [item for batch in batches for item in batch]

# ── Artnet — httpx + BeautifulSoup ────────────────────────────────────────────
async def _artnet_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url  = f"https://www.artnet.com/artists/{slug(name)}/works-for-sale/"
            resp = await client.get(url, headers=HEADERS, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")
            results = []
            for card in soup.select("[class*='artwork-card'], [class*='GridItem']")[:15]:
                title = (card.select_one("h2, h3, [class*='title']") or {}).get_text(strip=True) or "Untitled"
                med   = (card.select_one("[class*='medium']") or {}).get_text(strip=True)
                if not valid_medium(med):
                    continue
                price = (card.select_one("[class*='price']") or {}).get_text(strip=True)
                img   = card.select_one("img")
                img_url = img.get("src", "") if img else ""
                a_tag   = card.select_one("a")
                href    = a_tag.get("href", "") if a_tag else ""
                src_url = href if href.startswith("http") else f"https://www.artnet.com{href}"
                results.append(make_row(name, title, "", med, "", price, "Artnet", src_url, "Artnet", img_url, False))
            return results
        except Exception as e:
            print(f"Artnet [{name}]: {e}")
            return []

async def scrape_artnet(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_artnet_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Christie's — JSON search API ──────────────────────────────────────────────
async def _christies_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url    = "https://www.christies.com/api/discoverywebsite/auctioneventandlot/lotfinderputs"
            params = {"keyword": name, "language": "en", "sortby": "relevance", "upcoming": "true", "pagesize": 10}
            resp   = await client.get(url, params=params, headers=HEADERS)
            data   = resp.json()
            results = []
            for lot in (data.get("lots") or [])[:10]:
                title   = lot.get("object_name") or lot.get("title_primary_txt") or "Untitled"
                price   = lot.get("estimate_txt") or lot.get("price_realised_txt") or ""
                img_url = lot.get("image", {}).get("image_url") or ""
                lot_url = f"https://www.christies.com/lot/lot-{lot.get('lotid', '')}"
                results.append(make_row(name, title, "", "", "", price, "Christie's", lot_url, "Christie's", img_url, True))
            return results
        except Exception as e:
            print(f"Christie's [{name}]: {e}")
            return []

async def scrape_christies(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_christies_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Phillips — JSON search API ────────────────────────────────────────────────
async def _phillips_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url    = "https://www.phillips.com/api/search/lots"
            params = {"q": name, "upcoming": "true", "pageSize": 10}
            resp   = await client.get(url, params=params, headers=HEADERS)
            data   = resp.json()
            results = []
            for lot in (data.get("hits") or data.get("results") or [])[:10]:
                title   = lot.get("title") or lot.get("lotTitle") or "Untitled"
                price   = lot.get("estimateDisplay") or lot.get("estimate") or ""
                img_url = lot.get("imageUrl") or lot.get("image") or ""
                lot_url = lot.get("url") or "https://www.phillips.com"
                results.append(make_row(name, title, "", "", "", price, "Phillips", lot_url, "Phillips", img_url, True))
            return results
        except Exception as e:
            print(f"Phillips [{name}]: {e}")
            return []

async def scrape_phillips(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_phillips_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Seesaw — httpx + BeautifulSoup ───────────────────────────────────────────
async def _seesaw_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url  = f"https://www.seesaw.website/works?q={name.replace(' ', '+')}&for_sale=true"
            resp = await client.get(url, headers=HEADERS, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")
            results = []
            for card in soup.select("[class*='WorkCard'], article, [class*='work-card']")[:15]:
                title = (card.select_one("h2, h3, [class*='title']") or {}).get_text(strip=True) or "Untitled"
                med   = (card.select_one("[class*='medium']") or {}).get_text(strip=True)
                if not valid_medium(med):
                    continue
                price   = (card.select_one("[class*='price']") or {}).get_text(strip=True)
                img     = card.select_one("img")
                img_url = img.get("src", "") if img else ""
                a_tag   = card.select_one("a")
                href    = a_tag.get("href", "") if a_tag else ""
                src_url = href if href.startswith("http") else f"https://www.seesaw.website{href}"
                results.append(make_row(name, title, "", med, "", price, "Seesaw", src_url, "Seesaw", img_url, False))
            return results
        except Exception as e:
            print(f"Seesaw [{name}]: {e}")
            return []

async def scrape_seesaw(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_seesaw_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Main scrape job ───────────────────────────────────────────────────────────
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
            _set_status(db, "running", f"Scraping {len(artists)} artists concurrently…")

            # Run all scrapers concurrently
            artsy_r, artnet_r, christies_r, phillips_r, seesaw_r = await asyncio.gather(
                scrape_artsy(artists),
                scrape_artnet(artists),
                scrape_christies(artists),
                scrape_phillips(artists),
                scrape_seesaw(artists),
            )
            all_results = artsy_r + artnet_r + christies_r + phillips_r + seesaw_r

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

# Serves the frontend — must be last
app.mount("/", StaticFiles(directory="static", html=True), name="static")
