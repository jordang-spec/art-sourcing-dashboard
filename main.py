import os, uuid, re, asyncio, json
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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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

# ── Artsy — web scraper (no API key needed) ────────────────────────────────────
def _artsy_find_artworks(obj, depth=0) -> List[dict]:
    """Recursively find Artwork relay nodes in Artsy's __NEXT_DATA__."""
    if depth > 25 or not obj:
        return []
    results = []
    if isinstance(obj, dict):
        if obj.get("__typename") == "Artwork":
            results.append(obj)
        else:
            for v in obj.values():
                results.extend(_artsy_find_artworks(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_artsy_find_artworks(item, depth + 1))
    return results

def _artsy_img(aw: dict) -> str:
    img = aw.get("image") or aw.get("imageUrl") or {}
    if isinstance(img, dict):
        for sub_key in ("resized", "cropped", "scaled"):
            sub = img.get(sub_key)
            if isinstance(sub, dict):
                return sub.get("src") or sub.get("url") or ""
        for k in ("src", "url", "large", "medium", "small"):
            if img.get(k) and isinstance(img[k], str):
                return img[k]
    if isinstance(img, str) and img.startswith("http"):
        return img
    images = aw.get("images") or []
    if images and isinstance(images, list) and isinstance(images[0], dict):
        return images[0].get("url") or images[0].get("src") or ""
    return ""

def _artsy_price(aw: dict) -> str:
    for key in ("saleMessage", "listPrice", "price"):
        val = aw.get(key)
        if not val:
            continue
        if isinstance(val, dict):
            return val.get("display") or ""
        if isinstance(val, str) and val.strip():
            return val
    return ""

async def _artsy_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url  = f"https://www.artsy.net/artist/{slug(name)}/works-for-sale"
            resp = await client.get(url, headers=HEADERS, follow_redirects=True)
            if resp.status_code != 200:
                print(f"Artsy [{name}]: HTTP {resp.status_code}")
                return []

            soup   = BeautifulSoup(resp.text, "html.parser")
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if not nd_tag or not nd_tag.string:
                print(f"Artsy [{name}]: no __NEXT_DATA__")
                return []

            nd_data  = json.loads(nd_tag.string)
            aw_nodes = _artsy_find_artworks(nd_data)

            seen, results = set(), []
            for aw in aw_nodes:
                key = aw.get("internalID") or aw.get("slug") or aw.get("title")
                if not key or key in seen:
                    continue
                seen.add(key)
                med = aw.get("medium") or ""
                if not valid_medium(med):
                    continue
                partner  = aw.get("partner") or {}
                seller   = partner.get("name", "") if isinstance(partner, dict) else ""
                is_auc   = is_auction_house(seller)
                dims_obj = aw.get("dimensions") or {}
                dims     = ""
                if isinstance(dims_obj, dict):
                    in_o = dims_obj.get("in") or {}
                    cm_o = dims_obj.get("cm") or {}
                    dims = (in_o.get("text") if isinstance(in_o, dict) else "") or \
                           (cm_o.get("text") if isinstance(cm_o, dict) else "") or ""
                art_slug = aw.get("slug") or ""
                art_url  = f"https://www.artsy.net/artwork/{art_slug}" if art_slug else url
                results.append(make_row(
                    name, aw.get("title"), aw.get("date"),
                    med, dims, _artsy_price(aw),
                    seller or "Artsy", art_url, "Artsy",
                    _artsy_img(aw), is_auc,
                ))
            print(f"Artsy [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Artsy [{name}]: {e}")
            return []

async def scrape_artsy(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=20) as client:
        batches = await asyncio.gather(*[_artsy_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Artnet ────────────────────────────────────────────────────────────────────
async def _artnet_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url  = f"https://www.artnet.com/artists/{slug(name)}/works-for-sale/"
            resp = await client.get(url, headers=HEADERS, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")
            results = []
            for card in soup.select("[class*='artwork-card'], [class*='GridItem'], [class*='ArtworkCard']")[:15]:
                title   = (card.select_one("h2, h3, [class*='title']") or {}).get_text(strip=True) or "Untitled"
                med     = (card.select_one("[class*='medium']") or {}).get_text(strip=True)
                if not valid_medium(med):
                    continue
                price   = (card.select_one("[class*='price']") or {}).get_text(strip=True)
                img     = card.select_one("img")
                img_url = img.get("src") or img.get("data-src") or "" if img else ""
                a_tag   = card.select_one("a")
                href    = a_tag.get("href", "") if a_tag else ""
                src_url = href if href.startswith("http") else f"https://www.artnet.com{href}"
                results.append(make_row(name, title, "", med, "", price, "", src_url, "Artnet", img_url, False))
            print(f"Artnet [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Artnet [{name}]: {e}")
            return []

async def scrape_artnet(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_artnet_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Sotheby's ─────────────────────────────────────────────────────────────────
def _sothebys_find_lots(obj, depth=0) -> List[dict]:
    """Scan __NEXT_DATA__ for arrays that look like lot listings."""
    if depth > 15 or not obj:
        return []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("lots", "items", "results", "data", "nodes", "edges") and isinstance(v, list) and v:
                first = v[0] if isinstance(v[0], dict) else (v[0].get("node") if isinstance(v[0], dict) else None)
                if first and any(x in first for x in ("lotNumber", "lotId", "title", "estimate", "lotTitle")):
                    return v
            sub = _sothebys_find_lots(v, depth + 1)
            if sub:
                return sub
    elif isinstance(obj, list):
        for item in obj:
            sub = _sothebys_find_lots(item, depth + 1)
            if sub:
                return sub
    return []

async def _sothebys_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url    = "https://www.sothebys.com/en/buy/auction/search"
            params = {"query": name, "locale": "en"}
            resp   = await client.get(url, params=params, headers=HEADERS, follow_redirects=True)
            soup   = BeautifulSoup(resp.text, "html.parser")
            results = []

            # Try __NEXT_DATA__ first
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                try:
                    nd   = json.loads(nd_tag.string)
                    lots = _sothebys_find_lots(nd)
                    for lot in lots[:10]:
                        # Unwrap Relay edge/node if present
                        if "node" in lot and isinstance(lot["node"], dict):
                            lot = lot["node"]
                        title    = lot.get("title") or lot.get("lotTitle") or lot.get("description") or "Untitled"
                        estimate = lot.get("estimate") or lot.get("estimateDisplay") or lot.get("estimatedValue") or ""
                        if isinstance(estimate, dict):
                            lo, hi = estimate.get("from") or estimate.get("low") or "", estimate.get("to") or estimate.get("high") or ""
                            estimate = f"Est. {lo}–{hi}" if lo and hi else str(lo or hi or "")
                        img_raw  = lot.get("image") or lot.get("thumbnail") or lot.get("imageUrl") or {}
                        img_url  = (img_raw.get("url") or img_raw.get("src") or "") if isinstance(img_raw, dict) else (img_raw if isinstance(img_raw, str) else "")
                        lot_path = lot.get("url") or lot.get("path") or lot.get("lotUrl") or ""
                        lot_url  = lot_path if lot_path.startswith("http") else f"https://www.sothebys.com{lot_path}"
                        results.append(make_row(name, title, "", "", "", estimate, "Sotheby's", lot_url, "Sotheby's", img_url, True))
                except Exception as e:
                    print(f"Sotheby's [{name}] __NEXT_DATA__ parse: {e}")

            # HTML card fallback
            if not results:
                for card in soup.select("[class*='LotCard'], [class*='lotCard'], [class*='lot-card'], article")[:10]:
                    title    = (card.select_one("h2, h3, [class*='title'], [class*='Title']") or {}).get_text(strip=True) or "Untitled"
                    estimate = (card.select_one("[class*='estimate'], [class*='Estimate'], [class*='price']") or {}).get_text(strip=True)
                    img      = card.select_one("img")
                    img_url  = (img.get("src") or img.get("data-src") or "") if img else ""
                    a_tag    = card.select_one("a")
                    href     = (a_tag.get("href") or "") if a_tag else ""
                    lot_url  = href if href.startswith("http") else f"https://www.sothebys.com{href}"
                    if title and title != "Untitled":
                        results.append(make_row(name, title, "", "", "", estimate, "Sotheby's", lot_url, "Sotheby's", img_url, True))

            print(f"Sotheby's [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Sotheby's [{name}]: {e}")
            return []

async def scrape_sothebys(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(timeout=15) as client:
        batches = await asyncio.gather(*[_sothebys_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Christie's ────────────────────────────────────────────────────────────────
async def _christies_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url = "https://www.christies.com/api/discoverywebsite/auctioneventandlot/lotfinderputs"
            # Try POST (the endpoint name suggests it accepts a body)
            lots = []
            try:
                resp = await client.post(
                    url,
                    json={"keyword": name, "num_per_page": 10, "start_num": 0, "filters": {}},
                    headers={**HEADERS, "Content-Type": "application/json", "Accept": "application/json"},
                )
                data = resp.json()
                lots = data.get("lots") or []
            except Exception:
                # Fall back to GET
                resp = await client.get(
                    url,
                    params={"keyword": name, "language": "en", "sortby": "relevance", "upcoming": "true", "pagesize": 10},
                    headers={**HEADERS, "Accept": "application/json"},
                )
                data = resp.json()
                lots = data.get("lots") or []

            results = []
            for lot in lots[:10]:
                title   = lot.get("object_name") or lot.get("title_primary_txt") or lot.get("title") or "Untitled"
                price   = lot.get("estimate_txt") or lot.get("price_realised_txt") or lot.get("estimate") or ""
                img_url = (lot.get("image") or {}).get("image_url") or lot.get("imageUrl") or ""
                lot_id  = lot.get("lotid") or lot.get("id") or ""
                lot_url = f"https://www.christies.com/lot/lot-{lot_id}" if lot_id else "https://www.christies.com"
                results.append(make_row(name, title, "", "", "", price, "Christie's", lot_url, "Christie's", img_url, True))
            print(f"Christie's [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Christie's [{name}]: {e}")
            return []

async def scrape_christies(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_christies_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Phillips ──────────────────────────────────────────────────────────────────
async def _phillips_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url    = "https://www.phillips.com/api/search/lots"
            params = {"q": name, "upcoming": "true", "pageSize": 10}
            resp   = await client.get(url, params=params, headers={**HEADERS, "Accept": "application/json"})
            data   = resp.json()
            lots   = data.get("hits") or data.get("results") or data.get("lots") or []
            results = []
            for lot in lots[:10]:
                title   = lot.get("title") or lot.get("lotTitle") or lot.get("description") or "Untitled"
                price   = lot.get("estimateDisplay") or lot.get("estimate") or lot.get("price") or ""
                img_url = lot.get("imageUrl") or lot.get("image") or lot.get("thumbnailUrl") or ""
                lot_url = lot.get("url") or lot.get("lotUrl") or "https://www.phillips.com"
                if not lot_url.startswith("http"):
                    lot_url = f"https://www.phillips.com{lot_url}"
                results.append(make_row(name, title, "", "", "", price, "Phillips", lot_url, "Phillips", img_url, True))
            print(f"Phillips [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Phillips [{name}]: {e}")
            return []

async def scrape_phillips(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=12) as client:
        batches = await asyncio.gather(*[_phillips_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Seesaw ────────────────────────────────────────────────────────────────────
async def _seesaw_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url  = f"https://www.seesaw.website/works?q={name.replace(' ', '+')}&for_sale=true"
            resp = await client.get(url, headers=HEADERS, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")
            results = []
            for card in soup.select("[class*='WorkCard'], article, [class*='work-card']")[:15]:
                title   = (card.select_one("h2, h3, [class*='title']") or {}).get_text(strip=True) or "Untitled"
                med     = (card.select_one("[class*='medium']") or {}).get_text(strip=True)
                if not valid_medium(med):
                    continue
                price   = (card.select_one("[class*='price']") or {}).get_text(strip=True)
                img     = card.select_one("img")
                img_url = img.get("src", "") if img else ""
                a_tag   = card.select_one("a")
                href    = a_tag.get("href", "") if a_tag else ""
                src_url = href if href.startswith("http") else f"https://www.seesaw.website{href}"
                results.append(make_row(name, title, "", med, "", price, "", src_url, "Seesaw", img_url, False))
            print(f"Seesaw [{name}]: {len(results)} works")
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
            _set_status(db, "running", f"Scraping {len(artists)} artists across Artsy, Artnet, Sotheby's, Christie's, Phillips, Seesaw…")

            artsy_r, artnet_r, sothebys_r, christies_r, phillips_r, seesaw_r = await asyncio.gather(
                scrape_artsy(artists),
                scrape_artnet(artists),
                scrape_sothebys(artists),
                scrape_christies(artists),
                scrape_phillips(artists),
                scrape_seesaw(artists),
            )
            all_results = artsy_r + artnet_r + sothebys_r + christies_r + phillips_r + seesaw_r

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
