import os, uuid, re, asyncio, json, urllib.parse
from datetime import datetime
from typing import Optional, List

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, String, Boolean, DateTime, Text, text as sa_text
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
    date_listed = Column(String)   # when the work was listed / article published


class ScrapeStatus(Base):
    __tablename__ = "scrape_status"
    id       = Column(String, primary_key=True, default=lambda: "singleton")
    status   = Column(String, default="idle")
    last_run = Column(DateTime)
    message  = Column(Text)


Base.metadata.create_all(bind=engine)

# Schema migration: add date_listed column to existing tables if missing
try:
    with engine.connect() as _conn:
        if "sqlite" in DATABASE_URL:
            _conn.execute(sa_text("ALTER TABLE artworks ADD COLUMN date_listed TEXT"))
        else:
            _conn.execute(sa_text("ALTER TABLE artworks ADD COLUMN IF NOT EXISTS date_listed VARCHAR"))
        _conn.commit()
except Exception:
    pass  # Column already exists — safe to ignore

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID  = "app8GJgnPtP23cObV"
AIRTABLE_TABLE_ID = "tblClPSzECL3rpbd0"
SCRAPERAPI_KEY    = os.getenv("SCRAPERAPI_KEY", "")

# Global concurrency limit for ScraperAPI.
# render=true requests count as 5 credits each and are slower, so we keep
# the concurrency tight (3) to avoid hitting rate limits.
_SA_SEM = asyncio.Semaphore(3)

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

# Used to skip auction house listings when scraping private-sale platforms
AUCTION_KEYWORDS = [
    "auction", "christie", "sotheby", "phillips", "bonhams", "heritage",
    "wright", "rago", "swann", "doyle", "ketterer", "artcurial",
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

def is_auction_seller(seller: str) -> bool:
    if not seller:
        return False
    return any(h in seller.lower() for h in AUCTION_KEYWORDS)

def slug(name: str) -> str:
    s = re.sub(r"[^\w\s-]", "", name.lower().strip())
    return re.sub(r"\s+", "-", s)

def make_row(artist, title, year, medium, dims, price, seller,
             src_url, src_name, img, is_auc, date_listed=""):
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
        "date_listed": date_listed or "",
    }

def _purl(url: str, render: bool = False) -> str:
    """Wrap URL through ScraperAPI.
    render=True uses ScraperAPI's headless Chrome to execute JavaScript —
    essential for SPA sites that load artwork data client-side.
    Costs 5 credits instead of 1, but is the only reliable approach for
    React/Next.js sites that don't fully SSR their artwork grids.
    """
    if not SCRAPERAPI_KEY:
        return url
    params = f"api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(url)}"
    if render:
        params += "&render=true"
    return f"http://api.scraperapi.com?{params}"

async def _pget(client: httpx.AsyncClient, url: str,
                render: bool = False, **kwargs) -> httpx.Response:
    """HTTP GET through ScraperAPI with global concurrency cap."""
    target = _purl(url, render=render)
    if SCRAPERAPI_KEY:
        async with _SA_SEM:
            return await client.get(target, **kwargs)
    return await client.get(target, **kwargs)

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

# ── Artsy — private gallery sales ─────────────────────────────────────────────
# Artsy is a Next.js + Relay app. Their works-for-sale pages load artwork cards
# via client-side GraphQL after the initial HTML shell. Without JS execution,
# __NEXT_DATA__ only contains relay config, not the actual artwork records.
# ScraperAPI render=True (headless Chrome) executes the JS and gives us the
# fully rendered page — the only reliable way to get artwork data.

def _artsy_find_artworks(obj, depth=0) -> List[dict]:
    """Find Artsy artwork records from fully-rendered HTML.
    After JS execution, artworks appear as data-* attributes and in a relay
    store embedded as a second __NEXT_DATA__ script, or in window variables.
    We look for any dict that looks like an artwork record.
    """
    if depth > 30 or not obj:
        return []
    results = []
    if isinstance(obj, dict):
        typename = obj.get("__typename", "")
        # Explicit Relay typename
        if typename == "Artwork":
            results.append(obj)
        # Heuristic: objects with title + multiple artwork-specific fields
        elif (typename not in ("Artist", "Partner", "Gene", "Tag", "Fair",
                               "Sale", "HomePage", "MarketingCollection",
                               "ArtistGroup", "ArtworkFilterAggregation")) and \
             "title" in obj and \
             sum(1 for k in ("medium", "date", "image", "saleMessage",
                             "internalID", "slug", "availability",
                             "listPrice", "price") if k in obj) >= 2:
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
            url = f"https://www.artsy.net/artist/{slug(name)}/works-for-sale"
            # render=True: ScraperAPI headless Chrome executes React/Relay JS
            # so artwork cards are actually populated in the DOM / window state
            resp = await _pget(client, url, render=True, headers=HEADERS,
                               follow_redirects=True)
            print(f"Artsy [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")

            # ── Strategy 1: parse __NEXT_DATA__ relay store ──────────────────
            results = []
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                nd_data = json.loads(nd_tag.string)
                # Debug: show top-level shape so we can diagnose if 0 results
                top_keys = list(nd_data.keys())
                print(f"Artsy [{name}] __NEXT_DATA__ keys: {top_keys}")
                aw_nodes = _artsy_find_artworks(nd_data)
                print(f"Artsy [{name}]: relay store → {len(aw_nodes)} artwork candidates")
                seen = set()
                for aw in aw_nodes:
                    key = aw.get("internalID") or aw.get("slug") or aw.get("title")
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    med = aw.get("medium") or ""
                    if not valid_medium(med):
                        continue
                    partner = aw.get("partner") or {}
                    seller  = partner.get("name", "") if isinstance(partner, dict) else ""
                    if is_auction_seller(seller):   # private sales only
                        continue
                    dims_obj = aw.get("dimensions") or {}
                    dims = ""
                    if isinstance(dims_obj, dict):
                        in_o = dims_obj.get("in") or {}
                        cm_o = dims_obj.get("cm") or {}
                        dims = (in_o.get("text") if isinstance(in_o, dict) else "") or \
                               (cm_o.get("text") if isinstance(cm_o, dict) else "") or ""
                    art_slug = aw.get("slug") or ""
                    art_url  = f"https://www.artsy.net/artwork/{art_slug}" if art_slug else url
                    date_l   = (aw.get("published_at") or aw.get("publishedAt") or
                                aw.get("listingUpdatedAt") or aw.get("updatedAt") or "")
                    results.append(make_row(
                        name, aw.get("title"), aw.get("date"),
                        med, dims, _artsy_price(aw),
                        seller or "Artsy", art_url, "Artsy",
                        _artsy_img(aw), False, date_l,
                    ))

            # ── Strategy 2: rendered HTML card scraping ───────────────────────
            # After render=True, Artsy's React app populates the DOM with cards
            if not results:
                card_sel = (
                    "[data-testid='artworkGridItem'], "
                    "[class*='ArtworkGridItem'], "
                    "[class*='artwork-item'], "
                    "[class*='GridItem__Cell'], "
                    "[class*='ArtworkBrick']"
                )
                for card in soup.select(card_sel)[:20]:
                    a_tag = card.select_one("a[href*='/artwork/']")
                    href  = (a_tag.get("href") or "") if a_tag else ""
                    art_url = href if href.startswith("http") else f"https://www.artsy.net{href}"
                    title = (card.select_one("[class*='title'], [class*='Title'], h2, h3")
                             or {}).get_text(strip=True) or "Untitled"
                    med   = (card.select_one("[class*='medium'], [class*='Medium']")
                             or {}).get_text(strip=True)
                    if not valid_medium(med):
                        continue
                    price_el = card.select_one("[class*='price'], [class*='Price'], [class*='saleMessage']")
                    price = price_el.get_text(strip=True) if price_el else ""
                    img   = card.select_one("img")
                    img_u = (img.get("src") or img.get("data-src") or "") if img else ""
                    results.append(make_row(
                        name, title, "", med, "", price,
                        "Artsy", art_url, "Artsy", img_u, False,
                    ))

            print(f"Artsy [{name}]: {len(results)} private-sale works")
            return results
        except Exception as e:
            print(f"Artsy [{name}]: {e}")
            return []

async def scrape_artsy(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(2)   # render=True is slow; keep concurrency low
    async with httpx.AsyncClient(timeout=90) as client:
        batches = await asyncio.gather(*[_artsy_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Artnet — private gallery sales ────────────────────────────────────────────
# Artnet's artist page shows works currently for sale at galleries.
# The artist page is large (100-200KB) but cards are rendered by React,
# so we need render=True to get populated DOM.

def _artnet_find_works(obj, depth=0) -> List[dict]:
    """Locate artwork list in Artnet __NEXT_DATA__ or window state."""
    if depth > 20 or not obj:
        return []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if (k in ("artworks", "works", "results", "items", "forSale",
                      "artworksForSale", "worksForSale") and
                    isinstance(v, list) and v):
                first = v[0] if isinstance(v[0], dict) else {}
                if any(x in first for x in ("title", "name", "price",
                                             "image", "imageUrl", "medium")):
                    return v
            sub = _artnet_find_works(v, depth + 1)
            if sub:
                return sub
    elif isinstance(obj, list):
        for item in obj:
            sub = _artnet_find_works(item, depth + 1)
            if sub:
                return sub
    return []

async def _artnet_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            # Artnet artist page — works-for-sale section is rendered by React
            url  = f"https://www.artnet.com/artists/{slug(name)}/"
            resp = await _pget(client, url, render=True, headers=HEADERS,
                               follow_redirects=True)
            print(f"Artnet [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
            soup    = BeautifulSoup(resp.text, "html.parser")
            results = []

            # Strategy 1: __NEXT_DATA__ JSON
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                try:
                    nd = json.loads(nd_tag.string)
                    top_keys = list(nd.keys())
                    print(f"Artnet [{name}] __NEXT_DATA__ keys: {top_keys}")
                    artworks = _artnet_find_works(nd)
                    for aw in artworks[:15]:
                        title = aw.get("title") or aw.get("name") or "Untitled"
                        med   = (aw.get("medium") or aw.get("materials") or
                                 aw.get("mediumDisplay") or "")
                        if not valid_medium(med):
                            continue
                        price = (aw.get("price") or aw.get("priceDisplay") or
                                 aw.get("askingPrice") or "")
                        if isinstance(price, dict):
                            price = price.get("display") or price.get("value") or ""
                        img_r = (aw.get("image") or aw.get("imageUrl") or
                                 aw.get("thumbnail") or {})
                        img_u = ((img_r.get("url") or img_r.get("src") or "")
                                 if isinstance(img_r, dict) else
                                 (img_r if isinstance(img_r, str) else ""))
                        gallery = (aw.get("gallery") or aw.get("partner") or
                                   aw.get("seller") or {})
                        seller  = ((gallery.get("name") or "")
                                   if isinstance(gallery, dict) else str(gallery or ""))
                        path    = aw.get("url") or aw.get("href") or aw.get("slug") or ""
                        work_url = (path if path.startswith("http")
                                    else f"https://www.artnet.com{path}")
                        year    = str(aw.get("year") or aw.get("date") or
                                      aw.get("creationDate") or "")
                        date_l  = (aw.get("listedAt") or aw.get("createdAt") or
                                   aw.get("updatedAt") or aw.get("dateAdded") or "")
                        results.append(make_row(
                            name, title, year, med, "", str(price),
                            seller or "Artnet", work_url, "Artnet", img_u, False, date_l,
                        ))
                except Exception as e:
                    print(f"Artnet [{name}] __NEXT_DATA__: {e}")

            # Strategy 2: rendered HTML cards (after JS execution)
            if not results:
                selectors = (
                    "[class*='artwork-card'], [class*='ArtworkCard'], "
                    "[class*='GridItem'], [class*='artwork_card'], "
                    "[class*='WorkCard'], [class*='work-card'], "
                    "[class*='item-card'], [class*='ItemCard']"
                )
                for card in soup.select(selectors)[:15]:
                    title = (card.select_one(
                        "h2, h3, [class*='title'], [class*='Title'], "
                        "[class*='name'], [class*='Name']") or {}).get_text(strip=True) or "Untitled"
                    med   = (card.select_one(
                        "[class*='medium'], [class*='Medium'], "
                        "[class*='material']") or {}).get_text(strip=True)
                    if not valid_medium(med):
                        continue
                    price   = (card.select_one(
                        "[class*='price'], [class*='Price'], "
                        "[class*='asking']") or {}).get_text(strip=True)
                    img     = card.select_one("img")
                    img_url = (img.get("src") or img.get("data-src") or "") if img else ""
                    a_tag   = card.select_one("a")
                    href    = (a_tag.get("href") or "") if a_tag else ""
                    src_url = href if href.startswith("http") else f"https://www.artnet.com{href}"
                    date_el = card.select_one("time, [class*='date'], [class*='Date'], [datetime]")
                    date_l  = ""
                    if date_el:
                        date_l = date_el.get("datetime") or date_el.get_text(strip=True)
                    results.append(make_row(
                        name, title, "", med, "", price, "",
                        src_url, "Artnet", img_url, False, date_l,
                    ))

            print(f"Artnet [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Artnet [{name}]: {e}")
            return []

async def scrape_artnet(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(2)
    async with httpx.AsyncClient(timeout=90) as client:
        batches = await asyncio.gather(*[_artnet_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Seesaw — private gallery sales ───────────────────────────────────────────
async def _seesaw_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        for url in [
            f"https://www.seesaw.website/works?q={urllib.parse.quote(name)}&for_sale=true",
            f"https://www.seesaw.website/search?q={urllib.parse.quote(name)}",
            f"https://www.seesaw.website/artworks?search={urllib.parse.quote(name)}",
        ]:
            try:
                resp = await _pget(client, url, render=True, headers=HEADERS,
                                   follow_redirects=True)
                print(f"Seesaw [{name}] {url}: HTTP {resp.status_code} | {len(resp.text)} chars")
                if resp.status_code != 200:
                    continue
                soup    = BeautifulSoup(resp.text, "html.parser")
                results = []

                # Check for __NEXT_DATA__
                nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
                if nd_tag and nd_tag.string:
                    nd = json.loads(nd_tag.string)
                    def _find_works(o, d=0):
                        if d > 20 or not o:
                            return []
                        if isinstance(o, dict):
                            for k, v in o.items():
                                if k in ("works", "artworks", "results", "items") and isinstance(v, list) and v:
                                    first = v[0] if isinstance(v[0], dict) else {}
                                    if any(x in first for x in ("title", "name", "medium")):
                                        return v
                                sub = _find_works(v, d + 1)
                                if sub:
                                    return sub
                        elif isinstance(o, list):
                            for i in o:
                                sub = _find_works(i, d + 1)
                                if sub:
                                    return sub
                        return []
                    for aw in _find_works(nd)[:15]:
                        title = aw.get("title") or aw.get("name") or "Untitled"
                        med   = aw.get("medium") or aw.get("materials") or ""
                        if not valid_medium(med):
                            continue
                        price  = aw.get("price") or aw.get("priceDisplay") or ""
                        img_r  = aw.get("image") or aw.get("imageUrl") or aw.get("thumbnail") or {}
                        img_u  = ((img_r.get("url") or img_r.get("src") or "")
                                  if isinstance(img_r, dict) else
                                  (img_r if isinstance(img_r, str) else ""))
                        path   = aw.get("url") or aw.get("href") or aw.get("slug") or ""
                        w_url  = (path if path.startswith("http")
                                  else f"https://www.seesaw.website{path}")
                        date_l = aw.get("createdAt") or aw.get("publishedAt") or ""
                        results.append(make_row(
                            name, title, "", med, "", str(price), "",
                            w_url, "Seesaw", img_u, False, date_l,
                        ))

                # HTML fallback
                if not results:
                    for card in soup.select(
                        "[class*='WorkCard'], [class*='work-card'], "
                        "[class*='ArtworkCard'], article"
                    )[:15]:
                        title   = (card.select_one("h2, h3, [class*='title']") or {}).get_text(strip=True) or "Untitled"
                        med     = (card.select_one("[class*='medium']") or {}).get_text(strip=True)
                        if not valid_medium(med):
                            continue
                        price   = (card.select_one("[class*='price']") or {}).get_text(strip=True)
                        img     = card.select_one("img")
                        img_url = img.get("src", "") if img else ""
                        a_tag   = card.select_one("a")
                        href    = (a_tag.get("href") or "") if a_tag else ""
                        src_url = href if href.startswith("http") else f"https://www.seesaw.website{href}"
                        date_el = card.select_one("time, [datetime]")
                        date_l  = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
                        results.append(make_row(name, title, "", med, "", price, "",
                                                src_url, "Seesaw", img_url, False, date_l))

                if results:
                    print(f"Seesaw [{name}]: {len(results)} works")
                    return results
            except Exception as e:
                print(f"Seesaw [{name}]: {e}")

        print(f"Seesaw [{name}]: 0 works (all URLs tried)")
        return []

async def scrape_seesaw(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(2)
    async with httpx.AsyncClient(timeout=90) as client:
        batches = await asyncio.gather(*[_seesaw_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Ocula — gallery / private sales ──────────────────────────────────────────
def _ocula_find_artworks(obj, depth=0) -> List[dict]:
    """Recursively find artwork objects in Ocula's JSON.
    Ocula nodes carry 'title' + at least one of medium/price/gallery/imageUrl.
    """
    if depth > 20 or not obj:
        return []
    results = []
    if isinstance(obj, dict):
        if ("title" in obj and
                any(k in obj for k in ("medium", "price", "gallery",
                                       "imageUrl", "image", "askingPrice",
                                       "availableForSale"))):
            results.append(obj)
        else:
            for v in obj.values():
                results.extend(_ocula_find_artworks(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_ocula_find_artworks(item, depth + 1))
    return results

async def _ocula_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        try:
            url  = f"https://ocula.com/artists/{slug(name)}/artworks/"
            resp = await _pget(client, url, render=True, headers=HEADERS,
                               follow_redirects=True)
            print(f"Ocula [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
            if resp.status_code != 200:
                return []
            soup    = BeautifulSoup(resp.text, "html.parser")
            results = []

            # Try __NEXT_DATA__
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                try:
                    nd = json.loads(nd_tag.string)
                    print(f"Ocula [{name}] __NEXT_DATA__ keys: {list(nd.keys())}")
                    artworks = _ocula_find_artworks(nd)
                    seen = set()
                    for aw in artworks[:20]:
                        title = aw.get("title") or aw.get("name") or "Untitled"
                        key   = f"{name}_{title}"
                        if key in seen:
                            continue
                        seen.add(key)
                        year = str(aw.get("year") or aw.get("date") or "")
                        med  = (aw.get("medium") or aw.get("materials") or
                                aw.get("mediumDisplay") or "")
                        if not valid_medium(med):
                            continue
                        price = (aw.get("price") or aw.get("priceDisplay") or
                                 aw.get("askingPrice") or "")
                        if isinstance(price, dict):
                            price = price.get("display") or price.get("value") or ""
                        img_r = (aw.get("image") or aw.get("imageUrl") or
                                 aw.get("thumbnail") or aw.get("images") or {})
                        if isinstance(img_r, list) and img_r:
                            img_r = img_r[0]
                        img_u = ((img_r.get("url") or img_r.get("src") or
                                  img_r.get("file") or "")
                                 if isinstance(img_r, dict) else
                                 (img_r if isinstance(img_r, str) and
                                  img_r.startswith("http") else ""))
                        gallery = (aw.get("gallery") or aw.get("galleries") or
                                   aw.get("partner") or {})
                        if isinstance(gallery, list) and gallery:
                            gallery = gallery[0]
                        seller  = ((gallery.get("name") or gallery.get("title") or "")
                                   if isinstance(gallery, dict) else "")
                        path     = aw.get("url") or aw.get("slug") or aw.get("href") or ""
                        work_url = (path if path.startswith("http")
                                    else (f"https://ocula.com{path}" if path else url))
                        date_l   = (aw.get("dateAdded") or aw.get("createdAt") or
                                    aw.get("publishedAt") or aw.get("listed_at") or "")
                        results.append(make_row(
                            name, title, year, str(med), "", str(price),
                            seller or "Ocula", work_url, "Ocula", img_u, False, date_l,
                        ))
                except Exception as e:
                    print(f"Ocula [{name}] __NEXT_DATA__: {e}")

            # HTML fallback
            if not results:
                card_sel = (
                    "[class*='ArtworkCard'], [class*='artwork-card'], "
                    "[class*='WorkCard'], [class*='work-card'], "
                    "[class*='artwork_card'], [class*='ArtworkItem'], "
                    "[class*='artwork-item']"
                )
                for card in soup.select(card_sel)[:15]:
                    title = (card.select_one(
                        "h2, h3, [class*='title'], [class*='Title']") or {}).get_text(strip=True) or "Untitled"
                    med   = (card.select_one(
                        "[class*='medium'], [class*='Medium'], "
                        "[class*='materials']") or {}).get_text(strip=True)
                    if not valid_medium(med):
                        continue
                    price = (card.select_one(
                        "[class*='price'], [class*='Price']") or {}).get_text(strip=True)
                    img   = card.select_one("img")
                    img_u = (img.get("src") or img.get("data-src") or "") if img else ""
                    a     = card.select_one("a")
                    href  = (a.get("href") or "") if a else ""
                    work_url = (href if href.startswith("http")
                                else (f"https://ocula.com{href}" if href else url))
                    date_el = card.select_one("time, [class*='date'], [datetime]")
                    date_l  = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
                    if title and title != "Untitled":
                        results.append(make_row(
                            name, title, "", med, "", price,
                            "Ocula", work_url, "Ocula", img_u, False, date_l,
                        ))

            print(f"Ocula [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Ocula [{name}]: {e}")
            return []

async def scrape_ocula(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(2)
    async with httpx.AsyncClient(timeout=90) as client:
        batches = await asyncio.gather(*[_ocula_one(client, sem, n) for n in artists])
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
            proxy_note = (" via ScraperAPI (render=True)" if SCRAPERAPI_KEY
                          else " (add SCRAPERAPI_KEY for full results)")
            _set_status(db, "running",
                        f"Scraping {len(artists)} artists · private sales "
                        f"across 4 sources{proxy_note}…")

            # 4 private-sale sources, all share _SA_SEM (cap = 3 render=True concurrent)
            artsy_r, artnet_r, seesaw_r, ocula_r = await asyncio.gather(
                scrape_artsy(artists),
                scrape_artnet(artists),
                scrape_seesaw(artists),
                scrape_ocula(artists),
            )
            all_results = artsy_r + artnet_r + seesaw_r + ocula_r

            db.query(Artwork).delete()
            seen, saved = set(), 0
            for item in all_results:
                if item["source_id"] in seen:
                    continue
                seen.add(item["source_id"])
                db.add(Artwork(id=str(uuid.uuid4()), **item))
                saved += 1
            db.commit()
            _set_status(db, "done",
                        f"Found {saved} private-sale works across {len(artists)} artists")
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
        "date_listed": r.date_listed,
    }

@app.get("/api/artworks")
def get_artworks(artist: Optional[str] = None, sale_type: Optional[str] = None,
                 source: Optional[str] = None):
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
            return {"status": "idle", "last_run": None,
                    "message": "Click Refresh Data to start"}
        return {
            "status": s.status,
            "last_run": s.last_run.isoformat() if s.last_run else None,
            "message": s.message,
        }
    finally:
        db.close()

# Serves the frontend — must be last
app.mount("/", StaticFiles(directory="static", html=True), name="static")
