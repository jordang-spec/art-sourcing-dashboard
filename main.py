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
    date_listed = Column(String)


class ScrapeStatus(Base):
    __tablename__ = "scrape_status"
    id       = Column(String, primary_key=True, default=lambda: "singleton")
    status   = Column(String, default="idle")
    last_run = Column(DateTime)
    message  = Column(Text)


Base.metadata.create_all(bind=engine)

# Migration: add date_listed if it doesn't exist yet
try:
    with engine.connect() as _conn:
        if "sqlite" in DATABASE_URL:
            _conn.execute(sa_text("ALTER TABLE artworks ADD COLUMN date_listed TEXT"))
        else:
            _conn.execute(sa_text("ALTER TABLE artworks ADD COLUMN IF NOT EXISTS date_listed VARCHAR"))
        _conn.commit()
except Exception:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID  = "app8GJgnPtP23cObV"
AIRTABLE_TABLE_ID = "tblClPSzECL3rpbd0"
SCRAPERAPI_KEY    = os.getenv("SCRAPERAPI_KEY", "")

# Global ScraperAPI concurrency cap — standard requests (no render) are fast,
# so we can run 5 at once without hitting the free-tier limit.
_SA_SEM = asyncio.Semaphore(5)

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

AUCTION_KEYWORDS = [
    "auction", "christie", "sotheby", "phillips", "bonhams", "heritage",
    "wright", "rago", "swann", "doyle", "ketterer", "artcurial",
]

# Focused fallback list for dev/testing when Airtable isn't configured.
# Deliberately small (15) so test scrapes finish fast.
FALLBACK_ARTISTS = [
    "Gerhard Richter", "Jean-Michel Basquiat", "Andy Warhol",
    "Joan Mitchell", "Christopher Wool", "Cecily Brown", "George Condo",
    "Barkley L. Hendricks", "Helen Frankenthaler", "Henry Taylor",
    "Amy Sillman", "Lynette Yiadom-Boakye", "Sam Gilliam",
    "Tracey Emin", "Bridget Riley",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def valid_medium(medium: str) -> bool:
    if not medium:
        return True
    return not any(ex in medium.lower() for ex in EXCLUDED_MEDIUMS)

def is_auction_seller(seller: str) -> bool:
    if not seller:
        return False
    return any(h in seller.lower() for h in AUCTION_KEYWORDS)

def slug(name: str) -> str:
    return re.sub(r"\s+", "-", re.sub(r"[^\w\s-]", "", name.lower().strip()))

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

def _purl(url: str) -> str:
    """Route through ScraperAPI (no render — fast, 1 credit each)."""
    if not SCRAPERAPI_KEY:
        return url
    return f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(url)}"

async def _pget(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
    """GET via ScraperAPI with global concurrency cap."""
    target = _purl(url)
    if SCRAPERAPI_KEY:
        async with _SA_SEM:
            return await client.get(target, **kwargs)
    return await client.get(target, **kwargs)

# ── Airtable ──────────────────────────────────────────────────────────────────
async def get_target_artists() -> List[str]:
    if not AIRTABLE_TOKEN:
        return FALLBACK_ARTISTS
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    params = [
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

# ── Artsy — GraphQL API (fast, no ScraperAPI) then HTML fallback ──────────────
# Artsy's metaphysics GraphQL endpoint serves public artwork data without auth.
# Direct API calls are ~1-2 s each vs 30-45 s for render=True scraping.
# This is the right architectural choice for Artsy — use their own API.

ARTSY_GQL = "https://metaphysics.artsy.net/v2"

_ARTSY_QUERY = """
query ArtistPrivateSales($slug: String!) {
  artist(id: $slug) {
    artworksForSale(first: 20) {
      edges {
        node {
          title
          date
          medium
          slug
          saleMessage
          listPrice {
            ... on Money { display }
            ... on PriceRange { display }
          }
          image { resized(width: 500) { src } }
          partner { name }
          dimensions { in { text } cm { text } }
        }
      }
    }
  }
}
"""

async def _artsy_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        slug_val = slug(name)

        # ── Primary path: direct GraphQL API ─────────────────────────────
        try:
            resp = await client.post(
                ARTSY_GQL,
                json={"query": _ARTSY_QUERY, "variables": {"slug": slug_val}},
                headers={"Content-Type": "application/json",
                         "User-Agent": HEADERS["User-Agent"]},
                timeout=15,
            )
            print(f"Artsy GQL [{name}]: HTTP {resp.status_code}")
            if resp.status_code == 200:
                body = resp.json()
                edges = ((body.get("data") or {})
                         .get("artist") or {})
                edges = (edges.get("artworksForSale") or {}).get("edges") or []
                if edges:
                    results = []
                    for edge in edges:
                        node = edge.get("node") or {}
                        med  = node.get("medium") or ""
                        if not valid_medium(med):
                            continue
                        partner = node.get("partner") or {}
                        seller  = partner.get("name", "") if isinstance(partner, dict) else ""
                        if is_auction_seller(seller):
                            continue
                        # Price: prefer listPrice.display, fall back to saleMessage
                        price = ""
                        lp = node.get("listPrice") or {}
                        if isinstance(lp, dict):
                            price = lp.get("display") or ""
                        if not price:
                            price = node.get("saleMessage") or ""
                        img_url = ""
                        img = (node.get("image") or {})
                        if isinstance(img, dict):
                            resized = img.get("resized") or {}
                            img_url = resized.get("src") or ""
                        dims_obj = node.get("dimensions") or {}
                        dims = ""
                        if isinstance(dims_obj, dict):
                            in_o = dims_obj.get("in") or {}
                            dims = (in_o.get("text") or "") if isinstance(in_o, dict) else ""
                        art_s   = node.get("slug") or ""
                        art_url = f"https://www.artsy.net/artwork/{art_s}" if art_s else ""
                        results.append(make_row(
                            name, node.get("title"), node.get("date"),
                            med, dims, price,
                            seller or "Artsy", art_url, "Artsy", img_url, False,
                        ))
                    print(f"Artsy GQL [{name}]: {len(results)} private-sale works")
                    return results
                else:
                    print(f"Artsy GQL [{name}]: 0 edges (artist may have no for-sale works)")
                    return []
        except Exception as e:
            print(f"Artsy GQL [{name}]: {e}")

        # ── Fallback: HTML scraping via ScraperAPI ────────────────────────
        try:
            url  = f"https://www.artsy.net/artist/{slug_val}/works-for-sale"
            resp = await _pget(client, url, headers=HEADERS, follow_redirects=True)
            print(f"Artsy HTML [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
            if resp.status_code != 200:
                return []
            soup   = BeautifulSoup(resp.text, "html.parser")
            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if not nd_tag or not nd_tag.string:
                return []
            nd_data = json.loads(nd_tag.string)

            # Debug: log all __typename values found so we know the relay store shape
            typenames: dict = {}
            def _count_tn(o, d=0):
                if d > 35 or not o:
                    return
                if isinstance(o, dict):
                    t = o.get("__typename")
                    if t:
                        typenames[t] = typenames.get(t, 0) + 1
                    for v in o.values():
                        _count_tn(v, d + 1)
                elif isinstance(o, list):
                    for i in o:
                        _count_tn(i, d + 1)
            _count_tn(nd_data)
            print(f"Artsy HTML [{name}]: typenames → {dict(list(typenames.items())[:12])}")

            results, seen = [], set()
            def _walk(o, d=0):
                if d > 35 or not o:
                    return
                if isinstance(o, dict):
                    typename = o.get("__typename", "")
                    # Match explicit Artwork or heuristic (title + 2+ artwork fields)
                    is_aw = (typename == "Artwork") or (
                        typename not in ("Artist", "Partner", "Gene", "Tag",
                                         "Fair", "Sale", "HomePage", "SearchCriteriaLabel",
                                         "ArtistGroup", "FilterArtworksConnection",
                                         "ArtworkFilterAggregation") and
                        "title" in o and
                        sum(1 for k in ("medium", "date", "image", "saleMessage",
                                        "internalID", "slug", "availability") if k in o) >= 2
                    )
                    if is_aw:
                        key = o.get("internalID") or o.get("slug") or o.get("title")
                        if key and key not in seen:
                            seen.add(key)
                            med    = o.get("medium") or ""
                            if not valid_medium(med):
                                return
                            partner = o.get("partner") or {}
                            seller  = partner.get("name", "") if isinstance(partner, dict) else ""
                            if is_auction_seller(seller):
                                return
                            price  = ""
                            lp = o.get("listPrice") or {}
                            if isinstance(lp, dict):
                                price = lp.get("display") or ""
                            if not price:
                                price = o.get("saleMessage") or ""
                            dims_obj = o.get("dimensions") or {}
                            dims = ""
                            if isinstance(dims_obj, dict):
                                in_o = dims_obj.get("in") or {}
                                dims = (in_o.get("text") or "") if isinstance(in_o, dict) else ""
                            art_s   = o.get("slug") or ""
                            art_url = f"https://www.artsy.net/artwork/{art_s}" if art_s else url
                            results.append(make_row(
                                name, o.get("title"), o.get("date"),
                                med, dims, price,
                                seller or "Artsy", art_url, "Artsy",
                                _artsy_img(o), False,
                            ))
                    else:
                        for v in o.values():
                            _walk(v, d + 1)
                elif isinstance(o, list):
                    for i in o:
                        _walk(i, d + 1)
            _walk(nd_data)
            print(f"Artsy HTML [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Artsy HTML [{name}]: {e}")
            return []

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

async def scrape_artsy(artists: List[str]) -> List[dict]:
    # Artsy uses direct API — no ScraperAPI constraint, higher concurrency
    sem = asyncio.Semaphore(6)
    async with httpx.AsyncClient(timeout=20) as client:
        batches = await asyncio.gather(*[_artsy_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Artnet — private gallery sales ────────────────────────────────────────────
def _artnet_find_works(obj, depth=0) -> List[dict]:
    if depth > 20 or not obj:
        return []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if (k in ("artworks", "works", "results", "items",
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
            url  = f"https://www.artnet.com/artists/{slug(name)}/"
            resp = await _pget(client, url, headers=HEADERS, follow_redirects=True)
            print(f"Artnet [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
            soup    = BeautifulSoup(resp.text, "html.parser")
            results = []

            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                try:
                    nd = json.loads(nd_tag.string)
                    for aw in _artnet_find_works(nd)[:15]:
                        title = aw.get("title") or aw.get("name") or "Untitled"
                        med   = aw.get("medium") or aw.get("materials") or ""
                        if not valid_medium(med):
                            continue
                        price = aw.get("price") or aw.get("priceDisplay") or aw.get("askingPrice") or ""
                        if isinstance(price, dict):
                            price = price.get("display") or price.get("value") or ""
                        img_r = aw.get("image") or aw.get("imageUrl") or aw.get("thumbnail") or {}
                        img_u = ((img_r.get("url") or img_r.get("src") or "")
                                 if isinstance(img_r, dict) else
                                 (img_r if isinstance(img_r, str) else ""))
                        gallery = aw.get("gallery") or aw.get("partner") or {}
                        seller  = (gallery.get("name") or "") if isinstance(gallery, dict) else str(gallery or "")
                        path    = aw.get("url") or aw.get("href") or aw.get("slug") or ""
                        work_url = path if path.startswith("http") else f"https://www.artnet.com{path}"
                        year    = str(aw.get("year") or aw.get("date") or "")
                        date_l  = aw.get("listedAt") or aw.get("createdAt") or aw.get("updatedAt") or ""
                        results.append(make_row(name, title, year, med, "", str(price),
                                                seller or "Artnet", work_url, "Artnet", img_u, False, date_l))
                except Exception as e:
                    print(f"Artnet [{name}] JSON: {e}")

            # HTML card fallback
            if not results:
                selectors = ("[class*='artwork-card'], [class*='ArtworkCard'], "
                             "[class*='GridItem'], [class*='artwork_card'], "
                             "[class*='WorkCard'], [class*='item-card']")
                for card in soup.select(selectors)[:15]:
                    title = (card.select_one("h2,h3,[class*='title'],[class*='Title']") or {}).get_text(strip=True) or "Untitled"
                    med   = (card.select_one("[class*='medium'],[class*='Medium']") or {}).get_text(strip=True)
                    if not valid_medium(med):
                        continue
                    price   = (card.select_one("[class*='price'],[class*='Price']") or {}).get_text(strip=True)
                    img     = card.select_one("img")
                    img_url = (img.get("src") or img.get("data-src") or "") if img else ""
                    a       = card.select_one("a")
                    href    = (a.get("href") or "") if a else ""
                    src_url = href if href.startswith("http") else f"https://www.artnet.com{href}"
                    results.append(make_row(name, title, "", med, "", price, "",
                                            src_url, "Artnet", img_url, False))

            print(f"Artnet [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Artnet [{name}]: {e}")
            return []

async def scrape_artnet(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(timeout=15) as client:
        batches = await asyncio.gather(*[_artnet_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Seesaw — private gallery sales ───────────────────────────────────────────
async def _seesaw_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> List[dict]:
    async with sem:
        urls = [
            f"https://www.seesaw.website/works?q={urllib.parse.quote(name)}&for_sale=true",
            f"https://www.seesaw.website/search?q={urllib.parse.quote(name)}",
        ]
        for url in urls:
            try:
                resp = await _pget(client, url, headers=HEADERS, follow_redirects=True)
                print(f"Seesaw [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
                if resp.status_code != 200:
                    continue
                soup    = BeautifulSoup(resp.text, "html.parser")
                results = []

                nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
                if nd_tag and nd_tag.string:
                    def _find(o, d=0):
                        if d > 20 or not o:
                            return []
                        if isinstance(o, dict):
                            for k, v in o.items():
                                if k in ("works", "artworks", "results", "items") and isinstance(v, list) and v:
                                    first = v[0] if isinstance(v[0], dict) else {}
                                    if any(x in first for x in ("title", "name", "medium")):
                                        return v
                                sub = _find(v, d + 1)
                                if sub:
                                    return sub
                        elif isinstance(o, list):
                            for i in o:
                                sub = _find(i, d + 1)
                                if sub:
                                    return sub
                        return []
                    for aw in _find(json.loads(nd_tag.string))[:15]:
                        title  = aw.get("title") or aw.get("name") or "Untitled"
                        med    = aw.get("medium") or aw.get("materials") or ""
                        if not valid_medium(med):
                            continue
                        price  = str(aw.get("price") or aw.get("priceDisplay") or "")
                        img_r  = aw.get("image") or aw.get("imageUrl") or aw.get("thumbnail") or {}
                        img_u  = ((img_r.get("url") or img_r.get("src") or "")
                                  if isinstance(img_r, dict) else
                                  (img_r if isinstance(img_r, str) else ""))
                        path   = aw.get("url") or aw.get("href") or aw.get("slug") or ""
                        w_url  = path if path.startswith("http") else f"https://www.seesaw.website{path}"
                        date_l = aw.get("createdAt") or aw.get("publishedAt") or ""
                        results.append(make_row(name, title, "", med, "", price, "",
                                                w_url, "Seesaw", img_u, False, date_l))

                if not results:
                    for card in soup.select("[class*='WorkCard'],[class*='work-card'],article")[:15]:
                        title   = (card.select_one("h2,h3,[class*='title']") or {}).get_text(strip=True) or "Untitled"
                        med     = (card.select_one("[class*='medium']") or {}).get_text(strip=True)
                        if not valid_medium(med):
                            continue
                        price   = (card.select_one("[class*='price']") or {}).get_text(strip=True)
                        img     = card.select_one("img")
                        img_url = img.get("src", "") if img else ""
                        a_tag   = card.select_one("a")
                        href    = (a_tag.get("href") or "") if a_tag else ""
                        src_url = href if href.startswith("http") else f"https://www.seesaw.website{href}"
                        results.append(make_row(name, title, "", med, "", price, "",
                                                src_url, "Seesaw", img_url, False))

                if results:
                    print(f"Seesaw [{name}]: {len(results)} works")
                    return results
            except Exception as e:
                print(f"Seesaw [{name}]: {e}")

        print(f"Seesaw [{name}]: 0 works")
        return []

async def scrape_seesaw(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(timeout=15) as client:
        batches = await asyncio.gather(*[_seesaw_one(client, sem, n) for n in artists])
    return [item for batch in batches for item in batch]

# ── Ocula — gallery / private sales ──────────────────────────────────────────
def _ocula_find_artworks(obj, depth=0) -> List[dict]:
    if depth > 20 or not obj:
        return []
    results = []
    if isinstance(obj, dict):
        if ("title" in obj and
                any(k in obj for k in ("medium", "price", "gallery",
                                       "imageUrl", "image", "askingPrice"))):
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
            resp = await _pget(client, url, headers=HEADERS, follow_redirects=True)
            print(f"Ocula [{name}]: HTTP {resp.status_code} | {len(resp.text)} chars")
            if resp.status_code != 200:
                return []
            soup    = BeautifulSoup(resp.text, "html.parser")
            results = []

            nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if nd_tag and nd_tag.string:
                try:
                    nd = json.loads(nd_tag.string)
                    seen = set()
                    for aw in _ocula_find_artworks(nd)[:20]:
                        title = aw.get("title") or aw.get("name") or "Untitled"
                        key   = f"{name}_{title}"
                        if key in seen:
                            continue
                        seen.add(key)
                        med  = aw.get("medium") or aw.get("materials") or ""
                        if not valid_medium(med):
                            continue
                        price = aw.get("price") or aw.get("priceDisplay") or aw.get("askingPrice") or ""
                        if isinstance(price, dict):
                            price = price.get("display") or price.get("value") or ""
                        img_r = aw.get("image") or aw.get("imageUrl") or aw.get("thumbnail") or {}
                        if isinstance(img_r, list) and img_r:
                            img_r = img_r[0]
                        img_u = ((img_r.get("url") or img_r.get("src") or "")
                                 if isinstance(img_r, dict) else
                                 (img_r if isinstance(img_r, str) and img_r.startswith("http") else ""))
                        gallery = aw.get("gallery") or aw.get("galleries") or aw.get("partner") or {}
                        if isinstance(gallery, list) and gallery:
                            gallery = gallery[0]
                        seller  = (gallery.get("name") or gallery.get("title") or "") if isinstance(gallery, dict) else ""
                        path    = aw.get("url") or aw.get("slug") or aw.get("href") or ""
                        work_url = path if path.startswith("http") else (f"https://ocula.com{path}" if path else url)
                        year    = str(aw.get("year") or aw.get("date") or "")
                        date_l  = aw.get("dateAdded") or aw.get("createdAt") or aw.get("publishedAt") or ""
                        results.append(make_row(name, title, year, str(med), "", str(price),
                                                seller or "Ocula", work_url, "Ocula", img_u, False, date_l))
                except Exception as e:
                    print(f"Ocula [{name}] JSON: {e}")

            if not results:
                for card in soup.select(
                    "[class*='ArtworkCard'],[class*='artwork-card'],"
                    "[class*='WorkCard'],[class*='work-card'],"
                    "[class*='ArtworkItem'],[class*='artwork-item']"
                )[:15]:
                    title = (card.select_one("h2,h3,[class*='title'],[class*='Title']") or {}).get_text(strip=True) or "Untitled"
                    med   = (card.select_one("[class*='medium'],[class*='Medium']") or {}).get_text(strip=True)
                    if not valid_medium(med):
                        continue
                    price = (card.select_one("[class*='price'],[class*='Price']") or {}).get_text(strip=True)
                    img   = card.select_one("img")
                    img_u = (img.get("src") or img.get("data-src") or "") if img else ""
                    a     = card.select_one("a")
                    href  = (a.get("href") or "") if a else ""
                    work_url = href if href.startswith("http") else (f"https://ocula.com{href}" if href else url)
                    date_el  = card.select_one("time,[class*='date'],[datetime]")
                    date_l   = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else ""
                    if title and title != "Untitled":
                        results.append(make_row(name, title, "", med, "", price,
                                                "Ocula", work_url, "Ocula", img_u, False, date_l))

            print(f"Ocula [{name}]: {len(results)} works")
            return results
        except Exception as e:
            print(f"Ocula [{name}]: {e}")
            return []

async def scrape_ocula(artists: List[str]) -> List[dict]:
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(timeout=15) as client:
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
            _set_status(db, "running",
                        f"Scraping {len(artists)} artists · Artsy (API) + "
                        f"Artnet/Seesaw/Ocula (HTML)…")

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
            src_counts = {}
            for item in all_results:
                src_counts[item["source_name"]] = src_counts.get(item["source_name"], 0) + 1
            detail = " · ".join(f"{v} {k}" for k, v in src_counts.items())
            _set_status(db, "done",
                        f"Found {saved} private-sale works — {detail}")
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

app.mount("/", StaticFiles(directory="static", html=True), name="static")
