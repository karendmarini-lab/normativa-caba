"""Scrape Zonaprop listings in CABA and store in SQLite.

Usage:
    python scrape_zonaprop.py                           # terrenos-venta (default)
    python scrape_zonaprop.py departamentos venta
    python scrape_zonaprop.py casas alquiler
    python scrape_zonaprop.py departamentos,casas,terrenos venta,alquiler  # batch
"""

import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

DB_PATH = Path("zonaprop.db")
DELAY_BETWEEN_PAGES = 3


def log(msg: str) -> None:
    print(msg, flush=True)


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            posting_id   TEXT PRIMARY KEY,
            tipo         TEXT NOT NULL,
            operacion    TEXT NOT NULL,
            precio_usd   REAL,
            precio_raw   TEXT,
            superficie_m2 REAL,
            direccion    TEXT,
            barrio       TEXT,
            descripcion  TEXT,
            url          TEXT,
            imagenes     TEXT,
            fecha_scrape TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_tipo ON listings(tipo)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_barrio ON listings(barrio)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_listings_operacion ON listings(operacion)"
    )
    conn.commit()


def extract_posting_id(url: str) -> str | None:
    match = re.search(r"-(\d{6,})\.html", url)
    return match.group(1) if match else None


def parse_precio_usd(raw: str) -> float | None:
    raw = raw.strip()
    if not raw.upper().startswith("USD"):
        return None
    nums = re.sub(r"[^\d]", "", raw)
    return float(nums) if nums else None


def parse_superficie(raw: str) -> float | None:
    match = re.match(r"([\d.,]+)\s*m", raw)
    if not match:
        return None
    return float(match.group(1).replace(".", "").replace(",", "."))


JS_EXTRACT = """
() => {
  const cards = document.querySelectorAll('[data-qa="posting PROPERTY"]');
  return Array.from(cards).map(card => {
    const link = card.querySelector('a[href*="/propiedades/clasificado/"]');
    const headings = Array.from(card.querySelectorAll('h2, h3, h4'));
    const imgs = Array.from(card.querySelectorAll('img'))
      .map(i => i.src)
      .filter(s => s && (s.includes('zonapropcdn') || s.includes('naventcdn')));
    return {
      url: link ? link.href : null,
      texts: headings.map(h => h.textContent.trim()),
      imagenes: imgs
    };
  });
}
"""


def parse_listing(raw: dict, tipo: str, operacion: str) -> dict | None:
    url = raw.get("url")
    if not url:
        return None
    posting_id = extract_posting_id(url)
    if not posting_id:
        return None

    texts = raw.get("texts", [])
    if len(texts) < 4:
        return None

    precio_raw = texts[0]
    idx = 1
    if idx < len(texts) and (
        "expensas" in texts[idx].lower() or texts[idx].startswith("$")
    ):
        idx += 1

    superficie_raw = texts[idx] if idx < len(texts) else ""
    idx += 1
    direccion = texts[idx] if idx < len(texts) else ""
    idx += 1
    barrio = texts[idx] if idx < len(texts) else ""
    idx += 1
    descripcion = texts[idx][:500] if idx < len(texts) else ""

    return {
        "posting_id": posting_id,
        "tipo": tipo,
        "operacion": operacion,
        "precio_usd": parse_precio_usd(precio_raw),
        "precio_raw": precio_raw,
        "superficie_m2": parse_superficie(superficie_raw),
        "direccion": direccion,
        "barrio": barrio,
        "descripcion": descripcion,
        "url": url.split("?")[0],
        "imagenes": json.dumps(raw.get("imagenes", [])),
    }


def scrape_page(page, base_url: str, page_num: int) -> list[dict]:
    url = f"{base_url}.html" if page_num == 1 else f"{base_url}-pagina-{page_num}.html"
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    try:
        page.wait_for_selector('[data-qa="posting PROPERTY"]', timeout=15000)
    except Exception:
        log("  Cloudflare challenge, waiting...")
        time.sleep(10)
        page.wait_for_selector('[data-qa="posting PROPERTY"]', timeout=30000)
    return page.evaluate(JS_EXTRACT)


def save_listings(conn: sqlite3.Connection, listings: list[dict]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for lst in listings:
        conn.execute(
            """INSERT INTO listings
               (posting_id, tipo, operacion, precio_usd, precio_raw,
                superficie_m2, direccion, barrio, descripcion, url,
                imagenes, fecha_scrape)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(posting_id) DO UPDATE SET
                precio_usd=excluded.precio_usd, precio_raw=excluded.precio_raw,
                superficie_m2=excluded.superficie_m2, imagenes=excluded.imagenes,
                fecha_scrape=excluded.fecha_scrape
            """,
            (
                lst["posting_id"], lst["tipo"], lst["operacion"],
                lst["precio_usd"], lst["precio_raw"], lst["superficie_m2"],
                lst["direccion"], lst["barrio"], lst["descripcion"],
                lst["url"], lst["imagenes"], now,
            ),
        )
        count += 1
    conn.commit()
    return count


def get_total_pages(page) -> int:
    h1 = page.query_selector("h1")
    if not h1:
        return 1
    text = h1.text_content()
    match = re.search(r"([\d.]+)", text.replace(".", ""))
    if not match:
        return 1
    total = int(match.group(1))
    return (total + 29) // 30


def scrape_category(page, conn, tipo: str, operacion: str) -> None:
    """Scrape all pages for a given tipo+operacion combo."""
    base_url = f"https://www.zonaprop.com.ar/{tipo}-{operacion}-capital-federal"
    label = f"{tipo}/{operacion}"

    log(f"\n=== {label} ===")
    raw = scrape_page(page, base_url, 1)
    total_pages = get_total_pages(page)
    listings = [parse_listing(r, tipo, operacion) for r in raw]
    listings = [l for l in listings if l]
    save_listings(conn, listings)
    log(f"[{label}] Page 1/{total_pages}: {len(listings)} saved")

    for pg in range(2, total_pages + 1):
        time.sleep(DELAY_BETWEEN_PAGES)
        try:
            raw = scrape_page(page, base_url, pg)
            listings = [parse_listing(r, tipo, operacion) for r in raw]
            listings = [l for l in listings if l]
            save_listings(conn, listings)
            log(f"[{label}] Page {pg}/{total_pages}: {len(listings)} saved")
        except Exception as e:
            log(f"[{label}] Page {pg}/{total_pages}: ERROR - {e}")
            time.sleep(10)


def main() -> None:
    tipos = sys.argv[1].split(",") if len(sys.argv) > 1 else ["terrenos"]
    ops = sys.argv[2].split(",") if len(sys.argv) > 2 else ["venta"]

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        Stealth().apply_stealth_sync(context)
        page = context.new_page()

        for tipo in tipos:
            for op in ops:
                scrape_category(page, conn, tipo.strip(), op.strip())

        browser.close()

    # Summary
    for row in conn.execute(
        "SELECT tipo, operacion, COUNT(*) FROM listings GROUP BY tipo, operacion"
    ):
        log(f"  {row[0]}/{row[1]}: {row[2]} listings")
    conn.close()


if __name__ == "__main__":
    main()
