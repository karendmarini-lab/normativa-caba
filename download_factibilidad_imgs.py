"""Download gallery images from listings with factibilidad keywords.

Uses same Playwright stealth setup as download_remax_imgs.py.
Saves to factibilidad_images/{smp_normalized}/
"""

import os
import sqlite3
import time

import requests
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

DIR = os.path.dirname(os.path.abspath(__file__))
ZP_DB = os.path.join(DIR, "zonaprop.db")
IMG_DIR = os.path.join(DIR, "factibilidad_images")
DELAY = 3

JS_GET_IMAGES = """
() => {
    const imgs = new Set();
    document.querySelectorAll('script').forEach(s => {
        const matches = (s.textContent||'').matchAll(/https?:\\/\\/imgar\\.zonapropcdn\\.com\\/avisos[^"\\s,)]+/g);
        for (const m of matches) imgs.add(m[0].replace(/\\d+x\\d+/, '1500x1500'));
    });
    document.querySelectorAll('img, source').forEach(el => {
        for (const attr of ['src', 'data-src', 'srcset']) {
            const val = el.getAttribute(attr) || '';
            const matches = val.matchAll(/https?:\\/\\/imgar\\.zonapropcdn\\.com\\/avisos[^"\\s,)]+/g);
            for (const m of matches) imgs.add(m[0].replace(/\\d+x\\d+/, '1500x1500'));
        }
    });
    return [...imgs];
}
"""


def main():
    zp = sqlite3.connect(ZP_DB)
    listings = zp.execute("""
        SELECT h.smp_norm, l.url
        FROM haiku_matches h
        JOIN listings l ON h.posting_id = l.posting_id
        WHERE h.confidence >= 0.85
            AND (LOWER(l.descripcion) LIKE '%prefactibilidad%'
                 OR LOWER(l.descripcion) LIKE '%estudio%edif%'
                 OR LOWER(l.descripcion) LIKE '%informe%edif%')
    """).fetchall()
    zp.close()

    # Skip already downloaded (remax_images or factibilidad_images)
    todo = []
    for smp, url in listings:
        smp_dir_r = os.path.join(DIR, "remax_images", smp.replace("/", "_"))
        smp_dir_f = os.path.join(IMG_DIR, smp.replace("/", "_"))
        if os.path.exists(smp_dir_r) and len(os.listdir(smp_dir_r)) > 2:
            continue
        if os.path.exists(smp_dir_f) and len(os.listdir(smp_dir_f)) > 2:
            continue
        todo.append((smp, url))

    print(f"{len(todo)} listings to download (skipped {len(listings)-len(todo)} already done)")
    os.makedirs(IMG_DIR, exist_ok=True)
    session = requests.Session()

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        Stealth().apply_stealth_sync(context)
        page = context.new_page()

        for i, (smp, url) in enumerate(todo):
            smp_dir = os.path.join(IMG_DIR, smp.replace("/", "_"))

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(DELAY)
                imgs = page.evaluate(JS_GET_IMAGES)
                unique = list({u.split("?")[0]: u for u in imgs}.values())
                print(f"  [{i+1}/{len(todo)}] {smp}: {len(unique)} images", end="", flush=True)

                if not unique:
                    print(" (retrying...)", end="", flush=True)
                    time.sleep(10)
                    imgs = page.evaluate(JS_GET_IMAGES)
                    unique = list({u.split("?")[0]: u for u in imgs}.values())
                    print(f" → {len(unique)}", end="", flush=True)

                os.makedirs(smp_dir, exist_ok=True)
                for j, img_url in enumerate(unique):
                    try:
                        resp = session.get(img_url, timeout=10)
                        if resp.status_code == 200 and len(resp.content) > 1000:
                            with open(os.path.join(smp_dir, f"{j}.jpg"), "wb") as f:
                                f.write(resp.content)
                    except Exception:
                        pass
                n_saved = len(os.listdir(smp_dir))
                print(f" → {n_saved} saved", flush=True)

            except Exception as e:
                print(f"  [{i+1}/{len(todo)}] {smp}: ERROR {e}", flush=True)
                time.sleep(5)

        browser.close()

    total = sum(len(os.listdir(os.path.join(IMG_DIR, d)))
                for d in os.listdir(IMG_DIR)
                if os.path.isdir(os.path.join(IMG_DIR, d)))
    dirs = len([d for d in os.listdir(IMG_DIR)
                if os.path.isdir(os.path.join(IMG_DIR, d))])
    print(f"\nDone. {total} images from {dirs} listings in {IMG_DIR}/")


if __name__ == "__main__":
    main()
