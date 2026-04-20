"""Re-scrape full descriptions for truncated listings."""

import json
import sqlite3
import sys
import time

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

DB = "zonaprop.db"
DELAY = 2

JS_GET_DESC = """
() => {
    // Click "Leer descripción completa" if present
    const btn = document.querySelector('[class*="description"] button, [class*="Description"] button');
    if (btn) btn.click();

    // Get all text from description area
    const sections = document.querySelectorAll('section, article, div[class*="detail"], div[class*="Description"], div[class*="description"]');
    let best = '';
    sections.forEach(s => {
        const t = s.innerText;
        if (t.length > best.length && t.length < 8000) best = t;
    });

    // Also get structured features
    const feats = {};
    document.querySelectorAll('li, span').forEach(el => {
        const t = el.textContent.trim();
        if (t.match(/m²|frente|fondo|superficie|terreno/i) && t.length < 100) {
            feats[t] = true;
        }
    });

    return {desc: best, features: Object.keys(feats)};
}
"""


def main() -> None:
    conn = sqlite3.connect(DB)

    # Get truncated listings
    rows = conn.execute("""
        SELECT posting_id, url FROM listings
        WHERE tipo='terrenos' AND length(descripcion) = 500
        ORDER BY posting_id
    """).fetchall()

    print(f"Re-scraping {len(rows)} truncated descriptions", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        Stealth().apply_stealth_sync(ctx)
        page = ctx.new_page()

        updated = 0
        errors = 0

        for i, (pid, url) in enumerate(rows):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
                page.wait_for_selector("h1", timeout=10000)
                time.sleep(0.5)

                data = page.evaluate(JS_GET_DESC)
                desc = data.get("desc", "")

                if len(desc) > 500:
                    conn.execute(
                        "UPDATE listings SET descripcion = ? WHERE posting_id = ?",
                        (desc[:5000], pid),
                    )
                    updated += 1

                if i % 50 == 0:
                    conn.commit()
                    print(f"  [{i}/{len(rows)}] updated={updated} errors={errors}",
                          flush=True)

            except Exception as e:
                errors += 1
                if errors > 20 and errors > updated:
                    print(f"Too many errors, stopping", flush=True)
                    break

            time.sleep(DELAY)

        browser.close()

    conn.commit()
    print(f"\nDone. Updated {updated}, errors {errors}")
    conn.close()


if __name__ == "__main__":
    main()
