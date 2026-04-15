"""
scrape_store.py
---------------
Standalone scraping + saving script for Daraz stores.

Responsibilities:
  - Read store names from shop.txt (one per line)
  - Scrape ALL products from each store (all pages)
  - Upsert scraped products into the SQLite database

What this script does NOT do:
  - No alert logic
  - No Telegram notifications
  - No Celery / background workers
  - Does NOT touch target_price / target_discount (always NULL on insert)
"""

import asyncio
import os
import re
import sqlite3
import sys
import platform
import gc
import random
import time
import traceback
from lxml import html
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from config import (
    BASE_URL,
    DB_FILE,
    DB_SCHEMA,
    MAX_PAGES_PER_SHOP,
    WAIT_BETWEEN_SHOPS,
    ENABLE_MEMORY_MONITORING,
)

# ==================== WINDOWS FIX ====================
if platform.system() == "Windows":
    if sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ==================== ANTI-BAN CONFIG ====================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
]

MIN_PAGE_DELAY = 3
MAX_PAGE_DELAY = 8

# Matches the numeric part of a price like "৳ 1,234", "Tk. 1,234", "Tk1,234"
_PRICE_RE = re.compile(r'(?:৳|Tk\.?)\s*([\d,]+(?:\.\d+)?)', re.IGNORECASE)
# Matches a discount like "-22%", "- 22 %", "22% off" (captures the number)
_DISCOUNT_RE = re.compile(r'-\s*(\d+(?:\.\d+)?)\s*%|(\d+(?:\.\d+)?)\s*%\s*off', re.IGNORECASE)


# ==================== HELPERS ====================

def parse_price(val):
    """
    Parse price strings to float.
    Handles: '৳1,234'  'Tk 1,234'  'Tk. 1,234'  '1,234'  '1234.50'
    Returns None on failure.
    """
    if not val:
        return None
    try:
        cleaned = (
            str(val)
            .replace("৳", "")
            .replace("Tk.", "")
            .replace("Tk", "")
            .replace(",", "")
            .strip()
        )
        return float(cleaned) if cleaned else None
    except Exception:
        return None


def _extract_prices(card):
    """
    Extract (current_price, mrp, discount) from a product card lxml element.

    Strategy:
      - MRP   : full text inside the first <del> element (strikethrough price)
      - Price : first regex match for ৳/Tk in all text OUTSIDE <del> elements
      - Discount: regex search for -XX% anywhere in the card; if absent,
                  calculate from (mrp - price) / mrp * 100
    """
    # ---- MRP from <del> ----
    del_els = card.xpath(".//del")
    mrp = None
    if del_els:
        del_text = del_els[0].text_content()
        m = _PRICE_RE.search(del_text)
        if m:
            mrp = float(m.group(1).replace(",", ""))
        else:
            mrp = parse_price(del_text)

    # ---- Current price from text outside <del> ----
    non_del_parts = [
        t.strip()
        for t in card.xpath(".//text()[not(ancestor::del)]")
        if t.strip()
    ]
    non_del_text = " ".join(non_del_parts)
    current_price = None
    pm = _PRICE_RE.findall(non_del_text)
    if pm:
        current_price = float(pm[0].replace(",", ""))

    # ---- Discount ----
    full_text = " ".join(t.strip() for t in card.xpath(".//text()") if t.strip())
    discount = None
    dm = _DISCOUNT_RE.search(full_text)
    if dm:
        raw = dm.group(1) or dm.group(2)
        try:
            discount = float(raw)
        except Exception:
            pass

    # ---- Fallback: calculate discount from prices ----
    if discount is None and mrp and current_price and mrp > current_price:
        discount = round((mrp - current_price) / mrp * 100, 1)

    return current_price, mrp, discount


def get_memory_usage():
    """Return current process RSS in MB (0 if psutil is not installed)."""
    if not ENABLE_MEMORY_MONITORING:
        return 0
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except Exception:
        return 0


# ==================== DATABASE ====================

def init_db():
    """
    Create product_list table (IF NOT EXISTS) using the schema from config.py.
    Also ensures a UNIQUE index on the url column so upsert works reliably.
    """
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)
    print(f"  📂 Database : {db_path}")

    for attempt in range(3):
        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("BEGIN")
            conn.execute(DB_SCHEMA)
            # Guarantee unique index even on pre-existing databases that were
            # created before the UNIQUE constraint was added to the schema.
            try:
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_product_url"
                    " ON product_list(url)"
                )
            except sqlite3.OperationalError as idx_err:
                # Non-fatal: index creation can fail if duplicate URLs already
                # exist in the table.
                print(f"⚠️  Could not create unique index: {idx_err}")
            conn.execute("COMMIT")
            print(f"✅ Database ready: {db_path}")
            return True
        except Exception as e:
            print(f"❌ DB init error (attempt {attempt + 1}/3): {e}")
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            if attempt < 2:
                time.sleep(5)
            else:
                raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    return False


def save_products_to_db(products, shop_name):
    """
    Upsert *products* into product_list.

    New rows    → INSERT OR IGNORE: target_price / target_discount / send_alert = NULL always.
    Existing rows → UPDATE scraped fields only; target_price etc. are never touched.

    Returns total rows persisted (inserted + updated).
    """
    if not products:
        return 0

    # ------------------------------------------------------------------ #
    # Resolve to absolute path so the script writes to the right file
    # regardless of which directory it is launched from.
    # ------------------------------------------------------------------ #
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)
    print(f"  📂 Database : {db_path}")

    BATCH_SIZE = 100
    total_inserted = 0
    total_updated  = 0
    total_errors   = 0

    conn = None
    try:
        # isolation_level=None  →  autocommit OFF; we drive every BEGIN/COMMIT
        # explicitly.  This avoids Python's implicit transaction management,
        # which can auto-commit at surprising moments (e.g. on PRAGMA or DDL).
        conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cursor = conn.cursor()

        for batch_start in range(0, len(products), BATCH_SIZE):
            batch     = products[batch_start: batch_start + BATCH_SIZE]
            batch_end = batch_start + len(batch)

            print(f"  💾 Rows {batch_start + 1}–{batch_end} / {len(products)}  ({shop_name})")

            b_inserted = 0
            b_updated  = 0
            b_errors   = 0

            cursor.execute("BEGIN")
            try:
                for p in batch:
                    url = p.get("url")
                    if not url:
                        continue

                    try:
                        # ── Step 1: insert only if the URL is new ──────────────
                        # INSERT OR IGNORE leaves the row untouched when it already
                        # exists, so target_price / target_discount / send_alert
                        # set by the alert system are NEVER overwritten here.
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO product_list
                                (title, url, price, MRP, discount, sold,
                                 rating, reviews, location, image,
                                 target_price, target_discount, send_alert)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                            """,
                            (
                                p.get("title"), url,
                                p.get("price"), p.get("MRP"), p.get("discount"),
                                p.get("sold"),  p.get("rating"), p.get("reviews"),
                                p.get("location"), p.get("image"),
                            ),
                        )

                        if cursor.rowcount > 0:
                            # Brand-new row was inserted – nothing more to do.
                            b_inserted += 1
                        else:
                            # ── Step 2: row already existed → refresh scraped fields
                            # target_price / target_discount / send_alert are NOT
                            # in this SET list, so they are preserved exactly.
                            cursor.execute(
                                """
                                UPDATE product_list
                                SET  title    = ?,
                                     price    = ?,
                                     MRP      = ?,
                                     discount = ?,
                                     sold     = ?,
                                     rating   = ?,
                                     reviews  = ?,
                                     location = ?,
                                     image    = ?
                                WHERE url = ?
                                """,
                                (
                                    p.get("title"),
                                    p.get("price"), p.get("MRP"), p.get("discount"),
                                    p.get("sold"),  p.get("rating"), p.get("reviews"),
                                    p.get("location"), p.get("image"),
                                    url,
                                ),
                            )
                            b_updated += 1

                    except Exception as row_err:
                        b_errors += 1
                        print(f"  ❌ Row error  url={url!r}: {row_err}")
                        traceback.print_exc()

                # ── commit the whole batch atomically ──────────────────────
                cursor.execute("COMMIT")

            except Exception as batch_err:
                # If COMMIT itself fails, roll back and surface the error.
                try:
                    cursor.execute("ROLLBACK")
                except Exception:
                    pass
                print(f"  ❌ Batch {batch_start + 1}–{batch_end} failed: {batch_err}")
                traceback.print_exc()
                raise

            total_inserted += b_inserted
            total_updated  += b_updated
            total_errors   += b_errors
            print(
                f"     → inserted={b_inserted}  updated={b_updated}"
                f"  errors={b_errors}"
            )
            gc.collect()

        # ── final verification ─────────────────────────────────────────────
        cursor.execute("SELECT COUNT(*) FROM product_list")
        db_total = cursor.fetchone()[0]
        total = total_inserted + total_updated
        print(
            f"  ✅ {shop_name}: {total_inserted} new rows, "
            f"{total_updated} updated, {total_errors} errors"
        )
        print(f"  📊 Total rows in product_list now: {db_total}")
        return total

    except Exception as e:
        print(f"  ❌ DB error ({shop_name}): {e}")
        traceback.print_exc()
        return 0

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ==================== ASYNC SCRAPING ====================

async def scrape_page(crawler, url, retry_count=0, max_retries=3):
    """
    Scrape a single product-listing page.
    Returns (list[product_dict], last_page_number).
    On failure returns ([], None).
    """
    try:
        # Human-like delay before request
        await asyncio.sleep(random.uniform(2, 5))

        crawl_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            word_count_threshold=10,
            page_timeout=60000,
            simulate_user=True,
            magic=True,
        )

        result = await crawler.arun(url=url, config=crawl_config)

        # Human-like delay after request
        await asyncio.sleep(random.uniform(MIN_PAGE_DELAY, MAX_PAGE_DELAY))

        if not result.success:
            error_msg = result.error_message or "Unknown error"

            if "403" in error_msg or "blocked" in error_msg.lower():
                print("  ⚠️  Possible ban detected! Cooling down 60 s...")
                await asyncio.sleep(60)

            if retry_count < max_retries:
                print(
                    f"  ⚠️  Page failed, retrying"
                    f" ({retry_count + 1}/{max_retries}): {error_msg}"
                )
                await asyncio.sleep(10 * (retry_count + 1))
                return await scrape_page(crawler, url, retry_count + 1, max_retries)

            print(f"  ❌ Page failed after {max_retries} retries: {error_msg}")
            return [], None

        tree = html.fromstring(result.html)
        products = []

        for card in tree.xpath("//div[.//img[@type='product']]"):
            try:
                title_els = card.xpath(
                    ".//img[@type='product']/@alt"
                    " | .//a[@title]/@title"
                    " | .//a[contains(@href,'/products')]/text()"
                )
                url_els = card.xpath(".//a[contains(@href,'/products')]/@href")

                # --- prices via regex-based helper (handles ৳ and Tk variants) ---
                current_price, mrp, discount = _extract_prices(card)

                # Skip cards with no price at all
                if current_price is None:
                    continue

                rating_els = card.xpath(
                    ".//span[contains(@class,'rating')]/text()"
                    " | .//i[contains(@class,'star')]/following-sibling::text()"
                )
                rating = None
                if rating_els:
                    try:
                        rating = float(rating_els[0].strip())
                    except Exception:
                        pass

                reviews_els = card.xpath(
                    ".//span[contains(@class,'review')]/text()"
                    " | .//span[contains(text(),'ratings')]/text()"
                )
                reviews = None
                if reviews_els:
                    try:
                        reviews = int("".join(filter(str.isdigit, reviews_els[0])))
                    except Exception:
                        pass

                image_els = card.xpath(
                    ".//img[@type='product']/@src"
                    " | .//img[@type='product']/@data-src"
                )
                image = None
                if image_els:
                    img_url = image_els[0]
                    if img_url and not img_url.startswith("data:"):
                        image = (
                            img_url
                            if img_url.startswith("http")
                            else (
                                "https:" + img_url
                                if img_url.startswith("//")
                                else None
                            )
                        )

                product_url = (
                    "https:" + url_els[0]
                    if url_els and not url_els[0].startswith("http")
                    else (url_els[0] if url_els else None)
                )

                product = {
                    "title":    title_els[0].strip() if title_els else None,
                    "url":      product_url,
                    "price":    current_price,
                    "MRP":      mrp if mrp else current_price,
                    "discount": discount,
                    "sold":     None,
                    "rating":   rating,
                    "reviews":  reviews,
                    "location": None,
                    "image":    image,
                }

                if product["title"] and product["url"]:
                    products.append(product)

            except Exception as card_err:
                print(f"  ⚠️  Card parse error: {card_err}")
                continue

        # Determine last page number from pagination
        page_numbers = tree.xpath(
            "//li[@title]"
            "[not(contains(@title,'Previous'))]"
            "[not(contains(@title,'Next'))]"
            "/@title"
        )
        last_page = (
            max(int(p) for p in page_numbers if p.isdigit())
            if page_numbers
            else 1
        )

        return products, last_page

    except asyncio.TimeoutError:
        print(f"  ⏱️  Timeout on {url}")
        if retry_count < max_retries:
            await asyncio.sleep(15)
            return await scrape_page(crawler, url, retry_count + 1, max_retries)
        return [], None

    except Exception as e:
        print(f"  ❌ Error scraping {url}: {e}")
        if retry_count < max_retries:
            await asyncio.sleep(10)
            return await scrape_page(crawler, url, retry_count + 1, max_retries)
        return [], None


async def scrape_store_async(store_name):
    """
    Scrape all pages of *store_name*.
    Returns a flat list of product dicts.
    """
    all_products = []
    user_agent = random.choice(USER_AGENTS)

    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
        user_agent=user_agent,
        ignore_https_errors=True,
        text_mode=False,
        light_mode=False,
    )

    for browser_attempt in range(3):
        try:
            print(
                f"  🌐 Starting browser for {store_name}"
                f" (attempt {browser_attempt + 1}/3)..."
            )

            async with AsyncWebCrawler(config=browser_config) as crawler:
                print(f"  ✅ Browser ready — UA: {user_agent[:50]}...")

                # ---- Page 1 ----
                first_url = BASE_URL.format(store_name, 1)
                print(f"  📄 Fetching page 1: {first_url}")
                first_products, last_page = await scrape_page(crawler, first_url)

                if not first_products and last_page is None:
                    raise Exception("Failed to scrape page 1")

                all_products.extend(first_products)

                if last_page > MAX_PAGES_PER_SHOP:
                    print(
                        f"  ⚠️  {store_name} has {last_page} pages,"
                        f" capping at {MAX_PAGES_PER_SHOP}"
                    )
                    last_page = MAX_PAGES_PER_SHOP

                print(
                    f"  📄 {store_name} — page 1/{last_page}"
                    f" — {len(first_products)} products"
                )

                # ---- Pages 2 .. last_page ----
                for page in range(2, last_page + 1):
                    try:
                        mem = get_memory_usage()
                        if mem > 0 and mem > 700:
                            print(
                                f"  ⚠️  Memory high ({mem:.0f} MB),"
                                f" stopping at page {page}"
                            )
                            break

                        page_url = BASE_URL.format(store_name, page)
                        page_products, _ = await scrape_page(crawler, page_url)

                        if page_products:
                            all_products.extend(page_products)
                            print(
                                f"  📄 {store_name} — page {page}/{last_page}"
                                f" — {len(page_products)} products"
                            )
                        else:
                            print(
                                f"  ⚠️  No products on page {page}, continuing..."
                            )

                        # Extra anti-ban break every 10 pages
                        if page % 10 == 0:
                            gc.collect()
                            wait = random.uniform(10, 20)
                            print(f"  ⏸️  Anti-ban pause ({wait:.0f} s)...")
                            await asyncio.sleep(wait)

                    except Exception as page_err:
                        print(f"  ⚠️  Page {page} error: {page_err}, continuing...")
                        continue

            print(f"  ✅ Browser closed for {store_name}")
            return all_products

        except Exception as e:
            print(
                f"  ❌ Browser error (attempt {browser_attempt + 1}/3): {e}"
            )
            if browser_attempt < 2:
                wait = 30 * (browser_attempt + 1)
                print(f"  ⏳ Waiting {wait} s before retry...")
                await asyncio.sleep(wait)
                user_agent = random.choice(USER_AGENTS)
                browser_config.user_agent = user_agent
            else:
                print("  ❌ All browser retries exhausted")
                traceback.print_exc()

    return all_products


# ==================== PUBLIC API ====================

def scrape_and_save(store_name):
    """
    Scrape all products from *store_name* and save them to the database.

    Can also be used as a module entry point:
        from scrape_store import scrape_and_save
        saved = scrape_and_save("apex")

    Returns the number of rows inserted / updated.
    """
    print(f"\n{'='*60}")
    print(f"🪧  Scraping store : {store_name}")
    print(f"💾  Memory         : {get_memory_usage():.0f} MB")
    print(f"{'='*60}")

    # Run async scrape in a dedicated event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Windows requires ProactorEventLoop for subprocess / browser support
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        if not isinstance(loop, asyncio.ProactorEventLoop):
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)

    try:
        products = loop.run_until_complete(scrape_store_async(store_name))
    finally:
        try:
            loop.close()
        except Exception:
            pass

    gc.collect()

    if not products:
        print(f"⚠️  No products scraped for {store_name}")
        return 0

    print(f"✅ Scraped {len(products)} products from {store_name}")

    saved = save_products_to_db(products, store_name)

    print(f"💾  Memory after save: {get_memory_usage():.0f} MB")
    return saved


# ==================== STANDALONE RUNNER ====================

def get_shops(path="shop.txt"):
    """
    Load shop names from *path*, one per line.
    Lines starting with # and blank lines are ignored.
    """
    for attempt in range(3):
        try:
            with open(path, "r", encoding="utf-8") as f:
                shops = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]
            if not shops:
                print(f"⚠️  {path} is empty!")
                return []
            print(f"✅ Loaded {len(shops)} shops from {path}")
            return shops
        except FileNotFoundError:
            print(f"❌ {path} not found (attempt {attempt + 1}/3)")
            if attempt < 2:
                time.sleep(5)
            else:
                return []
        except Exception as e:
            print(f"❌ Error reading {path}: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                return []
    return []


def main():
    print("=" * 60)
    print("🔍  DARAZ STORE SCRAPER  (standalone mode)")
    print("=" * 60)
    print(f"⏰  Started : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Initialise / verify database
    print("\n🔧  Initialising database...")
    init_db()

    shops = get_shops()
    if not shops:
        print("❌  No shops to scrape. Add store names to shop.txt and retry.")
        sys.exit(1)

    total_shops = len(shops)
    total_saved = 0
    failed_shops = []

    for i, shop in enumerate(shops, 1):
        print(f"\n{'▀'*60}")
        print(f"▀  SHOP {i}/{total_shops}: {shop}")
        print(f"{'▀'*60}")

        try:
            saved = scrape_and_save(shop)
            total_saved += saved

        except KeyboardInterrupt:
            print("\n⛔  Interrupted by user")
            break

        except Exception as e:
            print(f"❌  Error scraping {shop}: {e}")
            traceback.print_exc()
            failed_shops.append(shop)

        # Brief pause between shops (anti-ban)
        if i < total_shops:
            print(f"\n⏳  Waiting {WAIT_BETWEEN_SHOPS} s before next shop...")
            time.sleep(WAIT_BETWEEN_SHOPS)

    print(f"\n{'='*60}")
    print("✅  SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"🪧  Shops processed : {total_shops - len(failed_shops)}/{total_shops}")
    print(f"📦  Products saved  : {total_saved}")
    if failed_shops:
        print(f"❌  Failed shops    : {', '.join(failed_shops)}")
    print(f"⏰  Finished        : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
