"""
scrape_store.py
---------------
Standalone scraping module for Daraz price monitor.

Responsibilities:
  - Read store names from shop.txt
  - Scrape all products from each store
  - Save new products / update existing product data in the database

What it does NOT do:
  - No alert logic
  - No Telegram notifications
  - No Celery tasks
"""

import asyncio
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


# ==================== HELPERS ====================

def parse_price(val):
    """Parse a price string like '৳1,234' to float."""
    try:
        return float(val.replace("৳", "").replace(",", "").strip())
    except Exception:
        return None


def get_memory_usage():
    """Return current process memory in MB (0 if psutil unavailable)."""
    if not ENABLE_MEMORY_MONITORING:
        return 0
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except Exception:
        return 0


# ==================== DATABASE ====================

def init_db():
    """Create the product_list table if it doesn't exist, and ensure url is unique."""
    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE, timeout=30)
            conn.execute(DB_SCHEMA)
            # Ensure the unique index exists even on pre-existing databases
            # (CREATE TABLE IF NOT EXISTS won't add a constraint to an old table)
            try:
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_product_url ON product_list(url)"
                )
            except sqlite3.OperationalError as idx_err:
                # Index may fail if there are already duplicate URLs — not fatal
                print(f"⚠️ Could not create unique index (duplicate urls?): {idx_err}")
            conn.commit()
            print(f"✅ Database ready: {DB_FILE}")
            return True
        except Exception as e:
            print(f"❌ DB init error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
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
    Upsert all scraped products into the database.

    - New products  → INSERT with scraped fields; target_price, target_discount,
                      and send_alert are left as NULL so the alert system can
                      set them later.
    - Known products → UPDATE only the fields that come from scraping (price,
                       MRP, discount, sold, rating, reviews, location, image,
                       title).  target_price / target_discount / send_alert are
                       intentionally preserved.

    Returns the number of rows saved/updated.
    """
    if not products:
        return 0

    BATCH_SIZE = 100
    saved = 0

    max_retries = 3
    for db_attempt in range(max_retries):
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.cursor()

            for batch_start in range(0, len(products), BATCH_SIZE):
                batch = products[batch_start: batch_start + BATCH_SIZE]
                print(
                    f"💾 Saving batch {batch_start}–{batch_start + len(batch)}"
                    f" / {len(products)} ({shop_name})"
                )

                for p in batch:
                    if not p.get("url"):
                        continue
                    try:
                        # Manual upsert: works even if the UNIQUE index is
                        # missing on an older database file.
                        cursor.execute(
                            "SELECT id FROM product_list WHERE url = ?",
                            (p["url"],),
                        )
                        row = cursor.fetchone()

                        if row:
                            # Update only the scraped fields; preserve user
                            # settings (target_price, target_discount, send_alert).
                            cursor.execute(
                                """
                                UPDATE product_list
                                SET title    = ?,
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
                                    p.get("price"),
                                    p.get("MRP"),
                                    p.get("discount"),
                                    p.get("sold"),
                                    p.get("rating"),
                                    p.get("reviews"),
                                    p.get("location"),
                                    p.get("image"),
                                    p["url"],
                                ),
                            )
                        else:
                            cursor.execute(
                                """
                                INSERT INTO product_list
                                    (title, url, price, MRP, discount, sold,
                                     rating, reviews, location, image,
                                     target_price, target_discount, send_alert)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                                """,
                                (
                                    p.get("title"),
                                    p["url"],
                                    p.get("price"),
                                    p.get("MRP"),
                                    p.get("discount"),
                                    p.get("sold"),
                                    p.get("rating"),
                                    p.get("reviews"),
                                    p.get("location"),
                                    p.get("image"),
                                ),
                            )
                        saved += 1
                    except Exception as e:
                        print(f"⚠️ DB error for {p.get('url', '?')}: {e}")
                        traceback.print_exc()
                        continue

                conn.commit()
                gc.collect()

            print(f"✅ {shop_name}: {saved} products saved/updated in DB")
            return saved

        except sqlite3.OperationalError as e:
            print(f"❌ DB operational error (attempt {db_attempt + 1}): {e}")
            if db_attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    return saved


# ==================== ASYNC SCRAPING ====================

async def scrape_page(crawler, url, retry_count=0, max_retries=3):
    """Scrape a single listing page; returns (products list, last_page int)."""
    try:
        await asyncio.sleep(random.uniform(2, 5))

        crawl_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            word_count_threshold=10,
            page_timeout=45000,
            wait_for="css:img[type='product']",
            simulate_user=True,
            magic=True,
        )

        result = await crawler.arun(url=url, config=crawl_config)

        await asyncio.sleep(random.uniform(MIN_PAGE_DELAY, MAX_PAGE_DELAY))

        if not result.success:
            error_msg = result.error_message or "Unknown error"

            if "403" in error_msg or "blocked" in error_msg.lower():
                print("⚠️ Possible ban detected! Waiting 60 seconds...")
                await asyncio.sleep(60)

            if retry_count < max_retries:
                print(f"⚠️ Failed, retrying ({retry_count + 1}/{max_retries}): {error_msg}")
                await asyncio.sleep(10 * (retry_count + 1))
                return await scrape_page(crawler, url, retry_count + 1, max_retries)

            print(f"❌ Failed after {max_retries} retries: {error_msg}")
            return [], None

        tree = html.fromstring(result.html)
        products = []

        for card in tree.xpath("//div[.//img[@type='product']]"):
            try:
                title = card.xpath(
                    ".//img[@type='product']/@alt"
                    " | .//a[@title]/@title"
                    " | .//a[contains(@href,'/products')]/text()"
                )
                url_ = card.xpath(".//a[contains(@href,'/products')]/@href")
                price = card.xpath(".//span[starts-with(normalize-space(.),'৳')]/text()")

                mrp_elements = card.xpath(".//del/text() | .//span[@class='old-price']/text()")
                mrp = parse_price(mrp_elements[0]) if mrp_elements else None

                discount_elements = card.xpath(
                    ".//span[contains(@class,'discount')]/text()"
                    " | .//span[contains(text(),'%')]/text()"
                )
                discount = None
                if discount_elements:
                    discount_text = discount_elements[0].strip().replace("-", "").replace("%", "")
                    try:
                        discount = float(discount_text)
                    except Exception:
                        pass

                rating_elements = card.xpath(
                    ".//span[contains(@class,'rating')]/text()"
                    " | .//i[contains(@class,'star')]/following-sibling::text()"
                )
                rating = None
                if rating_elements:
                    try:
                        rating = float(rating_elements[0].strip())
                    except Exception:
                        pass

                reviews_elements = card.xpath(
                    ".//span[contains(@class,'review')]/text()"
                    " | .//span[contains(text(),'ratings')]/text()"
                )
                reviews = None
                if reviews_elements:
                    try:
                        reviews = int("".join(filter(str.isdigit, reviews_elements[0])))
                    except Exception:
                        pass

                image_elements = card.xpath(
                    ".//img[@type='product']/@src"
                    " | .//img[@type='product']/@data-src"
                )
                image = None
                if image_elements:
                    img_url = image_elements[0]
                    if img_url and not img_url.startswith("data:"):
                        image = (
                            img_url if img_url.startswith("http")
                            else ("https:" + img_url if img_url.startswith("//") else None)
                        )

                current_price = parse_price(price[0]) if price else None

                product = {
                    "title":    title[0].strip() if title else None,
                    "url":      ("https:" + url_[0] if url_ and not url_[0].startswith("http")
                                 else (url_[0] if url_ else None)),
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

            except Exception as e:
                print(f"⚠️ Error parsing product card: {e}")
                continue

        pages = tree.xpath(
            "//li[@title][not(contains(@title,'Previous'))"
            " and not(contains(@title,'Next'))]/@title"
        )
        last_page = max(int(p) for p in pages if p.isdigit()) if pages else 1

        return products, last_page

    except asyncio.TimeoutError:
        print(f"⏱️ Timeout on {url}, retrying...")
        if retry_count < max_retries:
            await asyncio.sleep(15)
            return await scrape_page(crawler, url, retry_count + 1, max_retries)
        return [], None

    except Exception as e:
        print(f"❌ Error scraping page {url}: {e}")
        if retry_count < max_retries:
            print(f"🔄 Retrying ({retry_count + 1}/{max_retries})...")
            await asyncio.sleep(10)
            return await scrape_page(crawler, url, retry_count + 1, max_retries)
        return [], None


async def scrape_store_async(store_name):
    """Scrape all pages of a store; returns a flat list of product dicts."""
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

    max_browser_retries = 3

    for browser_attempt in range(max_browser_retries):
        try:
            print(f"🌐 Initializing browser for {store_name} (attempt {browser_attempt + 1})...")

            async with AsyncWebCrawler(config=browser_config) as crawler:
                print(f"✅ Browser ready — UA: {user_agent[:50]}...")

                first_url = BASE_URL.format(store_name, 1)
                print(f"📄 Fetching page 1: {first_url}")

                first_products, last_page = await scrape_page(crawler, first_url)

                if not first_products and last_page is None:
                    raise Exception("Failed to scrape first page")

                all_products.extend(first_products)

                if last_page > MAX_PAGES_PER_SHOP:
                    print(f"⚠️ {store_name} has {last_page} pages, limiting to {MAX_PAGES_PER_SHOP}")
                    last_page = MAX_PAGES_PER_SHOP

                print(f"📄 {store_name} — page 1/{last_page} — {len(first_products)} products")

                for page in range(2, last_page + 1):
                    try:
                        mem_usage = get_memory_usage()
                        if mem_usage > 0 and mem_usage > 700:
                            print(f"⚠️ Memory high ({mem_usage:.0f}MB), stopping at page {page}")
                            break

                        url = BASE_URL.format(store_name, page)
                        products, _ = await scrape_page(crawler, url)

                        if products:
                            all_products.extend(products)
                            print(
                                f"📄 {store_name} — page {page}/{last_page}"
                                f" — {len(products)} products"
                            )
                        else:
                            print(f"⚠️ No products on page {page}, continuing...")

                        if page % 10 == 0:
                            gc.collect()
                            print("⏸️ Anti-ban break...")
                            await asyncio.sleep(random.uniform(10, 20))

                    except Exception as page_error:
                        print(f"⚠️ Error on page {page}: {page_error}, continuing...")
                        continue

            print(f"✅ Browser closed for {store_name}")
            return all_products

        except Exception as e:
            print(
                f"❌ Browser error for {store_name}"
                f" (attempt {browser_attempt + 1}/{max_browser_retries}): {e}"
            )
            if browser_attempt < max_browser_retries - 1:
                wait_time = 30 * (browser_attempt + 1)
                print(f"⏳ Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)
                user_agent = random.choice(USER_AGENTS)
                browser_config.user_agent = user_agent
            else:
                print(f"❌ Failed after {max_browser_retries} browser attempts")
                traceback.print_exc()

    return all_products


# ==================== PUBLIC API ====================

def scrape_and_save(store_name):
    """
    Scrape all products from *store_name* and save them to the database.
    Returns the number of products saved/updated.

    This is the main entry point when using scrape_store as a module:

        from scrape_store import scrape_and_save
        count = scrape_and_save("apex")
    """
    print(f"\n{'='*60}")
    print(f"🪧 Scraping: {store_name}")
    print(f"💾 Memory: {get_memory_usage():.0f}MB")
    print(f"{'='*60}")

    # Run async scrape in a fresh event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

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
        print(f"⚠️ No products scraped for {store_name}")
        return 0

    print(f"✅ Scraped {len(products)} products from {store_name}")
    saved = save_products_to_db(products, store_name)

    print(f"💾 Memory after save: {get_memory_usage():.0f}MB")
    return saved


# ==================== STANDALONE RUNNER ====================

def get_shops(path="shop.txt"):
    """Load shop names from *path*, skipping blank lines and # comments."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                shops = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]
            if not shops:
                print("⚠️ shop.txt is empty!")
                return []
            print(f"✅ Loaded {len(shops)} shops from {path}")
            return shops
        except FileNotFoundError:
            print(f"❌ {path} not found (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return []
        except Exception as e:
            print(f"❌ Error reading {path}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return []
    return []


def main():
    print("=" * 60)
    print("🔍 DARAZ STORE SCRAPER")
    print("=" * 60)
    print(f"⏰ Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Initialise DB
    print("\n🔧 Initialising database...")
    init_db()

    shops = get_shops()
    if not shops:
        print("❌ No shops to scrape. Add store names to shop.txt and try again.")
        sys.exit(1)

    total_shops = len(shops)
    total_saved = 0
    failed_shops = []

    for i, shop in enumerate(shops, 1):
        print(f"\n{'▀'*60}")
        print(f"▀ SHOP {i}/{total_shops}: {shop}")
        print(f"{'▀'*60}")

        try:
            saved = scrape_and_save(shop)
            total_saved += saved
        except KeyboardInterrupt:
            print("\n⛔ Interrupted by user")
            break
        except Exception as e:
            print(f"❌ Error scraping {shop}: {e}")
            traceback.print_exc()
            failed_shops.append(shop)

        # Brief pause between shops (anti-ban)
        if i < total_shops:
            from config import WAIT_BETWEEN_SHOPS
            print(f"\n⏳ Waiting {WAIT_BETWEEN_SHOPS}s before next shop...")
            time.sleep(WAIT_BETWEEN_SHOPS)

    print(f"\n{'='*60}")
    print("✅ SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"🪧 Shops processed : {total_shops - len(failed_shops)}/{total_shops}")
    print(f"📦 Products saved  : {total_saved}")
    if failed_shops:
        print(f"❌ Failed shops    : {', '.join(failed_shops)}")
    print(f"⏰ Finished        : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
