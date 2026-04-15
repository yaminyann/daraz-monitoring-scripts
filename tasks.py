import asyncio
import sqlite3
import requests
import sys
import platform
import gc
import random
import time
from lxml import html
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from celery import Celery
from config import (
    BASE_URL, 
    TELEGRAM_BOT_TOKEN, 
    TELEGRAM_CHAT_IDS, 
    DB_FILE,
    DB_SCHEMA,
    WAIT_BETWEEN_PAGES,
    REDIS_URL,
    MAX_PAGES_PER_SHOP,
    ENABLE_MEMORY_MONITORING
)

# ==================== WINDOWS FIX ====================
if platform.system() == 'Windows':
    if sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Initialize Celery app
celery_app = Celery('daraz_monitor', broker=REDIS_URL, backend=REDIS_URL)

# ==================== ANTI-BAN CONFIG ====================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
]

# Random delays to appear human-like
MIN_PAGE_DELAY = 3
MAX_PAGE_DELAY = 8

# ==================== CELERY CONFIG ====================
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Dhaka',
    enable_utc=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=1,
    worker_max_tasks_per_child=20,
    result_expires=1800,
    broker_connection_retry_on_startup=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3300,
    worker_pool='solo',
    worker_max_memory_per_child=800000,
    task_autoretry_for=(Exception,),
    task_retry_kwargs={'max_retries': 3, 'countdown': 60},
    task_default_retry_delay=60,
)

# ==================== HELPER FUNCTIONS ====================

def init_db():
    """Initialize database with correct schema"""
    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE, timeout=30)
            cursor = conn.cursor()
            cursor.execute(DB_SCHEMA)
            conn.commit()
            print(f"✅ Database initialized: {DB_FILE}")
            return True
        except Exception as e:
            print(f"❌ Database init error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    return False

def parse_price(val):
    """Parse price string to float"""
    try:
        return float(val.replace("৳", "").replace(",", "").strip())
    except:
        return None

def send_telegram(msg: str, max_retries=3):
    """Send message to all Telegram chat IDs with retry"""
    for attempt in range(max_retries):
        success = False
        for chat_id in TELEGRAM_CHAT_IDS:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            try:
                response = requests.post(
                    url, 
                    data={"chat_id": chat_id, "text": msg}, 
                    timeout=15
                )
                if response.status_code == 200:
                    print(f"📩 Alert sent to {chat_id}")
                    success = True
            except Exception as e:
                print(f"⚠ Telegram send error to {chat_id} (attempt {attempt+1}): {e}")
        
        if success:
            return True
        
        if attempt < max_retries - 1:
            time.sleep(5)
    
    return False

def get_memory_usage():
    """Get current memory usage in MB"""
    if not ENABLE_MEMORY_MONITORING:
        return 0
    
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except:
        return 0

# ==================== ASYNC SCRAPING FUNCTIONS ====================

async def scrape_page(crawler, url, retry_count=0, max_retries=3):
    """Scrape a single page with retry logic"""
    try:
        # Random delay before request (anti-ban)
        await asyncio.sleep(random.uniform(2, 5))
        
        # According to Crawl4AI v0.7.x docs - proper CrawlerRunConfig
        crawl_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,          # Always fresh data
            word_count_threshold=10,               # Keep short content
            page_timeout=45000,                    # 45 second timeout
            wait_for="css:img[type='product']",    # Wait for products to load
            simulate_user=True,                    # Anti-bot: simulate human behavior
            magic=True,                            # Anti-bot: additional stealth
        )
        
        result = await crawler.arun(url=url, config=crawl_config)
        
        # Random delay after request (anti-ban)
        await asyncio.sleep(random.uniform(MIN_PAGE_DELAY, MAX_PAGE_DELAY))

        if not result.success:
            error_msg = result.error_message or "Unknown error"
            
            # Check if blocked/banned
            if "403" in error_msg or "blocked" in error_msg.lower():
                print(f"⚠️ Possible ban detected! Waiting 60 seconds...")
                await asyncio.sleep(60)
            
            if retry_count < max_retries:
                print(f"⚠️ Failed, retrying ({retry_count+1}/{max_retries}): {error_msg}")
                await asyncio.sleep(10 * (retry_count + 1))  # Exponential backoff
                return await scrape_page(crawler, url, retry_count + 1, max_retries)
            
            print(f"❌ Failed after {max_retries} retries: {error_msg}")
            return [], None

        tree = html.fromstring(result.html)
        products = []

        for card in tree.xpath("//div[.//img[@type='product']]"):
            try:
                title = card.xpath(".//img[@type='product']/@alt | .//a[@title]/@title | .//a[contains(@href,'/products')]/text()")
                url_ = card.xpath(".//a[contains(@href,'/products')]/@href")
                price = card.xpath(".//span[starts-with(normalize-space(.),'৳')]/text()")
                
                mrp_elements = card.xpath(".//del/text() | .//span[@class='old-price']/text()")
                mrp = parse_price(mrp_elements[0]) if mrp_elements else None
                
                discount_elements = card.xpath(".//span[contains(@class,'discount')]/text() | .//span[contains(text(),'%')]/text()")
                discount = None
                if discount_elements:
                    discount_text = discount_elements[0].strip().replace('-', '').replace('%', '')
                    try:
                        discount = float(discount_text)
                    except:
                        pass
                
                rating_elements = card.xpath(".//span[contains(@class,'rating')]/text() | .//i[contains(@class,'star')]/following-sibling::text()")
                rating = None
                if rating_elements:
                    try:
                        rating = float(rating_elements[0].strip())
                    except:
                        pass
                
                reviews_elements = card.xpath(".//span[contains(@class,'review')]/text() | .//span[contains(text(),'ratings')]/text()")
                reviews = None
                if reviews_elements:
                    try:
                        reviews = int(''.join(filter(str.isdigit, reviews_elements[0])))
                    except:
                        pass
                
                image_elements = card.xpath(".//img[@type='product']/@src | .//img[@type='product']/@data-src")
                image = None
                if image_elements:
                    img_url = image_elements[0]
                    if img_url and not img_url.startswith('data:'):
                        image = img_url if img_url.startswith('http') else ('https:' + img_url if img_url.startswith('//') else None)
                
                current_price = parse_price(price[0]) if price else None

                product = {
                    "title": title[0].strip() if title else None,
                    "url": "https:" + url_[0] if url_ and not url_[0].startswith('http') else (url_[0] if url_ else None),
                    "price": current_price,
                    "MRP": mrp if mrp else current_price,
                    "discount": discount,
                    "sold": None,
                    "rating": rating,
                    "reviews": reviews,
                    "location": None,
                    "image": image,
                }

                if product["title"] and product["url"]:
                    products.append(product)
            except Exception as e:
                print(f"⚠️ Error parsing product card: {e}")
                continue

        pages = tree.xpath("//li[@title][not(contains(@title,'Previous')) and not(contains(@title,'Next'))]/@title")
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
            print(f"🔄 Retrying ({retry_count+1}/{max_retries})...")
            await asyncio.sleep(10)
            return await scrape_page(crawler, url, retry_count + 1, max_retries)
        return [], None

async def scrape_store_async(store_name):
    """Async function to scrape all pages of a store with anti-ban measures"""
    all_products = []
    
    # Random user agent for anti-ban
    user_agent = random.choice(USER_AGENTS)
    
    # According to Crawl4AI v0.7.x docs - proper BrowserConfig
    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
        user_agent=user_agent,
        ignore_https_errors=True,
        text_mode=False,              # Keep images
        light_mode=False,             # Full rendering
    )

    max_browser_retries = 3
    
    for browser_attempt in range(max_browser_retries):
        try:
            print(f"🌐 Initializing browser for {store_name} (attempt {browser_attempt+1})...")
            
            async with AsyncWebCrawler(config=browser_config) as crawler:
                print(f"✅ Browser initialized with user agent: {user_agent[:50]}...")
                
                # Scrape first page
                first_url = BASE_URL.format(store_name, 1)
                print(f"📄 Fetching page 1: {first_url}")
                
                first_products, last_page = await scrape_page(crawler, first_url)
                
                if not first_products and last_page is None:
                    raise Exception("Failed to scrape first page")
                
                all_products.extend(first_products)

                # Limit max pages
                if last_page > MAX_PAGES_PER_SHOP:
                    print(f"⚠️ {store_name} has {last_page} pages, limiting to {MAX_PAGES_PER_SHOP}")
                    last_page = MAX_PAGES_PER_SHOP

                print(f"📄 {store_name} - Page 1/{last_page} - {len(first_products)} products")

                # Scrape remaining pages
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
                            print(f"📄 {store_name} - Page {page}/{last_page} - {len(products)} products")
                        else:
                            print(f"⚠️ No products on page {page}, continuing...")
                        
                        if page % 10 == 0:
                            gc.collect()
                            # Extra delay every 5 pages (anti-ban)
                            print(f"⏸️ Taking a break (anti-ban)...")
                            await asyncio.sleep(random.uniform(10, 20))
                            
                    except Exception as page_error:
                        print(f"⚠️ Error on page {page}: {page_error}, continuing to next page...")
                        continue

            print(f"✅ Browser closed for {store_name}")
            return all_products
        
        except Exception as e:
            print(f"❌ Browser error for {store_name} (attempt {browser_attempt+1}/{max_browser_retries}): {e}")
            
            if browser_attempt < max_browser_retries - 1:
                wait_time = 30 * (browser_attempt + 1)
                print(f"⏳ Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)
                
                # Try different user agent
                user_agent = random.choice(USER_AGENTS)
                browser_config.user_agent = user_agent
            else:
                print(f"❌ Failed after {max_browser_retries} browser attempts")
                import traceback
                traceback.print_exc()
    
    return all_products

# ==================== CELERY TASKS ====================

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5, 'countdown': 120})
def scrape_store_task(self, store_name):
    """Celery task to scrape a store with auto-retry"""
    try:
        print(f"\n{'='*60}")
        print(f"🪧 Starting scrape for: {store_name}")
        print(f"📊 Task attempt: {self.request.retries + 1}")
        print(f"💾 Memory: {get_memory_usage():.0f}MB")
        print(f"{'='*60}")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        if platform.system() == 'Windows' and sys.version_info >= (3, 8):
            if not isinstance(loop, asyncio.ProactorEventLoop):
                loop = asyncio.ProactorEventLoop()
                asyncio.set_event_loop(loop)
        
        try:
            products = loop.run_until_complete(scrape_store_async(store_name))
        finally:
            try:
                loop.close()
            except:
                pass
        
        gc.collect()
        
        if not products:
            print(f"⚠️ No products scraped for {store_name}")
            return []
        
        print(f"✅ {store_name} - Scraped {len(products)} products")
        print(f"💾 Memory after scrape: {get_memory_usage():.0f}MB")
        return products
    
    except Exception as e:
        print(f"❌ Error in scrape_store_task for {store_name}: {e}")
        import traceback
        traceback.print_exc()
        
        if self.request.retries >= 4:
            print(f"⚠️ Max retries reached for {store_name}, skipping...")
            send_telegram(f"⚠️ Failed to scrape {store_name} after 5 attempts")
            return []
        
        if "memory" in str(e).lower():
            print(f"⚠️ Memory error detected, not retrying")
            return []
        
        raise self.retry(exc=e, countdown=120 * (self.request.retries + 1))

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 30})
def process_products_task(self, products, shop_name):
    """Process products with error handling and retry"""
    
    if not products:
        print(f"⚠️ No products to process for {shop_name}")
        return (0, 0)
    
    BATCH_SIZE = 100
    conn = None
    
    max_retries = 3
    for db_attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_FILE, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except Exception as e:
            print(f"❌ DB connection error (attempt {db_attempt+1}): {e}")
            if db_attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise
    
    try:
        cursor = conn.cursor()
        processed_count = 0
        alert_count = 0
        
        print(f"\n{'='*60}")
        print(f"🔄 Processing {len(products)} products from {shop_name}")
        print(f"📊 Task attempt: {self.request.retries + 1}")
        print(f"💾 Memory: {get_memory_usage():.0f}MB")
        print(f"{'='*60}")

        for batch_start in range(0, len(products), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(products))
            batch = products[batch_start:batch_end]
            
            print(f"📦 Processing batch {batch_start}-{batch_end}/{len(products)}")
            
            for product in batch:
                try:
                    cursor.execute(
                        "SELECT price, target_price, target_discount, send_alert FROM product_list WHERE url=?", 
                        (product["url"],)
                    )
                    row = cursor.fetchone()

                    if not row:
                        continue

                    db_price, target_price, target_discount, last_alert_price = row
                    current_price = product["price"]
                    current_discount = product.get("discount")

                    if current_price is None:
                        continue

                    # Update with retry
                    update_retries = 3
                    for update_attempt in range(update_retries):
                        try:
                            cursor.execute("""
                                UPDATE product_list 
                                SET price=?, MRP=?, discount=?, rating=?, reviews=?, image=?
                                WHERE url=?
                            """, (
                                current_price,
                                product.get("MRP"),
                                current_discount,
                                product.get("rating"),
                                product.get("reviews"),
                                product.get("image"),
                                product["url"]
                            ))
                            conn.commit()
                            processed_count += 1
                            break
                        except sqlite3.OperationalError as db_err:
                            if update_attempt < update_retries - 1:
                                print(f"⚠️ DB locked, retrying...")
                                time.sleep(1)
                            else:
                                raise

                    # Alert logic
                    should_alert_price = False
                    should_alert_discount = False
                    
                    if target_price and current_price < target_price:
                        if last_alert_price is None or current_price != last_alert_price:
                            should_alert_price = True
                    
                    if target_discount and current_discount and current_discount >= target_discount:
                        if last_alert_price is None or current_price != last_alert_price:
                            should_alert_discount = True

                    if should_alert_price or should_alert_discount:
                        msg_parts = [f"🎯 {product['title']}\n"]
                        
                        if should_alert_price:
                            msg_parts.append(f"💰 বর্তমান দাম: {current_price} টাকা\n")
                            msg_parts.append(f"🏷 আপনার টার্গেট দাম: {target_price} টাকা\n")
                            if current_discount:
                                msg_parts.append(f"🔥 ডিসকাউন্ট: {current_discount}%\n")
                            msg_parts.append(f"\n🔥 ভাই, আপনার টার্গেট দামের নিচে চলে এসেছে!\n")
                        
                        if should_alert_discount and not should_alert_price:
                            msg_parts.append(f"💰 বর্তমান দাম: {current_price} টাকা\n")
                            msg_parts.append(f"🔥 ডিসকাউন্ট: {current_discount}%\n")
                            msg_parts.append(f"🎁 টার্গেট ডিসকাউন্ট: {target_discount}%\n")
                            msg_parts.append(f"\n🎉 ভাই, আপনার টার্গেট ডিসকাউন্ট পাওয়া গেছে!\n")
                        
                        msg_parts.append(f"\n🔗 {product['url']}")
                        msg = "".join(msg_parts)
                        
                        if send_telegram(msg):
                            cursor.execute(
                                "UPDATE product_list SET send_alert=? WHERE url=?", 
                                (current_price, product["url"])
                            )
                            conn.commit()
                            alert_count += 1
                        
                except Exception as e:
                    print(f"⚠️ Error processing product {product.get('title', 'Unknown')}: {e}")
                    try:
                        conn.rollback()
                    except:
                        pass
                    continue
            
            gc.collect()
        
        print(f"✅ {shop_name} - Completed: {processed_count} products, {alert_count} alerts")
        print(f"💾 Memory after processing: {get_memory_usage():.0f}MB")
        
        return (processed_count, alert_count)
    
    except Exception as e:
        print(f"❌ Error in process_products_task: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            conn.rollback()
        except:
            pass
        
        if self.request.retries >= 2:
            print(f"⚠️ Max retries reached for processing {shop_name}")
            return (0, 0)
        
        raise self.retry(exc=e, countdown=30)
    
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass
        gc.collect()

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5, 'countdown': 10})
def send_summary_notification(self, shop_name, processed_count, alert_count, elapsed_time):
    """Send summary notification with retry"""
    try:
        msg = (
            f"✅ {shop_name} - সম্পূর্ণ হয়েছে\n"
            f"📦 প্রোডাক্ট আপডেট: {processed_count}\n"
            f"🔔 নতুন অ্যালার্ট: {alert_count}\n"
            f"⏱ সময়: {elapsed_time:.1f} সেকেন্ড"
        )
        
        if send_telegram(msg):
            print(f"📩 Summary sent for {shop_name}")
        else:
            raise Exception("Failed to send telegram")
            
    except Exception as e:
        print(f"❌ Failed to send summary (attempt {self.request.retries + 1}): {e}")
        if self.request.retries < 4:
            raise self.retry(exc=e, countdown=10)