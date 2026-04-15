#!/usr/bin/env python3
"""
Health Check Script for Daraz Monitor
Tests all components before running
"""

import sys
import os
import time

print("="*70)
print("🏥 DARAZ MONITOR - HEALTH CHECK")
print("="*70)
print()

all_passed = True
warnings = []

# ==================== 1. Python Version ====================
print("1️⃣ Checking Python version...")
py_version = sys.version_info
if py_version >= (3, 8):
    print(f"   ✅ Python {py_version.major}.{py_version.minor}.{py_version.micro}")
else:
    print(f"   ❌ Python {py_version.major}.{py_version.minor} (need 3.8+)")
    all_passed = False

# ==================== 2. Required Modules ====================
print("\n2️⃣ Checking required modules...")
required_modules = {
    'celery': 'Celery',
    'redis': 'Redis client',
    'requests': 'HTTP client',
    'lxml': 'HTML parser',
    'crawl4ai': 'Web crawler',
    'asyncio': 'Async support'
}

missing = []
for module, name in required_modules.items():
    try:
        __import__(module)
        print(f"   ✅ {name}")
    except ImportError:
        print(f"   ❌ {name} (missing)")
        missing.append(module)
        all_passed = False

if missing:
    print(f"\n   💡 Install missing modules:")
    print(f"      pip install {' '.join(missing)}")

# ==================== 3. Optional Modules ====================
print("\n3️⃣ Checking optional modules...")
optional = {'psutil': 'System monitoring'}
for module, name in optional.items():
    try:
        __import__(module)
        print(f"   ✅ {name}")
    except ImportError:
        print(f"   ⚠️ {name} (optional)")
        warnings.append(f"{name} not installed (memory monitoring disabled)")

# ==================== 4. Crawl4AI Browser ====================
print("\n4️⃣ Testing Crawl4AI browser setup...")
try:
    import asyncio
    from crawl4ai import AsyncWebCrawler, BrowserConfig
    
    async def test_browser():
        config = BrowserConfig(browser_type='chromium', headless=True)
        try:
            async with AsyncWebCrawler(config=config) as crawler:
                result = await crawler.arun('https://example.com')
                return result.success
        except Exception as e:
            print(f"   ❌ Browser test failed: {e}")
            return False
    
    success = asyncio.run(test_browser())
    if success:
        print("   ✅ Chromium browser working")
    else:
        print("   ❌ Browser failed")
        print("   💡 Run: crawl4ai-setup")
        all_passed = False
        
except Exception as e:
    print(f"   ❌ Browser test error: {e}")
    print("   💡 Run: pip install -U crawl4ai && crawl4ai-setup")
    all_passed = False

# ==================== 5. Redis Connection ====================
print("\n5️⃣ Testing Redis connection...")
try:
    import redis
    from config import REDIS_URL
    
    r = redis.from_url(REDIS_URL, socket_connect_timeout=5)
    r.ping()
    print(f"   ✅ Redis connected: {REDIS_URL}")
except Exception as e:
    print(f"   ❌ Redis connection failed: {e}")
    print("   💡 Start Redis:")
    print("      redis-server")
    all_passed = False

# ==================== 6. Database ====================
print("\n6️⃣ Testing database...")
try:
    from config import DB_FILE, DB_SCHEMA
    import sqlite3
    
    conn = sqlite3.connect(DB_FILE, timeout=10)
    cursor = conn.cursor()
    cursor.execute(DB_SCHEMA)
    conn.commit()
    
    # Test query
    cursor.execute("SELECT COUNT(*) FROM product_list")
    count = cursor.fetchone()[0]
    
    conn.close()
    print(f"   ✅ Database OK ({count} products)")
    
except Exception as e:
    print(f"   ❌ Database error: {e}")
    all_passed = False

# ==================== 7. shop.txt ====================
print("\n7️⃣ Checking shop.txt...")
try:
    with open("shop.txt", "r", encoding="utf-8") as f:
        shops = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    
    if not shops:
        print("   ⚠️ shop.txt is empty")
        warnings.append("No shops configured")
    else:
        print(f"   ✅ {len(shops)} shops configured")
        
except FileNotFoundError:
    print("   ❌ shop.txt not found")
    print("   💡 Creating shop.txt...")
    with open("shop.txt", "w", encoding="utf-8") as f:
        f.write("# Add shop names here\naarongearth\n")
    print("   ✅ Created shop.txt with example")
    warnings.append("shop.txt was empty, added example")

# ==================== 8. Telegram ====================
print("\n8️⃣ Testing Telegram bot...")
try:
    from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS
    import requests
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        print("   ⚠️ Telegram not configured")
        warnings.append("Telegram notifications disabled")
    else:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            bot_info = response.json()
            bot_name = bot_info.get('result', {}).get('username', 'Unknown')
            print(f"   ✅ Telegram bot: @{bot_name}")
            
            # Test send
            test_msg = "✅ Health check passed!"
            test_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            for chat_id in TELEGRAM_CHAT_IDS[:1]:  # Test first chat only
                response = requests.post(
                    test_url,
                    data={"chat_id": chat_id, "text": test_msg},
                    timeout=10
                )
                if response.status_code == 200:
                    print(f"   ✅ Test message sent to {chat_id}")
                else:
                    print(f"   ⚠️ Failed to send to {chat_id}")
                    warnings.append(f"Telegram chat {chat_id} unreachable")
        else:
            print(f"   ❌ Invalid bot token")
            all_passed = False
            
except Exception as e:
    print(f"   ⚠️ Telegram test failed: {e}")
    warnings.append("Telegram notifications may not work")

# ==================== 9. Files ====================
print("\n9️⃣ Checking required files...")
required_files = ['config.py', 'tasks.py', 'monitor.py', 'run.py']
for filename in required_files:
    if os.path.exists(filename):
        print(f"   ✅ {filename}")
    else:
        print(f"   ❌ {filename} (missing)")
        all_passed = False

# ==================== 10. Quick Scrape Test ====================
print("\n🔟 Testing scrape functionality...")
try:
    import asyncio
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
    from lxml import html as lxml_html
    
    async def test_scrape():
        config = BrowserConfig(browser_type='chromium', headless=True)
        async with AsyncWebCrawler(config=config) as crawler:
            test_url = "https://www.daraz.com.bd/aarongearth/?from=wangpu&langFlag=en&page=1&pageTypeId=2&q=All-Products"
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=30000
            )
            result = await crawler.arun(url=test_url, config=crawl_config)
            
            if result.success:
                tree = lxml_html.fromstring(result.html)
                products = tree.xpath("//div[.//img[@type='product']]")
                return len(products)
            return 0
    
    product_count = asyncio.run(test_scrape())
    if product_count > 0:
        print(f"   ✅ Scraped {product_count} products from test shop")
    else:
        print(f"   ⚠️ No products found (site may have changed)")
        warnings.append("Scraper may need updating")
        
except Exception as e:
    print(f"   ❌ Scrape test failed: {e}")
    warnings.append("Scraping functionality needs checking")

# ==================== SUMMARY ====================
print("\n" + "="*70)
print("📊 HEALTH CHECK SUMMARY")
print("="*70)

if all_passed and not warnings:
    print("✅ ALL CHECKS PASSED - SYSTEM READY!")
    print("\n💡 You can now run: python run.py")
    
elif all_passed and warnings:
    print("✅ All critical checks passed")
    print(f"⚠️ {len(warnings)} warnings:")
    for i, warning in enumerate(warnings, 1):
        print(f"   {i}. {warning}")
    print("\n💡 System will work but some features may be limited")
    print("💡 You can run: python run.py")
    
else:
    print("❌ HEALTH CHECK FAILED")
    print("\n🔧 Please fix the issues above before running")
    print("\n💡 Quick fixes:")
    print("   1. Install missing modules: pip install -r requirements.txt")
    print("   2. Setup browser: crawl4ai-setup")
    print("   3. Start Redis: redis-server")
    print("   4. Configure shop.txt with your shop names")

print("="*70)
print()