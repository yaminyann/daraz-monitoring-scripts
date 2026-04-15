TELEGRAM_BOT_TOKEN = "8347084661:AAHQS_7WNzb18xZXqiiXG3vFy_t7_76nZBA"
TELEGRAM_CHAT_IDS = ["1173842592", "5761936471", "1908824171"]

# ==================== SCRAPING CONFIG ====================
BASE_URL = "https://www.daraz.com.bd/{}/?from=wangpu&langFlag=en&page={}&pageTypeId=2&q=All-Products"

# ==================== TIMING CONFIG (ANTI-BAN OPTIMIZED) ====================
# Random delays are added in code for better anti-ban
SLEEP_BETWEEN_CYCLES = 900      # 15 minutes between full cycles
WAIT_BETWEEN_SHOPS = 8         # 30 seconds between shops (increased for safety)
WAIT_BETWEEN_PAGES = 3          # 5 seconds between pages (increased for safety)

# Max pages per shop (prevents timeout and reduces ban risk)
MAX_PAGES_PER_SHOP = 40

# ==================== ANTI-BAN CONFIG ====================
# Random delays will be applied on top of base delays
RANDOM_DELAY_MIN = 2            # Minimum random delay (seconds)
RANDOM_DELAY_MAX = 6            # Maximum random delay (seconds)

# Every N pages, take a longer break
LONG_BREAK_EVERY_N_PAGES = 5
LONG_BREAK_DURATION = 3        # 15 seconds break every 5 pages

# Retry configuration
MAX_SCRAPE_RETRIES = 3          # Retry failed scrapes 3 times
MAX_PAGE_RETRIES = 3            # Retry failed pages 3 times
RETRY_DELAY = 5                # Wait 60s before retry

# Ban detection
BAN_DETECTION_KEYWORDS = ["403", "blocked", "captcha", "robot"]
BAN_COOLDOWN = 300              # Wait 5 minutes if banned

# ==================== DATABASE CONFIG ====================
DB_FILE = "product_list.db"

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS product_list (
    id INTEGER PRIMARY KEY,
    title TEXT,
    url TEXT UNIQUE,
    price REAL,
    MRP REAL,
    discount REAL,
    target_discount REAL,
    target_price REAL,
    sold INTEGER,
    rating REAL,
    reviews INTEGER,
    location TEXT,
    image TEXT,
    send_alert BOOLEAN
)
"""

# ==================== REDIS CONFIG ====================
REDIS_URL = "redis://localhost:6379/0"

# ==================== CELERY CONFIG ====================
# Task timeouts
SCRAPE_TASK_TIMEOUT = 3600      # 1 hour per shop
PROCESS_TASK_TIMEOUT = 600      # 10 minutes for processing

# Auto-retry config
CELERY_MAX_RETRIES = 5
CELERY_RETRY_DELAY = 10        # 2 minutes between retries

# ==================== MEMORY OPTIMIZATION ====================
ENABLE_MEMORY_MONITORING = True
MEMORY_THRESHOLD_MB = 1400       # Stop if memory exceeds 700MB

# Batch processing
PROCESS_BATCH_SIZE = 100        # Process 100 products at a time

# ==================== ERROR HANDLING ====================
MAX_CONSECUTIVE_ERRORS = 5      # Pause if 5 errors in a row
ERROR_COOLDOWN = 300            # 5 minutes cooldown after max errors

# ==================== LOGGING CONFIG ====================
VERBOSE_LOGGING = True          # Print detailed logs
LOG_FILE = "daraz_monitor.log"  # Optional: log to file

# ==================== FEATURE FLAGS ====================
ENABLE_TELEGRAM_ALERTS = True   # Send Telegram notifications
ENABLE_SUMMARY_NOTIFICATIONS = True  # Send summary after each shop
ENABLE_ERROR_NOTIFICATIONS = True    # Send error notifications

# ==================== NOTES ====================
# This configuration is optimized for:
# 1. Anti-ban protection (random delays, human-like behavior)
# 2. Error recovery (auto-retry, cooldowns)
# 3. Never stopping (continues even after errors)
# 4. Windows local machine (not VPS optimized)
#
# To reduce ban risk:
# - Increase WAIT_BETWEEN_SHOPS to 60 or more
# - Decrease MAX_PAGES_PER_SHOP to 20
# - Enable LONG_BREAK every 3-5 pages