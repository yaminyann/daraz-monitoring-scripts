import time
import sys
import traceback
from tasks import (
    init_db, 
    scrape_store_task, 
    process_products_task, 
    send_summary_notification,
    send_telegram
)
from config import SLEEP_BETWEEN_CYCLES, WAIT_BETWEEN_SHOPS

# Global counters for monitoring
total_cycles_completed = 0
total_errors = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 5


def get_shops():
    """Load shop names from shop.txt with retry"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with open("shop.txt", "r", encoding="utf-8") as f:
                shops = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            
            if not shops:
                print("⚠️ shop.txt is empty!")
                return []
            
            print(f"✅ Loaded {len(shops)} shops")
            return shops
            
        except FileNotFoundError:
            print(f"❌ shop.txt not found! (attempt {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                print("⚠️ Creating empty shop.txt...")
                with open("shop.txt", "w", encoding="utf-8") as f:
                    f.write("# Add shop names here, one per line\n")
                return []
        except Exception as e:
            print(f"❌ Error reading shop.txt: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return []
    return []

def process_single_shop(shop_name, shop_index, total_shops):
    """Process a single shop with comprehensive error handling"""
    global consecutive_errors
    
    try:
        shop_start_time = time.time()
        
        print(f"\n{'#'*70}")
        print(f"# Shop {shop_index}/{total_shops}: {shop_name}")
        print(f"{'#'*70}")
        
        # Step 1: Scrape with timeout and retry
        print(f"\n🔍 Step 1/3: Scraping {shop_name}...")
        print(f"⏰ Started at {time.strftime('%H:%M:%S')}")
        
        scrape_result = None
        scrape_timeout = 1800  # 30 minutes
        check_interval = 10
        
        try:
            scrape_result = scrape_store_task.apply_async(
                args=[shop_name],
                expires=scrape_timeout
            )
            
            print(f"📋 Task ID: {scrape_result.id[:12]}...")
            elapsed = 0
            
            while not scrape_result.ready() and elapsed < scrape_timeout:
                time.sleep(check_interval)
                elapsed += check_interval
                
                if elapsed % 60 == 0:  # Update every minute
                    print(f"⏱️ Still scraping... ({elapsed}s elapsed)")
            
            if not scrape_result.ready():
                print(f"⏰ Scraping timeout after {scrape_timeout}s")
                try:
                    scrape_result.revoke(terminate=True)
                except:
                    pass
                return 0, 0
            
            products = scrape_result.get(timeout=30)
            
        except Exception as scrape_error:
            print(f"❌ Scraping error: {scrape_error}")
            products = []
        
        if not products:
            print(f"⚠️ No products scraped for {shop_name}, skipping...")
            return 0, 0
        
        scrape_time = time.time() - shop_start_time
        print(f"✅ Scraped {len(products)} products in {scrape_time:.1f}s")
        
        # Reset consecutive errors on success
        consecutive_errors = 0
        
        # Step 2: Process products
        process_start_time = time.time()
        print(f"\n⚙️ Step 2/3: Processing products from {shop_name}...")
        
        process_result = None
        process_timeout = 600  # 10 minutes
        
        try:
            process_result = process_products_task.apply_async(
                args=[products, shop_name],
                expires=process_timeout
            )
            
            print(f"📋 Task ID: {process_result.id[:12]}...")
            elapsed = 0
            
            while not process_result.ready() and elapsed < process_timeout:
                time.sleep(check_interval)
                elapsed += check_interval
                
                if elapsed % 30 == 0:
                    print(f"⏱️ Still processing... ({elapsed}s)")
            
            if not process_result.ready():
                print(f"⏰ Processing timeout after {process_timeout}s")
                try:
                    process_result.revoke(terminate=True)
                except:
                    pass
                return 0, 0
            
            processed_count, alert_count = process_result.get(timeout=30)
            
        except Exception as process_error:
            print(f"❌ Processing error: {process_error}")
            processed_count, alert_count = 0, 0
        
        process_time = time.time() - process_start_time
        print(f"✅ Processed {processed_count} products in {process_time:.1f}s")
        print(f"🔔 Sent {alert_count} alerts")
        
        # Step 3: Send summary
        shop_elapsed_time = time.time() - shop_start_time
        print(f"\n📊 Step 3/3: Sending summary for {shop_name}...")
        
        try:
            summary_result = send_summary_notification.apply_async(
                args=[shop_name, processed_count, alert_count, shop_elapsed_time],
                expires=60
            )
            summary_result.get(timeout=30)
        except Exception as summary_error:
            print(f"⚠️ Summary notification failed: {summary_error}")
        
        print(f"\n{'='*70}")
        print(f"✅ {shop_name} COMPLETED")
        print(f"{'='*70}")
        print(f"⏱️ Total Time: {shop_elapsed_time:.1f}s")
        print(f"  ├─ Scraping: {scrape_time:.1f}s ({len(products)} products)")
        print(f"  ├─ Processing: {process_time:.1f}s ({processed_count} products)")
        print(f"  └─ Alerts: {alert_count} sent")
        print(f"{'='*70}")
        
        return processed_count, alert_count
        
    except KeyboardInterrupt:
        raise  # Allow keyboard interrupt to propagate
        
    except Exception as e:
        global total_errors
        total_errors += 1
        consecutive_errors += 1
        
        print(f"❌ Critical error processing {shop_name}: {e}")
        traceback.print_exc()
        
        error_msg = f"❌ Error in {shop_name}\n{str(e)[:200]}"
        try:
            send_telegram(error_msg)
        except:
            pass
        
        # If too many consecutive errors, pause longer
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            print(f"⚠️ {consecutive_errors} consecutive errors! Taking 5 min break...")
            try:
                send_telegram(f"⚠️ Multiple errors detected! Taking break...")
            except:
                pass
            time.sleep(300)
            consecutive_errors = 0
        
        return 0, 0

def monitor_cycle():
    """Execute one complete monitoring cycle with error recovery"""
    global total_cycles_completed
    
    shops = get_shops()
    
    if not shops:
        print("⚠️ No shops to monitor!")
        try:
            send_telegram("⚠️ No shops in shop.txt!")
        except:
            pass
        return False
    
    cycle_start_time = time.time()
    total_shops = len(shops)
    
    print(f"\n{'='*70}")
    print(f"🚀 STARTING CYCLE #{total_cycles_completed + 1}")
    print(f"{'='*70}")
    print(f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🪧 Shops: {total_shops}")
    print(f"📊 Total Errors So Far: {total_errors}")
    print(f"{'='*70}\n")
    
    total_products = 0
    total_alerts = 0
    completed_shops = 0
    failed_shops = 0
    
    for i, shop in enumerate(shops, 1):
        try:
            print(f"\n{'▀'*70}")
            print(f"▀ SHOP {i}/{total_shops} {'▀'*50}")
            print(f"{'▀'*70}")
            
            processed_count, alert_count = process_single_shop(shop, i, total_shops)
            
            if processed_count > 0:
                completed_shops += 1
            else:
                failed_shops += 1
            
            total_products += processed_count
            total_alerts += alert_count
            
            # Progress
            remaining = total_shops - i
            print(f"\n📈 PROGRESS:")
            print(f"  ├─ Completed: {completed_shops}/{total_shops}")
            print(f"  ├─ Failed: {failed_shops}")
            print(f"  ├─ Remaining: {remaining}")
            print(f"  ├─ Products: {total_products}")
            print(f"  └─ Alerts: {total_alerts}")
            
            # Wait before next shop
            if i < total_shops:
                print(f"\n⏳ Waiting {WAIT_BETWEEN_SHOPS}s before next shop...")
                print(f"{'─'*70}\n")
                time.sleep(WAIT_BETWEEN_SHOPS)
                
        except KeyboardInterrupt:
            raise  # Allow Ctrl+C
            
        except Exception as e:
            print(f"❌ Critical error with {shop}: {e}")
            failed_shops += 1
            time.sleep(5)  # Short pause before continuing
            continue
    
    # Cycle complete
    cycle_time = time.time() - cycle_start_time
    total_cycles_completed += 1
    
    print(f"\n{'='*70}")
    print(f"✅ CYCLE #{total_cycles_completed} COMPLETED")
    print(f"{'='*70}")
    print(f"⏱️ Duration: {cycle_time/60:.2f} minutes")
    print(f"🪧 Processed: {completed_shops}/{total_shops} shops")
    print(f"❌ Failed: {failed_shops}")
    print(f"📦 Products: {total_products}")
    print(f"🔔 Alerts: {total_alerts}")
    print(f"{'='*70}\n")
    
    # Send cycle summary
    summary_msg = (
        f"✅ Cycle #{total_cycles_completed} Complete\n"
        f"⏱️ Time: {cycle_time/60:.2f} min\n"
        f"✅ Success: {completed_shops}/{total_shops}\n"
        f"❌ Failed: {failed_shops}\n"
        f"📦 Products: {total_products}\n"
        f"🔔 Alerts: {total_alerts}"
    )
    
    try:
        send_telegram(summary_msg)
    except Exception as e:
        print(f"⚠️ Failed to send cycle summary: {e}")
    
    return True

def main():
    """Main monitoring loop - NEVER STOPS!"""
    global total_cycles_completed, total_errors
    
    # Initialize database with retry
    print("🔧 Initializing database...")
    max_db_retries = 5
    for attempt in range(max_db_retries):
        try:
            init_db()
            print("✅ Database initialized\n")
            break
        except Exception as e:
            print(f"❌ DB init error (attempt {attempt+1}): {e}")
            if attempt < max_db_retries - 1:
                time.sleep(10)
            else:
                print("❌ Cannot initialize database! Exiting...")
                sys.exit(1)
    
    print("="*70)
    print("🎯 DARAZ PRICE MONITOR - PRODUCTION EDITION")
    print("="*70)
    print("🛡️ Features:")
    print("  ✓ Auto-retry on errors")
    print("  ✓ Anti-ban protection")
    print("  ✓ Never stops running")
    print("  ✓ Error recovery")
    print("⏰ Started:", time.strftime('%Y-%m-%d %H:%M:%S'))
    print("="*70)
    
    # Startup notification
    startup_msg = (
        f"🚀 Monitor Started!\n"
        f"⏰ {time.strftime('%H:%M:%S')}\n"
        f"🛡️ Production Mode\n"
        f"⏱️ Cycle: {SLEEP_BETWEEN_CYCLES/60:.0f}min\n"
        f"⏳ Shop Delay: {WAIT_BETWEEN_SHOPS}s"
    )
    try:
        send_telegram(startup_msg)
    except:
        pass
    
    # Main infinite loop
    while True:
        try:
            print(f"\n{'█'*70}")
            print(f"█{'  CYCLE #' + str(total_cycles_completed + 1).center(62)}█")
            print(f"{'█'*70}\n")
            
            cycle_success = monitor_cycle()
            
            if not cycle_success:
                print("⚠️ Cycle failed, retrying in 5 minutes...")
                time.sleep(300)
                continue
            
            # Calculate next cycle
            next_cycle_time = time.time() + SLEEP_BETWEEN_CYCLES
            next_cycle_str = time.strftime('%H:%M:%S', time.localtime(next_cycle_time))
            
            print(f"💤 Sleeping {SLEEP_BETWEEN_CYCLES/60:.0f} minutes...")
            print(f"⏰ Next cycle: {next_cycle_str}")
            print(f"📊 Stats: {total_cycles_completed} cycles, {total_errors} total errors")
            print(f"{'─'*70}\n")
            
            time.sleep(SLEEP_BETWEEN_CYCLES)
            
        except KeyboardInterrupt:
            print("\n\n" + "="*70)
            print("👋 MONITOR STOPPED BY USER")
            print("="*70)
            print(f"📊 Final Stats:")
            print(f"  ├─ Total Cycles: {total_cycles_completed}")
            print(f"  └─ Total Errors: {total_errors}")
            print("="*70)
            
            try:
                send_telegram(
                    f"⛔ Monitor Stopped\n"
                    f"📊 {total_cycles_completed} cycles completed\n"
                    f"❌ {total_errors} total errors"
                )
            except:
                pass
            break
        
        except Exception as critical_error:
            total_errors += 1
            
            print(f"\n{'!'*70}")
            print(f"❌ CRITICAL ERROR IN MAIN LOOP")
            print(f"{'!'*70}")
            print(f"Error: {critical_error}")
            traceback.print_exc()
            print(f"{'!'*70}\n")
            
            # Notify about critical error
            try:
                error_msg = (
                    f"🚨 Critical Error!\n"
                    f"Cycle #{total_cycles_completed + 1}\n"
                    f"{str(critical_error)[:150]}\n"
                    f"⏳ Retrying in 2 minutes..."
                )
                send_telegram(error_msg)
            except:
                pass
            
            # Wait before retry
            print("🔄 Recovering in 120 seconds...")
            time.sleep(120)
            
            # Try to reconnect to everything
            print("🔄 Attempting recovery...")
            try:
                init_db()
                print("✅ Database reconnected")
            except:
                print("⚠️ Database reconnection failed")
            
            print("🔄 Continuing operation...\n")
            continue

if __name__ == "__main__":
    try:
        main()
    except Exception as fatal_error:
        print(f"\n{'X'*70}")
        print(f"💀 FATAL ERROR")
        print(f"{'X'*70}")
        print(f"{fatal_error}")
        traceback.print_exc()
        print(f"{'X'*70}\n")
        
        try:
            send_telegram(f"💀 Fatal Error\n{str(fatal_error)[:200]}")
        except:
            pass
        
        sys.exit(1)