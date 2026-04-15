#!/usr/bin/env python3
"""
VPS-Optimized runner for Daraz Price Monitor
Optimized for 1-core, 1GB RAM VPS
"""

import os
import sys
import time
import subprocess
import signal
from multiprocessing import Process

def start_celery_worker():
    """Start Celery worker process (VPS optimized)"""
    print("🔧 Starting Celery worker (VPS mode)...")
    
    # VPS OPTIMIZATION: Always use solo pool for single-core
    cmd = [
        sys.executable, "-m", "celery",
        "-A", "tasks",
        "worker",
        "--loglevel=info",
        "--pool=solo",              # Single-threaded for 1-core VPS
        "--concurrency=1",          # Process one task at a time
        "--max-tasks-per-child=10", # Restart after 10 tasks (prevent memory leaks)
        "--without-heartbeat",      # Reduce overhead
        "--without-mingle",         # Reduce startup overhead
        "--without-gossip",         # Reduce network overhead
    ]
    
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n⛔ Celery worker stopped")

def start_monitor():
    """Start monitor process"""
    print("🔧 Starting monitor...")
    time.sleep(5)  # Wait for Celery to be ready
    
    try:
        subprocess.run([sys.executable, "monitor.py"])
    except KeyboardInterrupt:
        print("\n⛔ Monitor stopped")

def check_redis():
    """Check if Redis is running"""
    try:
        import redis
        from config import REDIS_URL
        
        r = redis.from_url(REDIS_URL)
        r.ping()
        print("✅ Redis: Connected")
        return True
    except Exception as e:
        print(f"❌ Redis: Failed to connect - {e}")
        print("\n💡 Start Redis with:")
        print("   sudo systemctl start redis-server")
        return False

def check_dependencies():
    """Check if all dependencies are installed"""
    required_modules = [
        'celery',
        'redis', 
        'requests',
        'lxml',
        'crawl4ai'
    ]
    
    # Optional but recommended for VPS
    optional_modules = ['psutil']
    
    missing = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    
    if missing:
        print(f"❌ Missing dependencies: {', '.join(missing)}")
        print("\n💡 Install them with:")
        print("   pip install -r requirements.txt")
        return False
    
    # Check optional
    missing_optional = []
    for module in optional_modules:
        try:
            __import__(module)
        except ImportError:
            missing_optional.append(module)
    
    if missing_optional:
        print(f"⚠️  Optional dependencies missing: {', '.join(missing_optional)}")
        print("   (Memory monitoring will be disabled)")
    
    print("✅ Dependencies: All required modules installed")
    return True

def check_files():
    """Check if required files exist"""
    required_files = ['config.py', 'tasks.py', 'monitor.py', 'shop.txt']
    missing = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing.append(file)
    
    if missing:
        print(f"❌ Missing files: {', '.join(missing)}")
        if 'shop.txt' in missing:
            print("\n💡 Create shop.txt and add your shop names:")
            print("   echo 'shop-name' > shop.txt")
        return False
    
    print("✅ Files: All present")
    return True

def check_system_resources():
    """Check system resources"""
    try:
        import psutil
        
        # CPU
        cpu_count = psutil.cpu_count()
        print(f"💻 CPU Cores: {cpu_count}")
        
        # Memory
        mem = psutil.virtual_memory()
        mem_total_gb = mem.total / (1024**3)
        mem_available_gb = mem.available / (1024**3)
        
        print(f"💾 RAM: {mem_total_gb:.1f}GB total, {mem_available_gb:.1f}GB available")
        
        if mem_total_gb < 1.5:
            print("⚠️  WARNING: Low RAM detected! Consider:")
            print("   - Reducing MAX_PAGES_PER_SHOP in config.py")
            print("   - Processing fewer shops per cycle")
            print("   - Enabling swap if not already enabled")
        
        # Disk
        disk = psutil.disk_usage('/')
        disk_free_gb = disk.free / (1024**3)
        print(f"💿 Disk: {disk_free_gb:.1f}GB free")
        
        if disk_free_gb < 1:
            print("⚠️  WARNING: Low disk space!")
        
        return True
        
    except ImportError:
        print("⚠️  psutil not installed, skipping resource check")
        return True

def optimize_vps():
    """Display VPS optimization tips"""
    print("\n" + "="*70)
    print("💡 VPS OPTIMIZATION TIPS")
    print("="*70)
    print("1. Enable swap (if not already):")
    print("   sudo fallocate -l 2G /swapfile")
    print("   sudo chmod 600 /swapfile")
    print("   sudo mkswap /swapfile")
    print("   sudo swapon /swapfile")
    print()
    print("2. Optimize Redis memory:")
    print("   redis-cli CONFIG SET maxmemory 256mb")
    print("   redis-cli CONFIG SET maxmemory-policy allkeys-lru")
    print()
    print("3. Monitor with htop:")
    print("   htop")
    print("="*70)
    print()

def main():
    """Main function to run everything"""
    print("="*70)
    print("🚀 DARAZ PRICE MONITOR - VPS OPTIMIZED (1 Core, 1GB RAM)")
    print("="*70)
    print(f"⏰ Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    print()
    
    # Pre-flight checks
    print("🔍 Running pre-flight checks...\n")
    
    if not check_dependencies():
        sys.exit(1)
    
    if not check_files():
        sys.exit(1)
    
    if not check_redis():
        sys.exit(1)
    
    check_system_resources()
    
    print("\n✅ All checks passed!\n")
    
    optimize_vps()
    
    print("="*70)
    print("🎯 Starting Daraz Price Monitor (VPS Mode)...")
    print("="*70)
    print()
    print("📌 Configuration:")
    print("   • Pool: Solo (single-threaded)")
    print("   • Concurrency: 1")
    print("   • Max tasks per worker: 10")
    print("   • Max memory per worker: 800MB")
    print()
    print("⚠️  Press Ctrl+C to stop both processes")
    print("="*70)
    print()
    
    time.sleep(2)
    
    # Start both processes
    celery_process = Process(target=start_celery_worker, name="CeleryWorker")
    monitor_process = Process(target=start_monitor, name="Monitor")
    
    try:
        celery_process.start()
        time.sleep(8)  # Give Celery more time to start on VPS
        monitor_process.start()
        
        print("\n✅ Both processes started successfully!")
        print("📊 Monitor logs will appear below...")
        print("="*70)
        print()
        
        # Wait for processes
        celery_process.join()
        monitor_process.join()
        
    except KeyboardInterrupt:
        print("\n\n" + "="*70)
        print("⛔ Stopping all processes...")
        print("="*70)
        
        # Terminate both processes
        if celery_process.is_alive():
            celery_process.terminate()
            celery_process.join(timeout=5)
            if celery_process.is_alive():
                celery_process.kill()
        
        if monitor_process.is_alive():
            monitor_process.terminate()
            monitor_process.join(timeout=5)
            if monitor_process.is_alive():
                monitor_process.kill()
        
        print("✅ All processes stopped")
        print("👋 Goodbye!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        
        # Clean up
        if celery_process.is_alive():
            celery_process.terminate()
        if monitor_process.is_alive():
            monitor_process.terminate()
        
        sys.exit(1)

if __name__ == "__main__":
    main()