"""
find_api.py
-----------
Probes several URL patterns to find which one Daraz uses
to serve product listings as JSON or parseable HTML.

Usage:
    python find_api.py citiplus-electronic-technology
"""
import sys
import requests
import json

STORE = sys.argv[1] if len(sys.argv) > 1 else "citiplus-electronic-technology"
PAGE  = 1

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*; q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"https://www.daraz.com.bd/{STORE}/",
    "X-Requested-With": "XMLHttpRequest",
}

CANDIDATES = [
    # Pattern 1 – seller page with ajax=true
    f"https://www.daraz.com.bd/{STORE}/?ajax=true&from=wangpu&langFlag=en&page={PAGE}&pageTypeId=2&q=All-Products",
    # Pattern 2 – catalog search with seller filter
    f"https://www.daraz.com.bd/catalog/?ajax=true&from=wangpu&langFlag=en&page={PAGE}&pageTypeId=2&q=All-Products&sellerName={STORE}",
    # Pattern 3 – shop-page endpoint
    f"https://www.daraz.com.bd/shop-page/seller-items?sellerName={STORE}&page={PAGE}&limit=40",
    # Pattern 4 – seller listing endpoint
    f"https://www.daraz.com.bd/seller/listing?sellerName={STORE}&page={PAGE}",
    # Pattern 5 – search API
    f"https://www.daraz.com.bd/search-api/?ajax=true&from=wangpu&langFlag=en&page={PAGE}&pageTypeId=2&q=All-Products&sellerName={STORE}",
]

session = requests.Session()
session.headers.update(HEADERS)

# First: hit the store page to get real cookies (session, CAPTCHA tokens)
print(f"[*] Pre-fetching store page to get cookies...")
try:
    r0 = session.get(
        f"https://www.daraz.com.bd/{STORE}/",
        timeout=15,
        allow_redirects=True,
    )
    print(f"    Status: {r0.status_code}  |  Size: {len(r0.content):,} bytes")
    print(f"    Cookies: {dict(session.cookies)}")
except Exception as e:
    print(f"    Error: {e}")

print()

for url in CANDIDATES:
    print(f"[?] {url[:90]}...")
    try:
        r = session.get(url, timeout=15)
        ct = r.headers.get("Content-Type", "")
        body = r.text[:500].replace("\n", " ")
        print(f"    Status : {r.status_code}")
        print(f"    Type   : {ct}")
        print(f"    Preview: {body}")

        if r.status_code == 200:
            # Try to parse as JSON
            try:
                data = r.json()
                print(f"    ✅ VALID JSON — keys: {list(data.keys())[:10]}")
                with open("api_response.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print("    Saved to api_response.json")
            except Exception:
                if "৳" in r.text or "price" in r.text.lower():
                    print("    ✅ HTML with price data")
                    with open("api_response.html", "w", encoding="utf-8") as f:
                        f.write(r.text)
                    print("    Saved to api_response.html")
                elif "captcha" in r.text.lower() or "robot" in r.text.lower():
                    print("    ❌ CAPTCHA / bot block detected")
                else:
                    print("    ⚠️  No price data found in response")
    except Exception as e:
        print(f"    ERROR: {e}")
    print()
