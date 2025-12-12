#!/usr/bin/env python3
"""
gog_scraper.py

Single-file Selenium-based GOG scraper that downloads header image, screenshots and direct mp4 trailers,
and writes a detailed CSV. Configure via command-line args or constants below.

Requirements:
  pip install selenium webdriver-manager pandas requests

Notes:
  - Keep num_workers modest (2-6) to avoid anti-bot triggers. Increase slowly.
  - For debugging, set headless=False.
"""

import argparse
import os
import re
import time
import random
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -----------------------
# Configuration defaults
# -----------------------
DEFAULT_HEADLESS = True
DEFAULT_WORKERS = 3
DEFAULT_MAX_GAMES = 200
DEFAULT_PAGE_SLEEP = (1.0, 2.5)  # random delay between actions
DEFAULT_REQUEST_TIMEOUT = 20
MAX_SCREENSHOTS = 6
MAX_VIDEOS = 3
GAMES_PER_PAGE = 48  # default GOG layout, adjust if needed

# Shared state and locks
all_game_data = []
data_lock = Lock()

# -----------------------
# Utilities
# -----------------------
def sanitize_filename(name: str, maxlen: int = 80) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '', name)
    name = name.strip()
    return name[:maxlen] if len(name) > maxlen else name

def safe_getattr(elem, attr):
    try:
        return elem.get_attribute(attr)
    except:
        return None

def rand_sleep(a=DEFAULT_PAGE_SLEEP[0], b=DEFAULT_PAGE_SLEEP[1]):
    time.sleep(random.uniform(a, b))

# -----------------------
# Downloader with retries
# -----------------------
def download_media(url: str, save_dir: str, filename: str, timeout: int = DEFAULT_REQUEST_TIMEOUT, max_retries: int = 3):
    if not url or url == "N/A":
        return None
    try:
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, filename)
        # Skip if already downloaded
        if os.path.exists(path) and os.path.getsize(path) > 1024:
            return path
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        for attempt in range(1, max_retries + 1):
            try:
                with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                    if r.status_code == 200:
                        with open(path, 'wb') as fh:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk:
                                    fh.write(chunk)
                        return path
                    else:
                        # Non-200: break early for non-downloadable sources
                        break
            except Exception:
                if attempt == max_retries:
                    break
                time.sleep(1 + attempt)
        return None
    except Exception:
        return None

# -----------------------
# Price parsing
# -----------------------
def parse_price(txt: str):
    if not txt:
        return "N/A", "N/A", "N/A"
    t = txt.strip()
    if 'free' in t.lower():
        return "Free", "N/A", "N/A"
    disc = re.search(r'-(\d+)%', t)
    prices = re.findall(r'[€$£¥]\s*[\d,]+\.?\d*', t)
    price = prices[0].strip() if prices else "N/A"
    orig = prices[1].strip() if len(prices) > 1 else "N/A"
    discp = (disc.group(1) + "%") if disc else "N/A"
    return price, orig, discp

# -----------------------
# WebDriver factory
# -----------------------
def create_driver(headless=True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    # Reasonable UA
    opts.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    svc = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=svc, options=opts)
    # Stealth small patch
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    driver.set_page_load_timeout(60)
    return driver

def handle_cookies(driver):
    # Try a bunch of common cookie button selectors
    selectors = [
        "button.cookie-consent__accept",
        "#onetrust-accept-btn-handler",
        "button[class*='cookie']",
        "button[data-testid='cookie-accept']",
        "button[title*='Accept']",
        "button[aria-label*='accept']"
    ]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            btn.click()
            rand_sleep(0.5, 1.0)
            return True
        except Exception:
            continue
    return False

# -----------------------
# Scrape helpers
# -----------------------
def scrape_game_card(card):
    """
    Extract quick info from a game 'card' element (title, url, price)
    Return dict or None
    """
    try:
        data = {"title": "N/A", "url": "N/A", "price": "N/A", "original_price": "N/A", "discount_percentage": "N/A"}
        # URL: card may be an <a> or contain <a>
        href = safe_getattr(card, "href")
        if not href:
            try:
                a = card.find_element(By.CSS_SELECTOR, "a[href*='/game/']")
                href = a.get_attribute("href")
            except:
                href = None
        if href and '/game/' in href:
            if href.startswith("http"):
                data["url"] = href
            else:
                data["url"] = "https://www.gog.com" + href
        # Title
        try:
            title = card.find_element(By.CSS_SELECTOR, ".product-tile__title, [class*='title']").text.strip()
            if title:
                data["title"] = title
        except:
            try:
                aria = safe_getattr(card, "aria-label")
                if aria:
                    data["title"] = aria.strip()
            except:
                pass
        # Price
        try:
            price_elem = card.find_element(By.CSS_SELECTOR, ".product-tile__prices, [class*='price']")
            price_txt = price_elem.text.strip()
            p, o, d = parse_price(price_txt)
            data["price"], data["original_price"], data["discount_percentage"] = p, o, d
        except:
            pass

        return data if data["url"] != "N/A" else None
    except Exception:
        return None

def extract_details_from_page(driver, url, title, download_media_flag=True):
    """
    Visit the game page and extract details + download media into folder.
    Returns a dict with detailed fields.
    """
    details = {
        "title": title,
        "url": url,
        "release_date": "N/A",
        "genres": "N/A",
        "categories": "N/A",
        "platforms": "N/A",
        "review_summary": "N/A",
        "description": "N/A",
        "developer": "N/A",
        "publisher": "N/A",
        "system_requirements_windows": "N/A",
        "system_requirements_mac": "N/A",
        "system_requirements_linux": "N/A",
        "header_image": "N/A",
        "screenshots": "N/A",
        "videos": "N/A",
        "multiplayer": "No",
        "singleplayer": "No",
        "downloaded_images": [],
        "downloaded_videos": []
    }
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        rand_sleep()
        handle_cookies(driver)
        # Scroll a bit so lazy-loaded media appears
        for pos in (500, 1200, 2200, 3000):
            driver.execute_script(f"window.scrollTo(0, {pos});")
            rand_sleep(0.2, 0.6)
        driver.execute_script("window.scrollTo(0, 0);")
        rand_sleep(0.2, 0.6)

        # ======= DETAILS rows (developer, publisher, genres, release, platforms, features) =======
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, ".details__row, [class*='details-row'], [data-testid='game-info-row']")
            for row in rows:
                try:
                    label = row.find_element(By.CSS_SELECTOR, ".details__category, .label, .row-title").text.strip().lower()
                    content = ""
                    try:
                        content = row.find_element(By.CSS_SELECTOR, ".details__content, .value, .row-content").text.strip()
                    except:
                        pass
                    if 'genre' in label and content:
                        details["genres"] = ", ".join([g.strip() for g in content.split("\n") if g.strip()])
                    elif 'tag' in label or 'tags' in label:
                        details["categories"] = ", ".join([g.strip() for g in content.split("\n") if g.strip()][:20])
                    elif 'release' in label and content:
                        details["release_date"] = content
                    elif 'company' in label and content:
                        parts = [p.strip() for p in content.split("\n") if p.strip()]
                        if len(parts) >= 1: details["developer"] = parts[0]
                        if len(parts) >= 2: details["publisher"] = parts[1]
                    elif 'works on' in label or 'platform' in label:
                        ptxt = content.lower()
                        plats = []
                        if 'windows' in ptxt: plats.append("Windows")
                        if 'mac' in ptxt or 'os x' in ptxt: plats.append("Mac")
                        if 'linux' in ptxt: plats.append("Linux")
                        if plats:
                            details["platforms"] = ", ".join(plats)
                    elif 'game feature' in label or 'features' in label:
                        feats = row.find_elements(By.CSS_SELECTOR, ".details__feature, .feature, .tag")
                        for f in feats:
                            try:
                                ft = f.text.lower()
                                if 'single' in ft: details["singleplayer"] = "Yes"
                                if 'multi' in ft or 'co-op' in ft: details["multiplayer"] = "Yes"
                            except:
                                continue
                except:
                    continue
        except:
            pass

        # ======= DESCRIPTION =======
        try:
            desc_selectors = [
                ".description", "[class*='description']", ".productcard-description", "[data-testid='description']"
            ]
            for sel in desc_selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                    txt = elem.text.strip()
                    if len(txt) > 50:
                        details["description"] = txt[:1000] + ("..." if len(txt) > 1000 else "")
                        break
                except:
                    continue
            # fallback: longest paragraph
            if details["description"] == "N/A":
                paras = [p.text.strip() for p in driver.find_elements(By.TAG_NAME, "p") if len(p.text.strip()) > 80]
                if paras:
                    long = max(paras, key=len)
                    details["description"] = long[:1000] + ("..." if len(long) > 1000 else "")
        except:
            pass

        # ======= Reviews / rating =======
        try:
            revs = driver.find_elements(By.CSS_SELECTOR, "[class*='rating'], .product-rating, [data-testid='rating']")
            for r in revs:
                txt = r.text.strip()
                if txt and len(txt) < 120 and any(ch in txt.lower() for ch in ['star', '%', 'rating', 'positive']):
                    details["review_summary"] = txt
                    break
        except:
            pass

        # ======= SYSTEM REQUIREMENTS =======
        try:
            driver.execute_script("window.scrollTo(0, 2200);")
            rand_sleep(0.3, 0.7)
            req_candidates = driver.find_elements(By.CSS_SELECTOR, "[class*='system'], [class*='requirements'], [data-testid='system-requirements']")
            req_text = ""
            for rc in req_candidates:
                try:
                    txt = rc.text.strip()
                    if len(txt) > len(req_text):
                        req_text = txt
                except:
                    continue
            if req_text:
                # crude OS splitting using regex
                for os_name, patterns in [
                    ("windows", [r'Windows[:\s\-]+(.+?)(?=Mac|Linux|$)', r'PC[:\s\-]+(.+?)(?=Mac|Linux|$)']),
                    ("mac", [r'Mac[:\s\-]+(.+?)(?=Linux|Windows|$)', r'OS X[:\s\-]+(.+?)(?=Linux|Windows|$)']),
                    ("linux", [r'Linux[:\s\-]+(.+?)(?=Mac|Windows|$)'])
                ]:
                    for pat in patterns:
                        m = re.search(pat, req_text, re.DOTALL | re.IGNORECASE)
                        if m:
                            details[f"system_requirements_{os_name}"] = m.group(1).strip()[:800]
                            break
        except:
            pass

        # ======= MEDIA DOWNLOAD =======
        media_dir = None
        if download_media_flag:
            safe = sanitize_filename(title) or "game"
            media_dir = os.path.join("scraped_data", "game_media_gog", safe)
            os.makedirs(media_dir, exist_ok=True)

            # HEADER IMAGE - multiple fallbacks
            header_selectors = [
                "img[src*='cover']",
                "picture source[srcset*='cover']",
                "meta[property='og:image']",
                "img[class*='hero']",
                ".productcard-cover img",
                "img[src*='/cover/']",
                "img[class*='cover']"
            ]
            header_saved = False
            for sel in header_selectors:
                try:
                    if sel.startswith("meta"):
                        elem = driver.find_element(By.CSS_SELECTOR, sel)
                        img_url = safe_getattr(elem, "content") or safe_getattr(elem, "src")
                    else:
                        elem = driver.find_element(By.CSS_SELECTOR, sel)
                        img_url = safe_getattr(elem, "src") or safe_getattr(elem, "srcset") or safe_getattr(elem, "data-src")
                    if img_url and img_url.startswith("http"):
                        # attempt to get a higher-res variant
                        img_url = re.sub(r'([_-])(256|512)\.', r'\g<1>1024.', img_url)
                        details["header_image"] = img_url
                        dl = download_media(img_url, media_dir, "header.jpg")
                        if dl:
                            details["downloaded_images"].append(dl)
                        header_saved = True
                        break
                except:
                    continue

            # SCREENSHOTS / GALLERY
            try:
                screenshot_selectors = [
                    "[data-testid='media-gallery'] img",
                    "img[src*='screenshots']",
                    "img[src*='/gallery/']",
                    ".media-gallery img",
                    ".screenshot img",
                    ".screenshot"
                ]
                screenshots = []
                for sel in screenshot_selectors:
                    elems = driver.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        for idx, img in enumerate(elems[:MAX_SCREENSHOTS]):
                            url = safe_getattr(img, "src") or safe_getattr(img, "srcset") or safe_getattr(img, "data-src")
                            if not url:
                                continue
                            if not url.startswith("http"):
                                continue
                            url = re.sub(r'([_-])(256|512)\.', r'\g<1>1024.', url)
                            if url in screenshots:
                                continue
                            screenshots.append(url)
                            dl = download_media(url, media_dir, f"screenshot_{len(screenshots)}.jpg")
                            if dl:
                                details["downloaded_images"].append(dl)
                        if screenshots:
                            details["screenshots"] = ", ".join(screenshots)
                        break
            except:
                pass

            # VIDEOS / TRAILERS
            try:
                vid_selectors = [
                    "video source",
                    "video[src]",
                    "iframe[src*='youtube']",
                    "iframe[src*='vimeo']",
                    "a[href*='.mp4']"
                ]
                vids = []
                for sel in vid_selectors:
                    elems = driver.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        for elem in elems[:MAX_VIDEOS]:
                            url = safe_getattr(elem, "src") or safe_getattr(elem, "href") or safe_getattr(elem, "data-src")
                            if not url:
                                continue
                            if url in vids:
                                continue
                            vids.append(url)
                            # Download only direct mp4 links (youtube/vimeo iframes are stored as links)
                            if url.lower().endswith(".mp4"):
                                dl = download_media(url, media_dir, f"video_{len(details['downloaded_videos'])+1}.mp4")
                                if dl:
                                    details["downloaded_videos"].append(dl)
                        if vids:
                            details["videos"] = ", ".join(vids)
                        break
            except:
                pass

        # done
    except Exception:
        pass

    return details

# -----------------------
# Page worker
# -----------------------
def scrape_pages(worker_id: int, start_page: int, end_page: int, headless=True, scrape_details=True, download_media=True):
    driver = create_driver(headless=headless)
    local_results = []
    try:
        for page in range(start_page, end_page + 1):
            try:
                list_url = f"https://www.gog.com/en/games?order=desc:releaseDate&page={page}"
                print(f"[W{worker_id}] Fetching page {page} -> {list_url}")
                driver.get(list_url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                rand_sleep()

                # dismiss cookies if present
                handle_cookies(driver)

                # Scroll to trigger lazy load
                for y in (800, 1500, 2500):
                    driver.execute_script(f"window.scrollTo(0, {y});")
                    rand_sleep(0.3, 0.8)

                # Find cards
                card_selectors = [
                    "a.product-tile__content",
                    "a[href*='/game/']",
                    "[class*='product-tile']",
                    "[data-testid='product-tile']"
                ]
                cards = []
                for sel in card_selectors:
                    try:
                        cards = driver.find_elements(By.CSS_SELECTOR, sel)
                        if cards:
                            break
                    except:
                        continue

                if not cards:
                    print(f"[W{worker_id}] Page {page}: no cards found")
                    continue

                print(f"[W{worker_id}] Page {page}: found {len(cards)} cards")
                for card in cards:
                    g = scrape_game_card(card)
                    if not g:
                        continue
                    # Get details if requested
                    if scrape_details and g.get("url"):
                        details = extract_details_from_page(driver, g["url"], g.get("title", "N/A"), download_media_flag=download_media)
                        # merge
                        merged = {**g, **details}
                    else:
                        merged = g
                    local_results.append(merged)
                    rand_sleep(0.2, 0.8)

                print(f"[W{worker_id}] Page {page}: collected {len(local_results)} items so far")
                rand_sleep(0.5, 1.4)
            except Exception as e:
                print(f"[W{worker_id}] Page {page} error: {e}")
                continue
    finally:
        driver.quit()

    # append to global list
    with data_lock:
        all_game_data.extend(local_results)
    print(f"[W{worker_id}] Finished pages {start_page}-{end_page} -> {len(local_results)} games")
    return local_results

# -----------------------
# Orchestrator
# -----------------------
def scrape_gog_games(max_games=DEFAULT_MAX_GAMES, num_workers=DEFAULT_WORKERS, headless=DEFAULT_HEADLESS,
                     scrape_details=True, download_media=True):
    global all_game_data
    all_game_data = []

    total_pages = max(1, (max_games + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
    pages_per_worker = max(1, (total_pages + num_workers - 1) // num_workers)

    print(f"Starting GOG scrape: max_games={max_games}, pages={total_pages}, workers={num_workers}")
    with ThreadPoolExecutor(max_workers=num_workers) as exe:
        futures = []
        for i in range(num_workers):
            sp = i * pages_per_worker + 1
            ep = min(total_pages, sp + pages_per_worker - 1)
            if sp > total_pages:
                break
            futures.append(exe.submit(scrape_pages, i + 1, sp, ep, headless, scrape_details, download_media))

        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print("Worker error:", e)

    # postprocess global results and write CSV
    if not all_game_data:
        print("No games scraped.")
        return []

    df = pd.DataFrame(all_game_data)
    # dedupe by URL if present
    if 'url' in df.columns:
        df = df.drop_duplicates(subset=['url'], keep='first')
    out_dir = Path("scraped_data")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "gog_games_detailed.csv"
    # ensure some expected columns exist
    req_cols = [
        'title', 'release_date', 'original_price', 'price', 'discount_percentage',
        'review_summary', 'url', 'platforms', 'genres', 'categories', 'multiplayer',
        'singleplayer', 'developer', 'publisher', 'description',
        'system_requirements_windows', 'system_requirements_mac', 'system_requirements_linux',
        'header_image', 'screenshots', 'videos', 'downloaded_images', 'downloaded_videos'
    ]
    for c in req_cols:
        if c not in df.columns:
            df[c] = "N/A"
    df = df[req_cols]
    df.to_csv(out_file, index=False, encoding='utf-8-sig')
    print(f"Saved CSV: {out_file} (games={len(df)})")
    return df.to_dict(orient='records')

# -----------------------
# CLI
# -----------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GOG scraper - downloads media (header, screenshots, mp4)")
    parser.add_argument("--max-games", type=int, default=DEFAULT_MAX_GAMES, help="Maximum number of games to attempt")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of concurrent page workers")
    parser.add_argument("--headless", action="store_true", default=DEFAULT_HEADLESS, help="Run browser headless")
    parser.add_argument("--no-media", action="store_true", help="Do not download media (images/videos)")
    parser.add_argument("--no-details", action="store_true", help="Do not visit individual game detail pages")
    parser.add_argument("--debug", action="store_true", help="Run in non-headless mode and verbose")
    args = parser.parse_args()

    headless = args.headless and not args.debug
    download_media = not args.no_media
    scrape_details = not args.no_details

    if args.debug:
        print("DEBUG: headless disabled for easier troubleshooting")
        headless = False

    start = time.perf_counter()
    scrape_gog_games(max_games=50, num_workers=15, headless=True, scrape_details=True, download_media=True)
    end = time.perf_counter()
    print(f"Total execution time: {end - start:.4f} seconds")