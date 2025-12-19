#!/usr/bin/env python3
"""
GOG Scraper v3.0 - Complete Fixed Version
Properly extracts: ratings, review counts, descriptions, genres, publishers, dates, and all media
"""

import os, re, time, random, asyncio
from pathlib import Path
import requests
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

CFG = {
    'workers': 3,
    'headless': True,
    'page_timeout': 30000,
    'wait_after_load': 2000,
    'max_screenshots': 10,
    'max_videos': 5,
    'download_media': True,
}

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def sanitize(name, maxlen=80):
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', '', name).strip()[:maxlen]

def parse_price(txt):
    if not txt: return "N/A", "N/A", "N/A"
    txt = txt.strip()
    if 'free' in txt.lower(): return "Free", "N/A", "N/A"
    disc = re.search(r'-(\d+)%', txt)
    prices = re.findall(r'[€$£¥]\s*[\d,]+\.?\d*', txt)
    return (prices[0].strip() if prices else "N/A",
            prices[1].strip() if len(prices) > 1 else "N/A",
            disc.group(1) + "%" if disc else "N/A")

def download_file(url, path, timeout=20):
    if not url or url == "N/A" or os.path.exists(path):
        return path if os.path.exists(path) else None
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        r = requests.get(url, stream=True, timeout=timeout, headers=headers)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            return path
    except: pass
    return None

async def scrape_list_page(page, page_num, wid):
    """Scrape game list from a catalog page"""
    try:
        url = f"https://www.gog.com/en/games?order=desc:releaseDate&page={page_num}"
        log(f"W{wid} → Page {page_num}")
        
        await page.goto(url, wait_until="domcontentloaded", timeout=CFG['page_timeout'])
        
        # Handle cookies
        try:
            cookie_btn = page.locator("button.cookie-consent__accept, #onetrust-accept-btn-handler").first
            if await cookie_btn.is_visible(timeout=2000):
                await cookie_btn.click()
                await page.wait_for_timeout(500)
        except: pass
        
        # Wait for games to load
        await page.wait_for_selector("a[href*='/game/']", timeout=15000)
        await page.wait_for_timeout(CFG['wait_after_load'])
        
        # Scroll to load lazy content
        for i in range(6):
            await page.evaluate(f"window.scrollTo(0, {i * 900})")
            await page.wait_for_timeout(400)
        
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(500)
        
        # Get all game cards
        game_cards = await page.locator("[class*='product-tile'], [class*='game-card'], a[href*='/game/']").all()
        
        games = []
        seen_urls = set()
        
        for card in game_cards:
            try:
                # Get URL
                href = await card.get_attribute("href")
                if not href or '/game/' not in href:
                    continue
                
                url = href if href.startswith("http") else f"https://www.gog.com{href}"
                
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract status tag and filter out DLCs/Microtransactions
                status_tag = ""
                should_skip = False
                try:
                    badge = card.locator("[class*='badge'], [class*='label'], [class*='tag']").first
                    status_text = await badge.text_content(timeout=500)
                    if status_text:
                        status_text = status_text.strip().upper()
                        
                        # Skip DLCs and microtransactions
                        if any(x in status_text for x in ['DLC', 'MICROTRANSACTION', 'MICRO TRANSACTION', 'ADD-ON', 'EXPANSION']):
                            should_skip = True
                        
                        if any(x in status_text for x in ['SOON', 'PRE-ORDER', 'MOD', 'COMING']):
                            status_tag = status_text
                except: pass
                
                # Skip this game if it's a DLC or microtransaction
                if should_skip:
                    continue
                
                # Extract title and check for DLC keywords
                title = None
                try:
                    title_elem = card.locator(".product-title, [class*='title'], h3, h2").first
                    title = await title_elem.text_content(timeout=500)
                    title = title.strip() if title else None
                except: pass
                
                if not title:
                    try:
                        title = await card.get_attribute("aria-label")
                    except: pass
                
                if not title:
                    game_slug = url.split('/game/')[-1].strip('/')
                    title = game_slug.replace('_', ' ').replace('-', ' ').title()
                
                # Skip if title contains DLC indicators
                if title:
                    title_upper = title.upper()
                    dlc_keywords = ['DLC', ' - DLC', 'EXPANSION PACK', 'SEASON PASS', 
                                    'MICRO TRANSACTION', 'MICROTRANSACTION', 'ADD-ON',
                                    'CONTENT PACK', 'BONUS CONTENT', 'DELUXE UPGRADE']
                    
                    if any(keyword in title_upper for keyword in dlc_keywords):
                        continue
                
                if status_tag and not title.startswith(status_tag):
                    title = f"{status_tag}   {title}"
                
                # Extract price
                price, orig, disc = "N/A", "N/A", "N/A"
                try:
                    price_elem = card.locator("[class*='price'], .price-value").first
                    price_text = await price_elem.text_content(timeout=500)
                    price, orig, disc = parse_price(price_text)
                except: pass
                
                games.append({
                    "title": title,
                    "url": url,
                    "price": price,
                    "original_price": orig,
                    "discount_percentage": disc,
                    "status_tag": status_tag
                })
                
            except Exception as e:
                continue
        
        log(f"W{wid} → Page {page_num}: Found {len(games)} games")
        return games
        
    except Exception as e:
        log(f"W{wid} → Page {page_num} ERROR: {e}")
        return []

async def scrape_game_details(page, url, title, wid):
    """Scrape full details from game page - FIXED VERSION"""
    details = {
        "rating": "N/A",
        "rating_count": "N/A",
        "release_date": "N/A",
        "genres": "N/A",
        "platforms": "N/A",
        "developer": "N/A",
        "publisher": "N/A",
        "description": "N/A",
        "screenshots": [],
        "videos": [],
        "header_image": "N/A"
    }
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=CFG['page_timeout'])
        await page.wait_for_timeout(2000)
        
        # Handle cookies
        try:
            cookie_btn = page.locator("button.cookie-consent__accept, #onetrust-accept-btn-handler").first
            if await cookie_btn.is_visible(timeout=1000):
                await cookie_btn.click()
                await page.wait_for_timeout(300)
        except: pass
        
        # Scroll to load all content
        for i in range(5):
            await page.evaluate(f"window.scrollTo(0, {i * 1200})")
            await page.wait_for_timeout(400)
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(800)
        
        # === RATING - FIXED EXTRACTION ===
        # Method 1: productcard-rating__score (most reliable)
        try:
            score_elem = page.locator(".productcard-rating__score--version-a, .productcard-rating__score--version-b").first
            score_text = await score_elem.text_content(timeout=2000)
            if score_text:
                # Extract just the number (handles "4.6/5" or "4.6")
                rating_match = re.search(r'([\d.]+)', score_text.strip())
                if rating_match:
                    details["rating"] = rating_match.group(1)
        except: pass
        
        # Method 2: Inline rating in content-summary
        if details["rating"] == "N/A":
            try:
                inline_rating = page.locator(".productcard-rating--inline .rating").first
                rating_text = await inline_rating.text_content(timeout=1000)
                if rating_text:
                    rating_match = re.search(r'([\d.]+)', rating_text.strip())
                    if rating_match:
                        details["rating"] = rating_match.group(1)
            except: pass
        
        # === RATING COUNT - FIXED EXTRACTION ===
        try:
            # Look for review count in multiple locations
            review_selectors = [
                ".productcard-rating__details-reviews--version-a",
                ".productcard-rating__details-reviews--version-b",
                ".productcard-rating__details"
            ]
            
            for selector in review_selectors:
                try:
                    review_elem = page.locator(selector).first
                    review_text = await review_elem.text_content(timeout=1000)
                    if review_text:
                        # Extract number from "76 Reviews" or "(76 Reviews)"
                        count_match = re.search(r'(\d+)\s*Review', review_text)
                        if count_match:
                            details["rating_count"] = count_match.group(1)
                            break
                except: continue
        except: pass
        
        # === DESCRIPTION - FIXED EXTRACTION ===
        try:
            # Method 1: Content summary description
            desc_elem = page.locator(".content-summary-item__description").first
            desc = await desc_elem.text_content(timeout=2000)
            
            if desc and len(desc.strip()) > 50:
                desc = desc.strip()
                # Remove ellipsis and extra whitespace
                desc = re.sub(r'\.\.\.+$', '', desc)
                desc = re.sub(r'\s+', ' ', desc).strip()
                
                # Remove common UI text
                junk_phrases = [
                    "Discover the grim dark universes",
                    "Originally released in",
                    "See new chat messages",
                    "friend invites"
                ]
                for junk in junk_phrases:
                    if junk in desc:
                        desc = desc.split(junk)[0].strip()
                
                if len(desc) > 50:
                    details["description"] = desc[:1000]
        except: pass
        
        # Fallback: Meta description
        if details["description"] == "N/A" or len(details["description"]) < 50:
            try:
                meta_desc = await page.locator("meta[property='og:description'], meta[name='description']").first.get_attribute("content", timeout=1000)
                if meta_desc and len(meta_desc.strip()) > 50:
                    details["description"] = meta_desc.strip()[:1000]
            except: pass
        
        # === GENRES - FIXED EXTRACTION ===
        try:
            # Method 1: From details table row
            genre_row = page.locator(".table__row.details__row").filter(has=page.locator("text=/Genre:/i")).first
            genre_links = await genre_row.locator(".details__link, a").all()
            
            genres = []
            for link in genre_links:
                text = await link.text_content(timeout=300)
                if text:
                    text = text.strip()
                    if text and len(text) < 40 and text not in ['-', ',', '&']:
                        genres.append(text)
            
            if genres:
                details["genres"] = ", ".join(genres[:10])
        except: pass
        
        # Fallback: Genre links
        if details["genres"] == "N/A":
            try:
                genre_links = await page.locator("a[href*='/games?genres=']").all()
                genres = []
                for link in genre_links[:10]:
                    text = await link.text_content(timeout=300)
                    if text and len(text.strip()) < 30:
                        genres.append(text.strip())
                if genres:
                    details["genres"] = ", ".join(genres)
            except: pass
        
        # === OTHER DETAILS FROM TABLE ===
        try:
            rows = await page.locator(".table__row.details__row, .details__row").all()
            
            for row in rows:
                try:
                    # Get the label/category
                    label_elem = row.locator(".details__category, .table__row-label").first
                    label = await label_elem.text_content(timeout=500)
                    
                    if not label:
                        continue
                    
                    label = label.strip().lower()
                    
                    # Release date
                    if 'release' in label:
                        content_elem = row.locator(".details__content, .table__row-content").first
                        content = await content_elem.text_content(timeout=500)
                        if content:
                            details["release_date"] = content.strip()
                    
                    # Company (Developer/Publisher)
                    elif 'company' in label or 'developer' in label:
                        links = await row.locator(".details__content a, .table__row-content a").all()
                        
                        if links:
                            # First link is developer
                            dev_text = await links[0].text_content(timeout=300)
                            if dev_text and dev_text.strip():
                                details["developer"] = dev_text.strip()
                            
                            # Second link is publisher (if exists)
                            if len(links) > 1:
                                pub_text = await links[1].text_content(timeout=300)
                                if pub_text and pub_text.strip():
                                    details["publisher"] = pub_text.strip()
                    
                    # Publisher (standalone)
                    elif 'publisher' in label and details["publisher"] == "N/A":
                        content_elem = row.locator(".details__content a, .table__row-content a").first
                        content = await content_elem.text_content(timeout=500)
                        if content and content.strip():
                            details["publisher"] = content.strip()
                    
                    # Platforms
                    elif 'works on' in label or 'system' in label:
                        content = await row.text_content(timeout=500)
                        plats = []
                        cl = content.lower()
                        if 'windows' in cl: plats.append("Windows")
                        if 'mac' in cl or 'os x' in cl: plats.append("Mac")
                        if 'linux' in cl: plats.append("Linux")
                        if plats:
                            details["platforms"] = ", ".join(plats)
                
                except: continue
        except: pass
        
        # === PLATFORMS FALLBACK ===
        if details["platforms"] == "N/A":
            try:
                # Check for OS icons
                os_icons = await page.locator(".productcard-os-support__system").all()
                plats = []
                for icon in os_icons:
                    class_attr = await icon.get_attribute("class")
                    if 'windows' in class_attr.lower(): plats.append("Windows")
                    if 'mac' in class_attr.lower(): plats.append("Mac")
                    if 'linux' in class_attr.lower(): plats.append("Linux")
                if plats:
                    details["platforms"] = ", ".join(list(dict.fromkeys(plats)))
            except: pass
        
        # === HEADER IMAGE ===
        try:
            img = await page.locator("meta[property='og:image']").first.get_attribute("content", timeout=2000)
            if img and img.startswith("http"):
                details["header_image"] = img
        except:
            try:
                img = await page.locator("img[src*='cover'], .productcard-cover img, [class*='hero-image'] img").first.get_attribute("src", timeout=2000)
                if img:
                    if not img.startswith("http"):
                        img = f"https:{img}" if img.startswith("//") else f"https://www.gog.com{img}"
                    details["header_image"] = img
            except: pass
        
        # === SCREENSHOTS ===
        try:
            img_selectors = [
                "img[src*='screenshots']",
                "img[src*='/gallery/']",
                ".media-gallery img",
                "[class*='screenshot'] img"
            ]
            
            for selector in img_selectors:
                img_elems = await page.locator(selector).all()
                for img in img_elems:
                    if len(details["screenshots"]) >= CFG['max_screenshots']:
                        break
                    
                    src = await img.get_attribute("src")
                    if src:
                        if src.startswith("//"):
                            src = f"https:{src}"
                        elif src.startswith("/"):
                            src = f"https://www.gog.com{src}"
                        
                        src = re.sub(r'([_-])(256|512|thumb)\.', r'\g<1>1024.', src)
                        
                        if src.startswith("http") and src not in details["screenshots"]:
                            details["screenshots"].append(src)
                
                if details["screenshots"]:
                    break
        except: pass
        
        # === VIDEOS ===
        try:
            video_selectors = [
                "video source[src]",
                "video[src]",
                "source[src*='.mp4']",
                "source[src*='.webm']"
            ]
            
            for selector in video_selectors:
                vid_elems = await page.locator(selector).all()
                for vid in vid_elems:
                    if len(details["videos"]) >= CFG['max_videos']:
                        break
                    
                    src = await vid.get_attribute("src")
                    
                    if src:
                        if src.startswith("//"):
                            src = f"https:{src}"
                        elif src.startswith("/"):
                            src = f"https://www.gog.com{src}"
                        
                        if src not in details["videos"] and any(ext in src.lower() for ext in ['.mp4', '.webm']):
                            details["videos"].append(src)
                
                if details["videos"]:
                    break
        except: pass
        
        return details
        
    except Exception as e:
        log(f"W{wid} ⚠️  Detail error for {title}: {str(e)[:80]}")
        return details

async def worker(context, pages_to_scrape, wid):
    """Worker that processes assigned pages"""
    page = await context.new_page()
    all_games = []
    
    try:
        for page_num in pages_to_scrape:
            games = await scrape_list_page(page, page_num, wid)
            
            for idx, game in enumerate(games, 1):
                try:
                    details = await scrape_game_details(page, game['url'], game['title'], wid)
                    game.update(details)
                    
                    if CFG['download_media']:
                        game = download_media(game)
                    
                    all_games.append(game)
                    
                    if idx % 3 == 0:
                        log(f"W{wid} → Page {page_num}: {idx}/{len(games)} games")
                    
                    await page.wait_for_timeout(random.randint(400, 900))
                    
                except Exception as e:
                    log(f"W{wid} ⚠️  Error on {game.get('title', 'Unknown')}: {str(e)[:40]}")
                    all_games.append(game)
                    continue
            
            log(f"W{wid} → Page {page_num}: ✓ {len(games)} games (Total: {len(all_games)})")
            await page.wait_for_timeout(random.randint(2000, 4000))
        
    finally:
        await page.close()
    
    log(f"W{wid} → FINISHED: {len(all_games)} games")
    return all_games

def download_media(game_data, base_dir="scraped_data/game_media_gog"):
    """Download screenshots and videos"""
    if not CFG['download_media']:
        return game_data
    
    safe_title = sanitize(game_data.get("title", "game"))
    media_dir = os.path.join(base_dir, safe_title)
    
    downloaded_images = []
    downloaded_videos = []
    
    # Header
    if game_data.get("header_image") and game_data["header_image"] != "N/A":
        path = os.path.join(media_dir, "header.jpg")
        if download_file(game_data["header_image"], path):
            downloaded_images.append(path)
    
    # Screenshots
    screenshots = game_data.get("screenshots", [])
    if isinstance(screenshots, list):
        for idx, url in enumerate(screenshots, 1):
            path = os.path.join(media_dir, f"screenshot_{idx}.jpg")
            if download_file(url, path):
                downloaded_images.append(path)
    
    # Videos
    videos = game_data.get("videos", [])
    if isinstance(videos, list):
        for idx, url in enumerate(videos, 1):
            ext = ".mp4" if ".mp4" in url.lower() else ".webm"
            path = os.path.join(media_dir, f"video_{idx}{ext}")
            if download_file(url, path):
                downloaded_videos.append(path)
    
    game_data["downloaded_images"] = ", ".join(downloaded_images) if downloaded_images else "N/A"
    game_data["downloaded_videos"] = ", ".join(downloaded_videos) if downloaded_videos else "N/A"
    
    return game_data

async def scrape(pages=11, workers=3, headless=True, download_media=True):
    """Main scraping function"""
    CFG['workers'] = workers
    CFG['headless'] = headless
    CFG['download_media'] = download_media
    
    log(f"🚀 GOG Scraper v3.0 - Complete Fixed Edition")
    log(f"📊 Pages: {pages} (~{pages * 48} games)")
    log(f"👷 Workers: {workers}")
    log(f"💾 Download media: {'YES' if download_media else 'NO'}")
    log(f"🚫 Filtering: DLCs, expansions, and microtransactions excluded")
    
    start = time.time()
    all_games = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        pages_per_worker = max(1, pages // workers)
        tasks = []
        
        for i in range(workers):
            start_page = i * pages_per_worker + 1
            end_page = min(pages, start_page + pages_per_worker - 1) if i < workers - 1 else pages
            if start_page > pages:
                break
            
            worker_pages = list(range(start_page, end_page + 1))
            tasks.append(worker(context, worker_pages, i + 1))
        
        results = await asyncio.gather(*tasks)
        
        for result in results:
            all_games.extend(result)
        
        await browser.close()
    
    elapsed = time.time() - start
    
    if not all_games:
        log("❌ No games scraped")
        return []
    
    df = pd.DataFrame(all_games)
    
    # Dedupe
    if 'url' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        if before > len(df):
            log(f"🗑️  Removed {before - len(df)} duplicates")
    
    # Convert lists to strings
    for col in ['screenshots', 'videos']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    
    # Column order
    cols_order = ['title', 'url', 'price', 'original_price', 'discount_percentage', 
                  'rating', 'rating_count', 'release_date', 'genres', 'platforms', 
                  'developer', 'publisher', 'description', 'status_tag',
                  'screenshots', 'videos', 'header_image', 
                  'downloaded_images', 'downloaded_videos']
    
    df = df[[c for c in cols_order if c in df.columns]]
    
    # Save
    out_dir = Path("scraped_data")
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / "gog_games_complete.csv"
    
    df.to_csv(out_file, index=False, encoding='utf-8-sig')
    
    # Stats
    log(f"\n{'='*70}")
    log(f"✅ SUCCESS: {len(df)} games in {elapsed:.1f}s ({len(df)/elapsed:.2f} games/s)")
    log(f"💾 Saved: {out_file}")
    
    stats = {
        'Ratings': len(df[df['rating'] != 'N/A']),
        'Rating Counts': len(df[df['rating_count'] != 'N/A']),
        'Descriptions': len(df[(df['description'] != 'N/A') & (df['description'].str.len() > 100)]),
        'Genres': len(df[df['genres'] != 'N/A']),
        'Platforms': len(df[df['platforms'] != 'N/A']),
        'Developer': len(df[df['developer'] != 'N/A']),
        'Publisher': len(df[df['publisher'] != 'N/A']),
        'Screenshots': len(df[df['screenshots'].str.len() > 10]),
        'Videos': len(df[df['videos'].str.len() > 10])
    }
    
    log(f"\n📈 Data Quality:")
    for key, val in stats.items():
        pct = 100 * val / len(df)
        log(f"   {key}: {val}/{len(df)} ({pct:.1f}%)")
    
    log(f"{'='*70}\n")
    
    # Sample
    if len(df) > 0:
        print("\n📋 Sample (First 3 games):")
        sample_cols = ['title', 'rating', 'rating_count', 'genres', 'platforms', 'developer']
        sample_cols = [c for c in sample_cols if c in df.columns]
        print(df[sample_cols].head(3).to_string(index=False, max_colwidth=50))
    
    return df.to_dict(orient='records')

def main():
    import argparse
    p = argparse.ArgumentParser(description="GOG Scraper v3.0 - Complete Fixed")
    p.add_argument("--pages", type=int, default=11, help="Pages to scrape")
    p.add_argument("--workers", type=int, default=3, help="Concurrent workers")
    p.add_argument("--no-headless", action="store_true", help="Show browser")
    p.add_argument("--no-media", action="store_true", help="Skip media download")
    args = p.parse_args()
    
    asyncio.run(scrape(
        pages=args.pages,
        workers=args.workers,
        headless=not args.no_headless,
        download_media=not args.no_media
    ))

if __name__ == "__main__":
    main()

# Usage examples:
# python gog_scraper.py --pages 15 --workers 4
# python gog_scraper.py --pages 5 --no-headless  # See browser for debugging
# python gog_scraper.py --pages 20 --workers 5 --no-media  # Skip downloads