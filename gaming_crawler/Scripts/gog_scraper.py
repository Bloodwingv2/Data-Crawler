#!/usr/bin/env python3
"""
GOG Scraper - Playwright Version (MUCH better than Selenium!)
Faster, more reliable, better dynamic content handling

Install: pip install playwright pandas requests
Then: playwright install chromium
"""

import os, re, time, random, asyncio, json
from pathlib import Path
import requests
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

CFG = {
    'workers': 3,
    'headless': True,
    'page_timeout': 30000,
    'wait_after_load': 2000,
    'max_screenshots': 6,
    'max_videos': 3,
    'download_media': True,
}

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def sanitize(name, maxlen=80):
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', '', name).strip()[:maxlen]

def parse_price(txt):
    if not txt: return "N/A", "N/A", "N/A"
    txt = txt.strip().lower()
    if 'free' in txt: return "Free", "N/A", "N/A"
    disc = re.search(r'-(\d+)%', txt)
    prices = re.findall(r'[€$£¥]\s*[\d,]+\.?\d*', txt)
    return (prices[0].strip() if prices else "N/A",
            prices[1].strip() if len(prices) > 1 else "N/A",
            disc.group(1) + "%" if disc else "N/A")

def download_file(url, path, timeout=15):
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
        for i in range(5):
            await page.evaluate(f"window.scrollTo(0, {i * 800})")
            await page.wait_for_timeout(300)
        
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(500)
        
        # Get all game links
        game_links = await page.locator("a[href*='/game/']").all()
        
        # Extract unique games
        games = []
        seen_urls = set()
        
        for link in game_links:
            try:
                href = await link.get_attribute("href")
                if not href or '/game/' not in href:
                    continue
                
                url = href if href.startswith("http") else f"https://www.gog.com{href}"
                
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # Extract title
                title = None
                try:
                    title_elem = link.locator(".product-title, [class*='title']").first
                    title = await title_elem.text_content(timeout=500)
                    title = title.strip() if title else None
                except: pass
                
                if not title:
                    try:
                        title = await link.get_attribute("aria-label")
                    except: pass
                
                if not title:
                    game_slug = url.split('/game/')[-1].strip('/')
                    title = game_slug.replace('_', ' ').replace('-', ' ').title()
                
                # Extract price
                price, orig, disc = "N/A", "N/A", "N/A"
                try:
                    price_elem = link.locator("[class*='price']").first
                    price_text = await price_elem.text_content(timeout=500)
                    price, orig, disc = parse_price(price_text)
                except: pass
                
                games.append({
                    "title": title,
                    "url": url,
                    "price": price,
                    "original_price": orig,
                    "discount_percentage": disc
                })
                
            except Exception as e:
                continue
        
        log(f"W{wid} → Page {page_num}: Found {len(games)} games")
        return games
        
    except Exception as e:
        log(f"W{wid} → Page {page_num} ERROR: {e}")
        return []

async def scrape_game_details(page, url, title, wid):
    """Scrape full details from game page"""
    details = {
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
        await page.wait_for_timeout(1500)
        
        # Handle cookies
        try:
            cookie_btn = page.locator("button.cookie-consent__accept").first
            if await cookie_btn.is_visible(timeout=1000):
                await cookie_btn.click()
                await page.wait_for_timeout(300)
        except: pass
        
        # Scroll to load content
        for i in range(4):
            await page.evaluate(f"window.scrollTo(0, {i * 1000})")
            await page.wait_for_timeout(300)
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(500)
        
        # Extract details from rows
        try:
            rows = await page.locator(".details__row, [class*='details-row'], .table__row").all()
            
            for row in rows:
                try:
                    label = await row.locator(".details__category, .label, [class*='category']").first.text_content(timeout=500)
                    content = await row.locator(".details__content, .value, [class*='content']").first.text_content(timeout=500)
                    
                    if not label or not content:
                        continue
                    
                    label = label.strip().lower()
                    content = content.strip()
                    
                    if 'genre' in label and content:
                        genres = [g.strip() for g in content.split('\n') if g.strip()]
                        details["genres"] = ", ".join(genres[:10])
                    
                    elif 'release' in label:
                        details["release_date"] = content
                    
                    elif 'developer' in label or 'company' in label:
                        parts = [p.strip() for p in content.split('\n') if p.strip()]
                        if parts: details["developer"] = parts[0]
                        if len(parts) > 1: details["publisher"] = parts[1]
                    
                    elif 'publisher' in label and details["publisher"] == "N/A":
                        details["publisher"] = content
                    
                    elif 'works on' in label or 'platform' in label:
                        plats = []
                        cl = content.lower()
                        if 'windows' in cl: plats.append("Windows")
                        if 'mac' in cl or 'os x' in cl: plats.append("Mac")
                        if 'linux' in cl: plats.append("Linux")
                        if plats: details["platforms"] = ", ".join(plats)
                
                except: continue
        except: pass
        
        # Fallback: genres from tags
        if details["genres"] == "N/A":
            try:
                genre_links = await page.locator("a[href*='/games?genres='], .tag, [class*='genre']").all()
                genres = []
                for link in genre_links[:10]:
                    text = await link.text_content(timeout=300)
                    if text and len(text.strip()) < 30:
                        genres.append(text.strip())
                if genres:
                    details["genres"] = ", ".join(genres)
            except: pass
        
        # Fallback: platforms from icons
        if details["platforms"] == "N/A":
            try:
                icons = await page.locator("[class*='platform'], [class*='os-icon']").all()
                plats = []
                for icon in icons:
                    cls = await icon.get_attribute("class") or ""
                    title_attr = await icon.get_attribute("title") or ""
                    combined = (cls + " " + title_attr).lower()
                    if 'windows' in combined and "Windows" not in plats: plats.append("Windows")
                    if ('mac' in combined or 'apple' in combined) and "Mac" not in plats: plats.append("Mac")
                    if 'linux' in combined and "Linux" not in plats: plats.append("Linux")
                if plats: details["platforms"] = ", ".join(plats)
            except: pass
        
        # Description
        try:
            desc_elem = page.locator(".description, [class*='description'], .game-description").first
            desc = await desc_elem.text_content(timeout=2000)
            if desc and len(desc.strip()) > 50:
                details["description"] = desc.strip()[:800]
        except: pass
        
        # Header image
        try:
            img = await page.locator("meta[property='og:image']").first.get_attribute("content", timeout=2000)
            if img and img.startswith("http"):
                details["header_image"] = img
        except:
            try:
                img = await page.locator("img[src*='cover']").first.get_attribute("src", timeout=2000)
                if img and img.startswith("http"):
                    details["header_image"] = img
            except: pass
        
        # Screenshots
        try:
            img_elems = await page.locator("img[src*='screenshots'], img[src*='/gallery/'], .media-gallery img").all()
            for img in img_elems[:CFG['max_screenshots']]:
                src = await img.get_attribute("src")
                if src and src.startswith("http") and src not in details["screenshots"]:
                    src = re.sub(r'([_-])(256|512|thumb)\.', r'\g<1>1024.', src)
                    details["screenshots"].append(src)
        except: pass
        
        # Videos
        try:
            video_elems = await page.locator("video source, video[src], source[src*='.mp4']").all()
            for vid in video_elems[:CFG['max_videos']]:
                src = await vid.get_attribute("src")
                if src and src not in details["videos"]:
                    details["videos"].append(src)
        except: pass
        
        return details
        
    except Exception as e:
        log(f"W{wid} ⚠️  Detail error for {title}: {str(e)[:50]}")
        return details

async def worker(context, pages_to_scrape, wid):
    """Worker that processes assigned pages"""
    page = await context.new_page()
    all_games = []
    
    try:
        for page_num in pages_to_scrape:
            # Get list of games
            games = await scrape_list_page(page, page_num, wid)
            
            # Get details for each game
            for idx, game in enumerate(games, 1):
                try:
                    details = await scrape_game_details(page, game['url'], game['title'], wid)
                    game.update(details)
                    
                    # Download media
                    if CFG['download_media']:
                        game = download_media(game)
                    
                    all_games.append(game)
                    
                    if idx % 5 == 0:
                        log(f"W{wid} → Page {page_num}: {idx}/{len(games)} games processed")
                    
                    await page.wait_for_timeout(random.randint(300, 700))
                    
                except Exception as e:
                    log(f"W{wid} ⚠️  Error on {game.get('title', 'Unknown')}: {str(e)[:30]}")
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
            if url.lower().endswith('.mp4'):
                path = os.path.join(media_dir, f"video_{idx}.mp4")
                if download_file(url, path):
                    downloaded_videos.append(path)
    
    game_data["downloaded_images"] = downloaded_images
    game_data["downloaded_videos"] = downloaded_videos
    
    return game_data

async def scrape(pages=11, workers=3, headless=True, download_media=True):
    """Main scraping function"""
    CFG['workers'] = workers
    CFG['headless'] = headless
    CFG['download_media'] = download_media
    
    log(f"🚀 GOG Scraper - Playwright Edition")
    log(f"📊 Pages: {pages} (~{pages * 48} games)")
    log(f"👷 Workers: {workers}")
    log(f"💾 Download media: {'YES' if download_media else 'NO'}")
    
    start = time.time()
    all_games = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        # Distribute pages among workers
        pages_per_worker = max(1, pages // workers)
        tasks = []
        
        for i in range(workers):
            start_page = i * pages_per_worker + 1
            end_page = min(pages, start_page + pages_per_worker - 1) if i < workers - 1 else pages
            if start_page > pages:
                break
            
            worker_pages = list(range(start_page, end_page + 1))
            tasks.append(worker(context, worker_pages, i + 1))
        
        # Run all workers
        results = await asyncio.gather(*tasks)
        
        for result in results:
            all_games.extend(result)
        
        await browser.close()
    
    elapsed = time.time() - start
    
    if not all_games:
        log("❌ No games scraped")
        return []
    
    # Create DataFrame
    df = pd.DataFrame(all_games)
    
    # Dedupe
    if 'url' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        if before > len(df):
            log(f"🗑️  Removed {before - len(df)} duplicates")
    
    # Convert lists to strings
    for col in ['screenshots', 'videos', 'downloaded_images', 'downloaded_videos']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    
    # Save
    out_dir = Path("scraped_data")
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / "gog_games_playwright.csv"
    
    df.to_csv(out_file, index=False, encoding='utf-8-sig')
    
    # Stats
    log(f"\n{'='*60}")
    log(f"✅ SUCCESS: {len(df)} games in {elapsed:.1f}s ({len(df)/elapsed:.2f} games/s)")
    log(f"💾 Saved: {out_file}")
    
    with_genres = len(df[df['genres'] != 'N/A'])
    with_platforms = len(df[df['platforms'] != 'N/A'])
    with_dev = len(df[df['developer'] != 'N/A'])
    with_screenshots = len(df[df['screenshots'].str.len() > 10])
    with_videos = len(df[df['videos'].str.len() > 10])
    
    log(f"\n📈 Data Quality:")
    log(f"   Genres: {with_genres}/{len(df)} ({100*with_genres/len(df):.1f}%)")
    log(f"   Platforms: {with_platforms}/{len(df)} ({100*with_platforms/len(df):.1f}%)")
    log(f"   Developer: {with_dev}/{len(df)} ({100*with_dev/len(df):.1f}%)")
    log(f"   Screenshots: {with_screenshots}/{len(df)} ({100*with_screenshots/len(df):.1f}%)")
    log(f"   Videos: {with_videos}/{len(df)} ({100*with_videos/len(df):.1f}%)")
    
    log(f"{'='*60}\n")
    
    # Sample
    if len(df) > 0:
        print("\n📋 Sample:")
        print(df[['title', 'genres', 'platforms', 'developer']].head(5).to_string(index=False))
    
    return df.to_dict(orient='records')

def main():
    import argparse
    p = argparse.ArgumentParser(description="GOG Scraper - Playwright")
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

#python gog_scraper.py --pages 15 --workers 4