import pandas as pd
from playwright.async_api import async_playwright
import asyncio
import time, os, requests, re, sys, argparse
from datetime import datetime

# Force UTF-8 encoding and disable buffering
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None

def download_media(url, save_dir, filename):
    """Download images and videos from URLs."""
    if not url or url == "N/A" or not url.startswith('http'): 
        return None
    try:
        r = requests.get(url, timeout=15, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            filepath = os.path.join(save_dir, filename)
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(8192): f.write(chunk)
            return filepath
    except: pass
    return None

def safe_text(text):
    """Clean text for CSV"""
    if not text or text == "N/A":
        return "N/A"
    return text.replace('\n', ' ').replace('\r', '').replace('\t', ' ').strip()

async def scrape_game_details(page, game_url, game_title, download_media_files=True):
    """Scrape game details - async version"""
    details = {
        "title": game_title, "url": game_url, "developer": "N/A", "publisher": "N/A",
        "platforms": "N/A", "genre": "N/A", "release_date": "N/A", "description": "N/A",
        "current_price": "N/A", "original_price": "N/A", "discount_percentage": "N/A",
        "currency": "N/A", "stock_status": "N/A", "ig_rating": "N/A", "review_count": "N/A",
        "steam_recent_reviews": "N/A", "steam_all_reviews": "N/A", "steam_review_count": "N/A",
        "video_url": "N/A", "header_image": "N/A", "screenshots": [], "user_tags": [],
        "game_features": [], "system_requirements_min": "N/A", "system_requirements_rec": "N/A",
        "product_id": "N/A", "editions": [], "scrape_timestamp": datetime.now().isoformat()
    }
    
    try:
        await page.goto(game_url, wait_until="load", timeout=60000)
        await page.wait_for_timeout(2000)
        
        safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50].strip()
        game_media_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                      "scraped_data", "instant_gaming_media", safe_title)
        os.makedirs(game_media_dir, exist_ok=True)
        
        # Product ID
        id_match = re.search(r'/(\d+)-', game_url)
        if id_match: details["product_id"] = id_match.group(1)
        
        # PRICING
        try:
            price_elem = page.locator(".amount .total").first
            if await price_elem.count() > 0:
                details["current_price"] = safe_text(await price_elem.inner_text())
                currency_match = re.search(r'[€$£¥₹₽]', details["current_price"])
                if currency_match: details["currency"] = currency_match.group()
        except: pass
        
        try:
            retail_elem = page.locator(".amount .discounts .retail").first
            if await retail_elem.count() > 0:
                details["original_price"] = safe_text(await retail_elem.inner_text())
        except: pass
        
        try:
            discount_elem = page.locator(".amount .discounted").first
            if await discount_elem.count() > 0:
                details["discount_percentage"] = safe_text(await discount_elem.inner_text())
        except: pass
        
        try:
            stock_elem = page.locator(".stock span").first
            if await stock_elem.count() > 0:
                details["stock_status"] = safe_text(await stock_elem.inner_text())
        except: pass
        
        # META TAGS
        try:
            dev_meta = page.locator('meta[itemprop="author"]').first
            if await dev_meta.count() > 0:
                details["developer"] = safe_text(await dev_meta.get_attribute("content"))
        except: pass
        
        try:
            pub_meta = page.locator('meta[itemprop="publisher"]').first
            if await pub_meta.count() > 0:
                details["publisher"] = safe_text(await pub_meta.get_attribute("content"))
        except: pass
        
        try:
            platform_meta = page.locator('meta[itemprop="gamePlatform"]').first
            if await platform_meta.count() > 0:
                details["platforms"] = safe_text(await platform_meta.get_attribute("content"))
        except: pass
        
        # TABLE DATA
        try:
            genre_row = page.locator("tr.genres a.tag").first
            if await genre_row.count() > 0:
                details["genre"] = safe_text(await genre_row.inner_text())
        except: pass
        
        try:
            date_row = page.locator("tr.release-date th:nth-child(2)").first
            if await date_row.count() > 0:
                details["release_date"] = safe_text(await date_row.inner_text())
        except: pass
        
        # DESCRIPTION
        try:
            desc_elem = page.locator("span[itemprop='description']").first
            if await desc_elem.count() > 0:
                desc = (await desc_elem.inner_text()).strip()
                details["description"] = safe_text(desc[:1000])
        except: pass
        
        if details["description"] == "N/A":
            try:
                desc_elem = page.locator(".product-text .text").first
                if await desc_elem.count() > 0:
                    desc = (await desc_elem.inner_text()).strip()
                    details["description"] = safe_text(desc[:1000])
            except: pass
        
        # RATINGS
        try:
            rating_elem = page.locator(".ig-search-reviews-avg").first
            if await rating_elem.count() > 0:
                details["ig_rating"] = safe_text(await rating_elem.inner_text())
        except: pass
        
        try:
            review_count_elem = page.locator(".based .link").first
            if await review_count_elem.count() > 0:
                details["review_count"] = safe_text(await review_count_elem.inner_text())
        except: pass
        
        try:
            steam_recent = page.locator("tr:has-text('Recent Steam reviews') th:nth-child(2)").first
            if await steam_recent.count() > 0:
                details["steam_recent_reviews"] = safe_text(await steam_recent.inner_text())
        except: pass
        
        try:
            steam_all_elem = page.locator("tr:has-text('All Steam reviews') th:nth-child(2) span").first
            if await steam_all_elem.count() > 0:
                details["steam_all_reviews"] = safe_text(await steam_all_elem.inner_text())
            
            steam_count = page.locator("tr:has-text('All Steam reviews') th:nth-child(2) span:nth-child(2)").first
            if await steam_count.count() > 0:
                count_text = await steam_count.inner_text()
                count_match = re.search(r'\((\d+)\)', count_text)
                if count_match:
                    details["steam_review_count"] = count_match.group(1)
        except: pass
        
        # TAGS
        try:
            tag_links = await page.locator(".users-tags a.searchtag").all()
            tags = []
            for tag in tag_links[:20]:
                tag_text = (await tag.inner_text()).strip()
                if tag_text and tag_text != "...":
                    tags.append(tag_text)
            details["user_tags"] = tags
        except: pass
        
        # FEATURES
        try:
            feature_links = await page.locator(".features-listing a.feature .feature-text").all()
            features = []
            for feat in feature_links:
                feat_text = (await feat.inner_text()).strip()
                if feat_text:
                    features.append(feat_text)
            details["game_features"] = features
        except: pass
        
        # SYSTEM REQUIREMENTS
        try:
            min_items = await page.locator(".minimal ul.specs li").all()
            min_reqs = []
            for item in min_items:
                min_reqs.append(safe_text(await item.inner_text()))
            if min_reqs:
                details["system_requirements_min"] = " | ".join(min_reqs)
        except: pass
        
        try:
            rec_items = await page.locator(".recommended ul.specs li").all()
            rec_reqs = []
            for item in rec_items:
                rec_reqs.append(safe_text(await item.inner_text()))
            if rec_reqs:
                details["system_requirements_rec"] = " | ".join(rec_reqs)
        except: pass
        
        # EDITIONS
        try:
            edition_items = await page.locator(".editions .item").all()
            editions = []
            for edition in edition_items[:5]:
                try:
                    name_elem = edition.locator(".name h3").first
                    price_elem = edition.locator(".amount .total").first
                    
                    if await name_elem.count() > 0 and await price_elem.count() > 0:
                        edition_name = safe_text(await name_elem.inner_text())
                        edition_price = safe_text(await price_elem.inner_text())
                        editions.append(f"{edition_name}: {edition_price}")
                except: continue
            
            if editions:
                details["editions"] = editions
        except: pass
        
        # MEDIA
        try:
            img_meta = page.locator('meta[itemprop="image"]').first
            if await img_meta.count() > 0:
                details["header_image"] = await img_meta.get_attribute("content")
                if download_media_files and details["header_image"] != "N/A":
                    download_media(details["header_image"], game_media_dir, "cover.jpg")
        except: pass
        
        try:
            video_iframe = page.locator("#ig-vimeo-player").first
            if await video_iframe.count() > 0:
                details["video_url"] = await video_iframe.get_attribute("src")
        except: pass
        
        try:
            screenshot_links = await page.locator(".screenshots a[itemprop='screenshot']").all()
            screenshots = []
            for idx, link in enumerate(screenshot_links[:10]):
                try:
                    href = await link.get_attribute("href")
                    if href:
                        screenshots.append(href)
                        if download_media_files:
                            ext = "jpg"
                            if ".png" in href.lower(): ext = "png"
                            elif ".webp" in href.lower(): ext = "webp"
                            download_media(href, game_media_dir, f"screenshot_{idx+1}.{ext}")
                except: continue
            details["screenshots"] = screenshots
        except: pass
        
        
    except Exception as e:
        print(f"✗ Error scraping {game_title}: {e}", flush=True)
    
    return details

async def scrape_search_page(page, page_num, search_query=""):
    """Scrape games from search page"""
    games = []
    
    try:
        # Build search URL
        if search_query:
            base_url = f"https://www.instant-gaming.com/en/search/?q={search_query}"
        else:
            # Use generic search to get all games
            base_url = "https://www.instant-gaming.com/en/search/"
        
        separator = '&' if '?' in base_url else '?'
        page_url = f"{base_url}{separator}page={page_num}" if page_num > 1 else base_url
        
        print(f"[Search Page {page_num}] Loading: {page_url[:80]}...", flush=True)
        
        await page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)
        
        # Scroll to load lazy content
        for _ in range(5):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
        
        # Try multiple selectors for game items
        items = await page.locator(".search article.item, .listing-items article.item, article.item").all()
        
        if not items:
            print(f"[Search Page {page_num}] No items found", flush=True)
            return games
        
        for item in items:
            try:
                # Try to find the link
                link = item.locator("a.cover, a.picture, a[href*='/en/']").first
                if await link.count() == 0: continue
                
                href = await link.get_attribute("href")
                if not href: continue
                
                if href.startswith("/"):
                    href = f"https://www.instant-gaming.com{href}"
                
                # Skip non-game links
                if not re.search(r'/\d+-', href):
                    continue
                
                # Get title
                title_elem = item.locator(".name .title, .title, h3").first
                title = "Unknown"
                if await title_elem.count() > 0:
                    title = await title_elem.get_attribute("title")
                    if not title:
                        title = (await title_elem.inner_text()).strip()
                
                # Skip gift cards and non-games
                skip_keywords = ['gift card', 'points', 'gems', 'credits', 'wallet', 'season pass']
                if any(skip in title.lower() for skip in skip_keywords):
                    continue
                
                games.append({"url": href, "title": title, "page": page_num})
                
            except Exception as e:
                continue
        
        print(f"[Search Page {page_num}] ✓ Found {len(games)} games", flush=True)
        
    except Exception as e:
        print(f"[Search Page {page_num}] ✗ Error: {e}", flush=True)
    
    return games

async def scrape_category_pages(browser, max_concurrent=10):
    """Scrape games from multiple category pages"""
    
    categories = [
        "https://www.instant-gaming.com/en/search/?type%5B%5D=steam",
        "https://www.instant-gaming.com/en/search/?type%5B%5D=epic",
        "https://www.instant-gaming.com/en/search/?type%5B%5D=uplay",
        "https://www.instant-gaming.com/en/search/?type%5B%5D=origin",
        "https://www.instant-gaming.com/en/search/?type%5B%5D=gog",
        "https://www.instant-gaming.com/en/search/?platforms%5B%5D=1",  # PC
        "https://www.instant-gaming.com/en/search/?platforms%5B%5D=7",  # PS5
        "https://www.instant-gaming.com/en/search/?platforms%5B%5D=5",  # Xbox
        "https://www.instant-gaming.com/en/search/?sort_by=bestseller",
        "https://www.instant-gaming.com/en/search/?sort_by=price_asc",
        "https://www.instant-gaming.com/en/search/?sort_by=release_date",
    ]
    
    all_games = []
    seen_urls = set()
    
    print("\n🎮 Phase 1A: Scraping from multiple categories...", flush=True)
    
    for category_url in categories:
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        try:
            # Get first 3 pages from each category
            for page_num in range(1, 4):
                games = await scrape_search_page(page, page_num, "")
                
                for game in games:
                    if game['url'] not in seen_urls:
                        seen_urls.add(game['url'])
                        all_games.append(game)
                
                await asyncio.sleep(1)  # Be polite
                
        except Exception as e:
            print(f"Error scraping category {category_url[:50]}: {e}", flush=True)
        finally:
            await context.close()
    
    return all_games

async def scrape_game_worker(game_data, browser, download_media):
    """Worker function to scrape a single game"""
    context = await browser.new_context(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        viewport={'width': 1920, 'height': 1080}
    )
    page = await context.new_page()
    
    try:
        details = await scrape_game_details(page, game_data['url'], game_data['title'], download_media)
        print(f"✓ [{game_data.get('index', '?')}] {game_data['title'][:60]}", flush=True)
        return details
    except Exception as e:
        print(f"✗ [{game_data.get('index', '?')}] {game_data['title']}: {e}", flush=True)
        return None
    finally:
        await context.close()

async def run_scraper(max_games, download_media, max_concurrent=10):
    """Main scraper with async concurrency"""
    print("\n" + "="*70)
    print("INSTANT GAMING SCRAPER - SEARCH-BASED VERSION")
    print("="*70 + "\n")
    
    os.makedirs("scraped_data", exist_ok=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # PHASE 1: Collect game URLs from categories
        print(f"PHASE 1: Collecting game URLs (Target: {max_games})")
        print("="*70)
        
        all_games = await scrape_category_pages(browser, max_concurrent)
        
        print(f"\n✓ Collected {len(all_games)} unique games from categories", flush=True)
        
        # If we need more, scrape general search pages
        if len(all_games) < max_games:
            print(f"\n🎮 Phase 1B: Scraping general search pages to reach {max_games} games...", flush=True)
            
            pages_needed = ((max_games - len(all_games)) // 30) + 5
            seen_urls = {g['url'] for g in all_games}
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()
            
            for page_num in range(1, pages_needed + 1):
                games = await scrape_search_page(page, page_num, "")
                
                for game in games:
                    if game['url'] not in seen_urls:
                        seen_urls.add(game['url'])
                        all_games.append(game)
                
                if len(all_games) >= max_games:
                    break
                
                await asyncio.sleep(1)
            
            await context.close()
        
        print(f"\n✓ Total unique games collected: {len(all_games)}", flush=True)
        print(f"✓ Will scrape {min(len(all_games), max_games)} games", flush=True)
        
        if not all_games:
            print("✗ No games found!", flush=True)
            await browser.close()
            return None
        
        # Limit to max_games
        games_to_scrape = all_games[:max_games]
        
        # PHASE 2: Scrape each game concurrently
        print(f"\nPHASE 2: Scraping game details ({len(games_to_scrape)} games)")
        print("="*70 + "\n")
        
        all_results = []
        
        # Add index for progress tracking
        for idx, game in enumerate(games_to_scrape, 1):
            game['index'] = f"{idx}/{len(games_to_scrape)}"
        
        # Create tasks for all games
        tasks = []
        for game in games_to_scrape:
            tasks.append(scrape_game_worker(game, browser, download_media))
        
        # Run with concurrency limit
        completed = 0
        for i in range(0, len(tasks), max_concurrent):
            batch = tasks[i:i+max_concurrent]
            results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    print(f"Error in game worker: {result}", flush=True)
                    continue
                    
                if result:
                    all_results.append(result)
                    completed += 1
                    
                    # Save backup every 50 games
                    if completed % 50 == 0:
                        temp_df = pd.DataFrame(all_results)
                        for col in ['screenshots', 'user_tags', 'game_features', 'editions']:
                            if col in temp_df.columns:
                                temp_df[col] = temp_df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) and x else 'N/A')
                        temp_df.to_csv("scraped_data/instant_gaming_backup.csv", index=False, encoding='utf-8-sig')
                        print(f"\n💾 Backup saved: {completed} games\n", flush=True)
        
        await browser.close()
        
        # Save final results
        df = pd.DataFrame(all_results)
        
        for col in ['screenshots', 'user_tags', 'game_features', 'editions']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) and x else 'N/A')
        
        df.to_csv("scraped_data/instant_gaming_data.csv", index=False, encoding='utf-8-sig')
        
        print("\n" + "="*70)
        print(f"✓ SCRAPING COMPLETE!")
        print(f"  Successfully scraped: {len(all_results)} games")
        print("  CSV: scraped_data/instant_gaming_data.csv")
        print("="*70 + "\n")
        
        print("DATA QUALITY:")
        for col in ['current_price', 'developer', 'genre', 'description', 'ig_rating']:
            if col in df.columns:
                non_na = len(df[df[col] != 'N/A'])
                print(f"  {col}: {non_na}/{len(df)} ({non_na/len(df)*100:.1f}%)")
        
        return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Instant Gaming Scraper - Search-Based')
    parser.add_argument('--max-games', type=int, default=700, help='Max games to scrape (default: 700)')
    parser.add_argument('--no-media', action='store_true', help='Skip downloading media')
    parser.add_argument('--concurrent', type=int, default=10, help='Number of concurrent tasks (default: 10)')
    
    args = parser.parse_args()
    
    print(f"\n{'='*70}\nCONFIGURATION\n{'='*70}")
    print(f"Max Games: {args.max_games}")
    print(f"Download Media: {not args.no_media}")
    print(f"Concurrent Tasks: {args.concurrent}")
    print("="*70 + "\n")
    
    df = asyncio.run(run_scraper(args.max_games, not args.no_media, args.concurrent))
    
    if df is not None:
        print("\nSAMPLE DATA (first 5 rows):")
        print("="*70)
        cols = ['title', 'current_price', 'discount_percentage', 'genre', 'ig_rating']
        print(df[cols].head(5).to_string(index=False))

# Usage:
# python instantgaming_fixed.py --max-games 700 --concurrent 15
# python instantgaming_fixed.py --max-games 500 --concurrent 10 --no-mediac