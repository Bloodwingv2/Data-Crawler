import pandas as pd
from playwright.sync_api import sync_playwright
import time, os, requests, re, sys, argparse
from datetime import datetime

# Force UTF-8 encoding
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

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
            print(f"      ✓ {filename}")
            return filepath
    except Exception as e:
        print(f"      ✗ {filename}: {str(e)[:30]}")
    return None

def safe_text(text):
    """Clean text for CSV"""
    if not text or text == "N/A":
        return "N/A"
    return text.replace('\n', ' ').replace('\r', '').replace('\t', ' ').strip()

def scrape_game_details(page, game_url, game_title, download_media_files=True):
    """Scrape ALL game details from Instant Gaming using exact HTML structure."""
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
        print(f"\n{'='*70}\nSCRAPING: {game_title}\n{'='*70}")
        
        page.goto(game_url, wait_until="load", timeout=60000)
        page.wait_for_timeout(3000)
        print("✓ Page loaded")
        
        safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50].strip()
        game_media_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                      "scraped_data", "instant_gaming_media", safe_title)
        os.makedirs(game_media_dir, exist_ok=True)
        
        # Product ID from URL
        id_match = re.search(r'/(\d+)-', game_url)
        if id_match: 
            details["product_id"] = id_match.group(1)
        
        # === PRICING ===
        print("\n[PRICING]")
        try:
            # Current price from .total
            price_elem = page.locator(".amount .total").first
            if price_elem.count() > 0:
                details["current_price"] = safe_text(price_elem.inner_text())
                # Extract currency
                currency_match = re.search(r'[€$£¥₹₽]', details["current_price"])
                if currency_match: 
                    details["currency"] = currency_match.group()
        except: pass
        
        try:
            # Original/Retail price from .discounts .retail
            retail_elem = page.locator(".amount .discounts .retail").first
            if retail_elem.count() > 0:
                details["original_price"] = safe_text(retail_elem.inner_text())
        except: pass
        
        try:
            # Discount percentage from .discounted
            discount_elem = page.locator(".amount .discounted").first
            if discount_elem.count() > 0:
                details["discount_percentage"] = safe_text(discount_elem.inner_text())
        except: pass
        
        try:
            # Stock status from .stock span
            stock_elem = page.locator(".stock span").first
            if stock_elem.count() > 0:
                details["stock_status"] = safe_text(stock_elem.inner_text())
        except: pass
        
        print(f"   ✓ Price: {details['current_price']} (was {details['original_price']}) {details['discount_percentage']}")
        
        # === GAME INFO FROM META TAGS ===
        print("\n[GAME INFO]")
        try:
            dev_meta = page.locator('meta[itemprop="author"]').first
            if dev_meta.count() > 0:
                details["developer"] = safe_text(dev_meta.get_attribute("content"))
        except: pass
        
        try:
            pub_meta = page.locator('meta[itemprop="publisher"]').first
            if pub_meta.count() > 0:
                details["publisher"] = safe_text(pub_meta.get_attribute("content"))
        except: pass
        
        try:
            platform_meta = page.locator('meta[itemprop="gamePlatform"]').first
            if platform_meta.count() > 0:
                details["platforms"] = safe_text(platform_meta.get_attribute("content"))
        except: pass
        
        # === GAME INFO FROM TABLE ===
        try:
            # Genre from tr.genres
            genre_row = page.locator("tr.genres a.tag").first
            if genre_row.count() > 0:
                details["genre"] = safe_text(genre_row.inner_text())
        except: pass
        
        try:
            # Release date from tr.release-date
            date_row = page.locator("tr.release-date th:nth-child(2)").first
            if date_row.count() > 0:
                details["release_date"] = safe_text(date_row.inner_text())
        except: pass
        
        # === DESCRIPTION ===
        try:
            desc_elem = page.locator("span[itemprop='description']").first
            if desc_elem.count() > 0:
                desc = desc_elem.inner_text().strip()
                details["description"] = safe_text(desc[:1000])  # Limit to 1000 chars
        except: pass
        
        # Fallback description from .product-text .text
        if details["description"] == "N/A":
            try:
                desc_elem = page.locator(".product-text .text").first
                if desc_elem.count() > 0:
                    desc = desc_elem.inner_text().strip()
                    details["description"] = safe_text(desc[:1000])
            except: pass
        
        print(f"   ✓ {details['developer']} | {details['publisher']} | {details['genre']}")
        print(f"   ✓ Release: {details['release_date']} | Platform: {details['platforms']}")
        
        # === RATINGS & REVIEWS ===
        print("\n[RATINGS]")
        try:
            # IG Rating from .ig-search-reviews-avg
            rating_elem = page.locator(".ig-search-reviews-avg").first
            if rating_elem.count() > 0:
                details["ig_rating"] = safe_text(rating_elem.inner_text())
        except: pass
        
        try:
            # Review count from .based .link
            review_count_elem = page.locator(".based .link").first
            if review_count_elem.count() > 0:
                details["review_count"] = safe_text(review_count_elem.inner_text())
        except: pass
        
        try:
            # Steam recent reviews
            steam_recent = page.locator("tr:has-text('Recent Steam reviews') th:nth-child(2)").first
            if steam_recent.count() > 0:
                details["steam_recent_reviews"] = safe_text(steam_recent.inner_text())
        except: pass
        
        try:
            # Steam all reviews with count
            steam_all_elem = page.locator("tr:has-text('All Steam reviews') th:nth-child(2) span").first
            if steam_all_elem.count() > 0:
                details["steam_all_reviews"] = safe_text(steam_all_elem.inner_text())
                
            # Steam review count
            steam_count = page.locator("tr:has-text('All Steam reviews') th:nth-child(2) span:nth-child(2)").first
            if steam_count.count() > 0:
                count_text = steam_count.inner_text()
                # Extract number from parentheses
                count_match = re.search(r'\((\d+)\)', count_text)
                if count_match:
                    details["steam_review_count"] = count_match.group(1)
        except: pass
        
        print(f"   ✓ IG Rating: {details['ig_rating']} | Reviews: {details['review_count']}")
        print(f"   ✓ Steam: {details['steam_all_reviews']} ({details['steam_review_count']})")
        
        # === USER TAGS ===
        print("\n[TAGS & FEATURES]")
        try:
            tag_links = page.locator(".users-tags a.searchtag").all()
            tags = []
            for tag in tag_links[:20]:  # Limit to 20 tags
                tag_text = tag.inner_text().strip()
                if tag_text and tag_text != "...":
                    tags.append(tag_text)
            details["user_tags"] = tags
            if tags:
                print(f"   ✓ User Tags: {len(tags)} found")
        except: pass
        
        # === GAME FEATURES ===
        try:
            feature_links = page.locator(".features-listing a.feature .feature-text").all()
            features = []
            for feat in feature_links:
                feat_text = feat.inner_text().strip()
                if feat_text:
                    features.append(feat_text)
            details["game_features"] = features
            if features:
                print(f"   ✓ Game Features: {len(features)} found")
        except: pass
        
        # === SYSTEM REQUIREMENTS ===
        print("\n[SYSTEM REQUIREMENTS]")
        try:
            # Minimum requirements
            min_items = page.locator(".minimal ul.specs li").all()
            min_reqs = []
            for item in min_items:
                min_reqs.append(safe_text(item.inner_text()))
            if min_reqs:
                details["system_requirements_min"] = " | ".join(min_reqs)
                print(f"   ✓ Min Requirements: {len(min_reqs)} items")
        except: pass
        
        try:
            # Recommended requirements
            rec_items = page.locator(".recommended ul.specs li").all()
            rec_reqs = []
            for item in rec_items:
                rec_reqs.append(safe_text(item.inner_text()))
            if rec_reqs:
                details["system_requirements_rec"] = " | ".join(rec_reqs)
                print(f"   ✓ Rec Requirements: {len(rec_reqs)} items")
        except: pass
        
        # === EDITIONS ===
        try:
            edition_items = page.locator(".editions .item").all()
            editions = []
            for edition in edition_items[:5]:  # Limit to 5 editions
                try:
                    name_elem = edition.locator(".name h3").first
                    price_elem = edition.locator(".amount .total").first
                    
                    if name_elem.count() > 0 and price_elem.count() > 0:
                        edition_name = safe_text(name_elem.inner_text())
                        edition_price = safe_text(price_elem.inner_text())
                        editions.append(f"{edition_name}: {edition_price}")
                except: continue
            
            if editions:
                details["editions"] = editions
                print(f"   ✓ Editions: {len(editions)} found")
        except: pass
        
        # === MEDIA ===
        print("\n[MEDIA]")
        
        # Header image from meta
        try:
            img_meta = page.locator('meta[itemprop="image"]').first
            if img_meta.count() > 0:
                details["header_image"] = img_meta.get_attribute("content")
                if download_media_files and details["header_image"] != "N/A":
                    dl = download_media(details["header_image"], game_media_dir, "cover.jpg")
        except: pass
        
        # Video from iframe
        try:
            video_iframe = page.locator("#ig-vimeo-player").first
            if video_iframe.count() > 0:
                details["video_url"] = video_iframe.get_attribute("src")
                print(f"   ✓ Video: Found")
        except: pass
        
        # Screenshots
        try:
            screenshot_links = page.locator(".screenshots a[itemprop='screenshot']").all()
            screenshots = []
            
            for idx, link in enumerate(screenshot_links[:10]):
                try:
                    href = link.get_attribute("href")
                    if href:
                        screenshots.append(href)
                        if download_media_files:
                            ext = "jpg"
                            if ".png" in href.lower(): ext = "png"
                            elif ".webp" in href.lower(): ext = "webp"
                            download_media(href, game_media_dir, f"screenshot_{idx+1}.{ext}")
                except: continue
            
            details["screenshots"] = screenshots
            if screenshots:
                print(f"   ✓ Screenshots: {len(screenshots)} found")
        except: pass
        
        print(f"\n{'='*70}\n✓ COMPLETE: {game_title}\n{'='*70}\n")
        
    except Exception as e:
        print(f"\n✗✗✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    return details

def scrape_products_from_page(page, base_url, max_games):
    """Scrape game listings from homepage."""
    print(f"\n{'#'*70}\nLOADING HOMEPAGE\n{'#'*70}\n")
    
    try:
        page.goto(base_url, wait_until="load", timeout=60000)
        page.wait_for_timeout(5000)
        print("✓ Page loaded")
        
        # Scroll to load more
        for i in range(5):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
        
        game_links = []
        
        # Get all game items
        items = page.locator(".listing-items article.item").all()
        print(f"✓ Found {len(items)} game items")
        
        for idx, item in enumerate(items[:max_games]):
            try:
                link = item.locator("a.cover").first
                if link.count() == 0: continue
                
                href = link.get_attribute("href")
                
                # Get title
                title_elem = item.locator(".name .title").first
                title = "Unknown"
                if title_elem.count() > 0:
                    title = title_elem.get_attribute("title") or title_elem.inner_text().strip()
                
                if href:
                    if href.startswith("/"):
                        href = f"https://www.instant-gaming.com{href}"
                    
                    game_links.append({"url": href, "title": title})
                    print(f"  [{idx+1}] ✓ {title[:60]}")
            except: continue
        
        # Remove duplicates
        unique = []
        seen = set()
        for g in game_links:
            if g["url"] not in seen:
                seen.add(g["url"])
                unique.append(g)
        
        print(f"\n✓ Extracted {len(unique)} unique games\n")
        return unique[:max_games]
        
    except Exception as e:
        print(f"\n✗✗✗ ERROR: {e}")
        return []

def run_scraper(base_url, max_games, download_media, headless):
    """Main scraper function."""
    print("\n" + "="*70)
    print("INSTANT GAMING SCRAPER - OPTIMIZED")
    print("="*70 + "\n")
    
    os.makedirs("scraped_data", exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = ctx.new_page()
        
        try:
            games = scrape_products_from_page(page, base_url, max_games)
            if not games:
                return None
            
            all_results = []
            for idx, game in enumerate(games, 1):
                print(f"\n[{idx}/{len(games)}] {game['title']}")
                details = scrape_game_details(page, game['url'], game['title'], download_media)
                all_results.append(details)
                time.sleep(2)
            
            # Convert to DataFrame
            df = pd.DataFrame(all_results)
            
            # Convert lists to pipe-separated strings
            for col in ['screenshots', 'user_tags', 'game_features', 'editions']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) and x else 'N/A')
            
            # Save CSV with UTF-8
            df.to_csv("scraped_data/instant_gaming_data.csv", index=False, encoding='utf-8-sig')
        
            
            print("\n" + "="*70)
            print(f"✓ COMPLETE! {len(all_results)} games scraped")
            print("  CSV: scraped_data/instant_gaming_data.csv")
            print("="*70 + "\n")
            
            # Show stats
            print("DATA QUALITY:")
            for col in ['current_price', 'developer', 'genre', 'description', 'ig_rating']:
                if col in df.columns:
                    non_na = len(df[df[col] != 'N/A'])
                    print(f"  {col}: {non_na}/{len(df)} ({non_na/len(df)*100:.1f}%)")
            
            return df
            
        finally:
            ctx.close()
            browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Instant Gaming Scraper - Optimized')
    parser.add_argument('--url', type=str, default='https://www.instant-gaming.com/en/', 
                       help='URL to scrape')
    parser.add_argument('--max-games', type=int, default=20, help='Max games to scrape')
    parser.add_argument('--no-media', action='store_true', help='Skip downloading media')
    parser.add_argument('--headless', action='store_true', help='Run headless (default: False)')
    
    args = parser.parse_args()
    
    print(f"\n{'='*70}\nCONFIGURATION\n{'='*70}")
    print(f"URL: {args.url}")
    print(f"Max Games: {args.max_games}")
    print(f"Download Media: {not args.no_media}")
    print(f"Headless: {args.headless}")
    print("="*70 + "\n")
    
    df = run_scraper(args.url, args.max_games, not args.no_media, args.headless)
    
    if df is not None:
        print("\nSAMPLE DATA (first 3 rows):")
        print("="*70)
        cols = ['title', 'current_price', 'discount_percentage', 'genre', 'ig_rating']
        print(df[cols].head(3).to_string(index=False))
        
# Default: 20 games, with media
# python scraper.py

# Headless mode, 50 games, no media
# python scraper.py --headless --max-games 50 --no-media

# Custom URL
# python scraper.py --url "https://www.instant-gaming.com/en/search/?type=software"