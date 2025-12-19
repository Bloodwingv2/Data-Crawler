import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
import re
import json
from typing import Optional, Dict, List

data_lock = Lock()
all_game_data = []

def convert_steam_rating_to_score(review_text):
    """Convert Steam's text ratings to numerical scores (0-100)."""
    if not review_text or review_text == "N/A":
        return None
    
    review_lower = review_text.lower()
    rating_map = {
        'overwhelmingly positive': 95, 'very positive': 85, 'positive': 75,
        'mostly positive': 70, 'mixed': 50, 'mostly negative': 30,
        'negative': 25, 'very negative': 15, 'overwhelmingly negative': 5
    }
    
    for rating_text, score in rating_map.items():
        if rating_text in review_lower:
            return score
    return None

def extract_review_percentage(review_text):
    """Extract the percentage from Steam's review tooltip."""
    if not review_text or review_text == "N/A":
        return None
    match = re.search(r'(\d+)%', review_text)
    return int(match.group(1)) if match else None

def convert_hls_to_direct_url(hls_url):
    """Convert HLS manifest URL to direct video URLs - FROM SELENIUM VERSION."""
    try:
        # Remove the HLS filename and query params
        base_url = hls_url.split('/hls_')[0] + '/'
        
        # Try multiple formats that Steam commonly uses
        possible_formats = [
            base_url + 'movie_max_vp9.webm',      # Highest quality VP9
            base_url + 'movie480_vp9.webm',       # Standard VP9
            base_url + 'movie_max.webm',          # Highest quality
            base_url + 'movie480.webm',           # Standard WebM
            base_url + 'movie_max.mp4',           # Highest quality MP4
            base_url + 'movie480.mp4',            # Standard MP4
        ]
        
        return possible_formats
    except:
        return []

def download_media(url, save_dir, filename):
    """Download media file from URL - handles HLS manifest conversion."""
    try:
        # For HLS manifests, save URL info for manual download
        if url.endswith('.m3u8') or url.endswith('.mpd'):
            filepath = os.path.join(save_dir, filename.replace('.webm', '.txt').replace('.mp4', '.txt'))
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"Video Manifest URL:\n{url}\n\n")
                f.write("Note: This is an HLS/DASH manifest. Use ffmpeg to download:\n")
                f.write(f'ffmpeg -i "{url}" -c copy "{filename.replace(".txt", ".mp4")}"\n')
            return filepath
        
        # Regular download for images and direct video files
        response = requests.get(url, timeout=15, stream=True, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://store.steampowered.com/'
        })
        
        if response.status_code == 200:
            filepath = os.path.join(save_dir, filename)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Verify file was downloaded
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                return filepath
            else:
                print(f"   ⚠️ Downloaded file is empty")
                return None
        else:
            print(f"   HTTP {response.status_code} for {filename}")
            return None
            
    except Exception as e:
        print(f"   Download error {filename}: {str(e)[:50]}")
    return None

def handle_age_gate(page):
    """Handle Steam age verification gate - FAST version."""
    try:
        if page.locator(".agegate_birthday_selector").is_visible(timeout=500):
            page.select_option("#ageYear", "1990")
            page.click("#age_gate_btn_continue")
            page.wait_for_load_state("domcontentloaded", timeout=5000)
            return True
    except:
        pass
    return False

def extract_video_urls(page, page_content: str) -> List[str]:
    """
    Extract game trailer URLs - ENHANCED VERSION from Selenium scraper.
    Prioritizes direct video files over HLS manifests.
    """
    video_urls = []
    
    try:
        # Method 0: Extract embedded videos from game description (BEST - actual video files!)
        try:
            video_elements = page.locator("video source[src*='.webm'], video source[src*='.mp4']").all()
            
            for video_elem in video_elements[:3]:
                try:
                    video_url = video_elem.get_attribute("src")
                    if video_url and 'store_item_assets' in video_url:
                        video_urls.append(video_url)
                        print(f"      ✓ Embedded video: {video_url[:80]}...")
                except:
                    continue
            
            if video_urls:
                print(f"      Found {len(video_urls)} embedded videos")
        except Exception as e:
            pass
        
        # Method 1: Parse data-props JSON for trailers
        if len(video_urls) < 3:
            try:
                selectors = [
                    ".gamehighlight_desktopcarousel[data-props]",
                    "[data-featuretarget='gamehighlight-desktopcarousel'][data-props]",
                    "div[data-props*='trailers']",
                    "[class*='gamehighlight'][data-props]"
                ]
                
                carousel = None
                for selector in selectors:
                    try:
                        carousel = page.locator(selector).first
                        if carousel.count() > 0:
                            break
                    except:
                        continue
                
                if carousel and carousel.count() > 0:
                    data_props = carousel.get_attribute("data-props")
                    
                    if data_props:
                        # Unescape HTML entities
                        data_props = data_props.replace('&quot;', '"').replace('&amp;', '&').replace('\\/', '/')
                        
                        # Parse the JSON data
                        data = json.loads(data_props)
                        
                        # Extract trailer URLs
                        if "trailers" in data and isinstance(data["trailers"], list):
                            for trailer in data["trailers"][:3]:
                                # Get HLS manifest and convert to direct URLs
                                if "hlsManifest" in trailer and trailer["hlsManifest"]:
                                    hls_url = trailer["hlsManifest"].replace('\\/', '/')
                                    
                                    # Get all possible direct video URLs
                                    possible_urls = convert_hls_to_direct_url(hls_url)
                                    
                                    # Add the first converted URL (not the HLS manifest)
                                    for url in possible_urls:
                                        if not url.endswith('.m3u8'):
                                            video_urls.append(url)
                                            print(f"      ✓ Converted HLS: {url[:80]}...")
                                            break
                                    else:
                                        # If no direct URL, keep HLS as last resort
                                        video_urls.append(hls_url)
                                        print(f"      HLS manifest: {hls_url[:80]}...")
                                        
                                # Fallback to DASH manifest
                                elif "dashManifests" in trailer and trailer["dashManifests"] and len(trailer["dashManifests"]) > 0:
                                    url = trailer["dashManifests"][0].replace('\\/', '/')
                                    video_urls.append(url)
                                    print(f"      DASH: {url[:80]}...")
                        
                        if len(video_urls) > 0:
                            print(f"      Found {len(video_urls)} from data-props")
                    
            except json.JSONDecodeError as e:
                pass
            except Exception as e:
                pass
        
        # Method 2: Regex search for embedded video URLs in page source
        if len(video_urls) < 3:
            try:
                # Pattern for embedded game description videos (direct files!)
                embedded_pattern = r'https://shared\.fastly\.steamstatic\.com/store_item_assets/steam/apps/\d+/extras/[^"\'<>\s]+\.webm'
                embedded_matches = re.findall(embedded_pattern, page_content)
                
                for url in embedded_matches[:3]:
                    if url not in video_urls:
                        video_urls.append(url)
                        print(f"      ✓ Regex embedded: {url[:80]}...")
                        if len(video_urls) >= 3:
                            break
                
                # Also search for direct trailer videos
                video_patterns = [
                    r'https://video\.[^"\'<>\s]+/store_trailers/[^"\'<>\s]+/movie480_vp9\.webm',
                    r'https://video\.[^"\'<>\s]+/store_trailers/[^"\'<>\s]+/movie_max_vp9\.webm',
                    r'https://video\.[^"\'<>\s]+/store_trailers/[^"\'<>\s]+/movie480\.webm',
                    r'https://cdn\.[^"\'<>\s]+/steam/apps/\d+/movie480\.webm',
                ]
                
                exclude_keywords = ['steamdeck', 'hardware']
                
                for pattern in video_patterns:
                    matches = re.findall(pattern, page_content)
                    for url in matches:
                        if not any(kw in url.lower() for kw in exclude_keywords):
                            if url not in video_urls:
                                video_urls.append(url)
                                print(f"      ✓ Regex trailer: {url[:80]}...")
                                if len(video_urls) >= 3:
                                    break
                    if len(video_urls) >= 3:
                        break
                
            except Exception as e:
                pass
        
        # Method 3: Construct URLs from app ID as last resort
        if len(video_urls) == 0:
            try:
                current_url = page.url
                app_id_match = re.search(r'/app/(\d+)/', current_url)
                
                if app_id_match:
                    app_id = app_id_match.group(1)
                    
                    constructed_urls = [
                        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/movie480.webm",
                        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/movie480.webm",
                    ]
                    
                    video_urls.append(constructed_urls[0])
                    print(f"      Constructed: {constructed_urls[0][:80]}")
            except Exception as e:
                pass
            
    except Exception as e:
        print(f"   Fatal video error: {e}")
    
    # Return unique URLs, limit to 3
    unique_urls = []
    for url in video_urls:
        if url not in unique_urls:
            unique_urls.append(url)
    
    return unique_urls[:3]

def scrape_game_details(page, game_url, game_title, download_media_files=True):
    """Scrape detailed game information - ENHANCED with better video extraction."""
    # ADDED developer and publisher to default dictionary
    details = {
        "genres": "N/A", "developer": "N/A", "publisher": "N/A", 
        "categories": "N/A", "multiplayer": "No", "singleplayer": "No",
        "system_requirements_windows": "N/A", "header_image": "N/A",
        "screenshots": "N/A", "videos": "N/A",
        "downloaded_images": [], "downloaded_videos": []
    }
    
    try:
        # Navigate with shorter timeout
        page.goto(game_url, wait_until="domcontentloaded", timeout=15000)
        handle_age_gate(page)
        
        # Wait for essential content only
        try:
            page.wait_for_selector(".game_page_background, .page_content", timeout=3000)
        except:
            pass
        
        # Wait a bit for videos to load
        time.sleep(2)
        
        # Get page content once for regex extraction
        page_content = page.content()
        
        # === FAST DATA EXTRACTION ===
        
        # Developer and Publisher Extraction
        try:
            # Targeting the specific ID you provided: appHeaderGridContainer
            grid_container = page.locator("#appHeaderGridContainer")
            if grid_container.count() > 0:
                # Developer is usually the first content block in the grid
                dev_text = grid_container.locator(".grid_content").first.inner_text()
                details["developer"] = dev_text.strip() if dev_text else "N/A"
                
                # Publisher is usually the second content block in the grid
                pub_text = grid_container.locator(".grid_content").nth(1).inner_text()
                details["publisher"] = pub_text.strip() if pub_text else "N/A"
        except:
            pass

        # Genres - single query
        try:
            genres = page.locator(".details_block a[href*='genre']").all_inner_texts()
            details["genres"] = ", ".join([g.strip() for g in genres if g.strip()]) or "N/A"
        except:
            pass
        
        # Categories + Multiplayer detection
        try:
            categories = []
            cats = page.locator(".game_area_features_list_ctn a").all_inner_texts()
            for cat_text in cats:
                if cat_text:
                    categories.append(cat_text)
                    cat_lower = cat_text.lower()
                    if "multi" in cat_lower:
                        details["multiplayer"] = "Yes"
                    if "single" in cat_lower:
                        details["singleplayer"] = "Yes"
            details["categories"] = ", ".join(set(categories)[:10]) if categories else "N/A"
        except:
            pass
        
        # System Requirements (Windows only, simplified)
        try:
            req = page.locator(".game_area_sys_req_leftCol, .sysreq_contents").first
            if req.is_visible(timeout=1000):
                req_text = req.inner_text(timeout=500).strip()[:300]
                if req_text:
                    details["system_requirements_windows"] = req_text
        except:
            pass
        
        # === MEDIA EXTRACTION ===
        
        # Header image
        try:
            header = page.locator(".game_header_image_full").first
            if header.is_visible(timeout=1000):
                details["header_image"] = header.get_attribute("src")
        except:
            pass
        
        # Screenshots
        try:
            screenshot_imgs = page.locator(".highlight_screenshot img, .screenshot_holder img").all()
            urls = []
            for img in screenshot_imgs[:10]:
                src = img.get_attribute("src")
                if src and "steam" in src:
                    full_url = src.replace("116x65", "1920x1080").replace(".116x65", "")
                    urls.append(full_url)
            if urls:
                details["screenshots"] = ", ".join(urls)
        except:
            pass
        
        # Videos - ENHANCED extraction using Selenium method
        try:
            video_urls = extract_video_urls(page, page_content)
            if video_urls:
                details["videos"] = ", ".join(video_urls)
        except Exception as e:
            print(f"   Video extraction error: {e}")
        
        # === DOWNLOAD MEDIA ===
        if download_media_files and (details["screenshots"] != "N/A" or details["videos"] != "N/A"):
            safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50]
            script_dir = os.path.dirname(os.path.abspath(__file__))
            game_media_dir = os.path.join(script_dir, "scraped_data", "steam_media", safe_title)
            os.makedirs(game_media_dir, exist_ok=True)
            
            # Download header
            if details["header_image"] != "N/A":
                downloaded = download_media(details["header_image"], game_media_dir, "header.jpg")
                if downloaded:
                    details["downloaded_images"].append(downloaded)
            
            # Download screenshots (max 5)
            if details["screenshots"] != "N/A":
                screenshot_urls = details["screenshots"].split(", ")
                for idx, img_url in enumerate(screenshot_urls[:5]):
                    downloaded = download_media(img_url, game_media_dir, f"screenshot_{idx+1}.jpg")
                    if downloaded:
                        details["downloaded_images"].append(downloaded)
            
            # Download videos (max 3)
            if details["videos"] != "N/A":
                video_urls = details["videos"].split(", ")
                for idx, video_url in enumerate(video_urls[:3]):
                    try:
                        # Determine file extension
                        if '.m3u8' in video_url or '.mpd' in video_url:
                            ext = ".txt"  # HLS manifest info
                        elif '.mp4' in video_url:
                            ext = ".mp4"
                        else:
                            ext = ".webm"
                        
                        downloaded = download_media(video_url, game_media_dir, f"video_{idx+1}{ext}")
                        if downloaded:
                            details["downloaded_videos"].append(downloaded)
                            print(f"      ✓ Video {idx+1} downloaded")
                    except Exception as e:
                        print(f"      Failed to download video {idx+1}: {e}")
        
    except Exception as e:
        print(f"   Error details {game_title[:30]}: {str(e)[:50]}")
    
    return details

def scrape_game_from_search(game_element):
    """Extract game data from search result element - FAST VERSION."""
    try:
        # Get all text at once
        title = game_element.locator(".title").inner_text(timeout=2000).strip()
        
        # Release date
        release_date = "N/A"
        try:
            release_date = game_element.locator(".search_released").inner_text(timeout=500).strip()
        except:
            pass
        
        # Price info
        price = discount_pct = original_price = "N/A"
        try:
            if game_element.locator(".discount_block").count() > 0:
                discount_block = game_element.locator(".discount_block").first
                try:
                    discount_pct = discount_block.locator(".discount_pct").inner_text(timeout=300).strip()
                except:
                    pass
                try:
                    original_price = discount_block.locator(".discount_original_price").inner_text(timeout=300).strip()
                except:
                    pass
                try:
                    price = discount_block.locator(".discount_final_price").inner_text(timeout=300).strip()
                except:
                    pass
        except:
            pass
        
        if price == "N/A":
            try:
                price_text = game_element.locator(".search_price").inner_text(timeout=500).strip()
                price = "Free" if "Free" in price_text else (price_text if price_text else "N/A")
            except:
                pass
        
        # Reviews
        review_summary_text = "N/A"
        rating_score = None
        rating_percentage = None
        try:
            if game_element.locator(".search_review_summary").count() > 0:
                review_elem = game_element.locator(".search_review_summary").first
                review_summary_text = review_elem.get_attribute("data-tooltip-html", timeout=300) or "N/A"
                rating_score = convert_steam_rating_to_score(review_summary_text)
                rating_percentage = extract_review_percentage(review_summary_text)
        except:
            pass
        
        # URL
        game_url = "N/A"
        try:
            game_url = game_element.get_attribute("href", timeout=500)
        except:
            pass
        
        # Platforms
        platforms = []
        if game_element.locator(".platform_img.win").count() > 0:
            platforms.append("Windows")
        if game_element.locator(".platform_img.mac").count() > 0:
            platforms.append("Mac")
        if game_element.locator(".platform_img.linux").count() > 0:
            platforms.append("Linux")
        
        return {
            "title": title, "release_date": release_date,
            "original_price": original_price, "price": price,
            "discount_percentage": discount_pct, "review_summary": review_summary_text,
            "rating_score": rating_score, "rating_percentage": rating_percentage,
            "url": game_url, "platforms": ", ".join(platforms) if platforms else "N/A"
        }
    except:
        return None

def scrape_page_range(worker_id, start_page, end_page, scrape_details=True, download_media_files=True):
    """Scrape a range of pages - OPTIMIZED VERSION."""
    local_data = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        page.set_default_timeout(10000)  # 10s default
        
        try:
            print(f"[Worker {worker_id}] Pages {start_page}-{end_page}")
            
            for page_num in range(start_page, end_page + 1):
                try:
                    # Navigate to search page
                    url = f"https://store.steampowered.com/search/?filter=topsellers&page={page_num}"
                    page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    
                    # Wait for search results
                    page.wait_for_selector("#search_resultsRows", timeout=8000)
                    time.sleep(0.3)  # Brief pause
                    
                    # Get ALL game elements at once
                    game_elements = page.locator("#search_resultsRows > a").all()
                    
                    # Process all games on this page
                    page_games = []
                    for idx, game_elem in enumerate(game_elements):
                        try:
                            game_data = scrape_game_from_search(game_elem)
                            if game_data and game_data["url"] != "N/A":
                                page_games.append(game_data)
                        except:
                            continue
                    
                    print(f"[Worker {worker_id}] Page {page_num}: Found {len(page_games)} games")
                    
                    # Now scrape details for each game
                    if scrape_details:
                        for game_data in page_games:
                            try:
                                print(f"[Worker {worker_id}] {game_data['title'][:40]} (⭐{game_data['rating_score']})")
                                details = scrape_game_details(page, game_data["url"], game_data["title"], download_media_files)
                                game_data.update(details)
                                
                                # Filter: Only keep games with media
                                if details["screenshots"] != "N/A" or details["videos"] != "N/A":
                                    local_data.append(game_data)
                                else:
                                    print(f"[Worker {worker_id}] ⚠️ Skipped (no media)")
                            except Exception as e:
                                print(f"[Worker {worker_id}] Error: {str(e)[:40]}")
                                continue
                    else:
                        local_data.extend(page_games)
                    
                    print(f"[Worker {worker_id}] Page {page_num} complete: {len(local_data)} total games")
                    time.sleep(1)  # Rate limiting
                    
                except PlaywrightTimeout:
                    print(f"[Worker {worker_id}] Timeout page {page_num}, skipping...")
                    continue
                except Exception as e:
                    print(f"[Worker {worker_id}] Error page {page_num}: {str(e)[:50]}")
                    continue
            
            print(f"[Worker {worker_id}] ✓ Complete: {len(local_data)} games")
            
        except Exception as e:
            print(f"[Worker {worker_id}] Fatal: {str(e)[:60]}")
        finally:
            try:
                context.close()
                browser.close()
            except:
                pass
    
    with data_lock:
        all_game_data.extend(local_data)
    
    return local_data

def scrape_steam_games(max_games=100, num_workers=5, scrape_details=True, download_media_files=True):
    """
    Scrape Steam games using Playwright with multithreading - OPTIMIZED.
    
    Args:
        max_games: Target number of games to scrape
        num_workers: Number of parallel workers (recommended: 3-7)
        scrape_details: Whether to scrape detailed game info
        download_media_files: Whether to download media files
    """
    global all_game_data
    all_game_data = []
    
    # Optimize worker count
    num_workers = min(num_workers, 7)
    
    print(f"🚀 HIGH-PERFORMANCE MODE | {num_workers} workers | Target: {max_games} games")
    print(f"🔍 Details: {'ON' if scrape_details else 'OFF'} | Media Downloads: {'ON' if download_media_files else 'OFF'}")
    print(f"🎬 Filter: Games WITHOUT screenshots/videos will be dropped")
    print(f"⚡ Video extraction: Embedded videos → JSON trailers → Regex → Constructed URLs")
    print(f"📹 Converts HLS manifests to direct .webm/.mp4 URLs\n")
    
    start_time = time.time()
    
    games_per_page = 25
    total_pages_needed = (max_games + games_per_page - 1) // games_per_page
    pages_per_worker = max(1, total_pages_needed // num_workers)
    
    print(f"📄 Pages: {total_pages_needed} | Per worker: {pages_per_worker}\n")
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            start_page = (i * pages_per_worker) + 1
            end_page = start_page + pages_per_worker - 1
            if i == num_workers - 1:
                end_page = total_pages_needed
            
            future = executor.submit(scrape_page_range, i + 1, start_page, end_page, scrape_details, download_media_files)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"⚠️ Worker error: {str(e)[:60]}")
    
    elapsed = time.time() - start_time
    
    if all_game_data:
        df = pd.DataFrame(all_game_data)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, "scraped_data")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "steam_games_detailed.csv")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*70}")
        print(f"✅ COMPLETE | {len(df)} games in {elapsed:.1f}s | ⚡{len(df)/elapsed:.2f} games/s")
        if initial_count > len(df):
            print(f"🗑️  Removed {initial_count - len(df)} duplicates")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*70}\n")
        
        # Show sample
        display_cols = [col for col in ['title', 'price', 'rating_score', 'rating_percentage', 'genres'] if col in df.columns]
        print(df[display_cols].head(10).to_string(index=False))
        
        if scrape_details:
            print(f"\n📊 Statistics:")
            stats = {
                "Single-player": len(df[df['singleplayer'] == 'Yes']),
                "Multi-player": len(df[df['multiplayer'] == 'Yes']),
                "Free games": len(df[df['price'] == 'Free']),
                "On sale": len(df[df['discount_percentage'] != 'N/A']),
                "With screenshots": len(df[df['screenshots'] != 'N/A']),
                "With videos": len(df[df['videos'] != 'N/A'])
            }
            for key, val in stats.items():
                print(f"   {key}: {val}")
            
            rated_games = df[df['rating_score'].notna()]
            if len(rated_games) > 0:
                print(f"\n⭐ Ratings:")
                print(f"   Games rated: {len(rated_games)}")
                print(f"   Average: {rated_games['rating_score'].mean():.1f}/100")
                print(f"   Highest: {rated_games['rating_score'].max()}/100")
                print(f"   Lowest: {rated_games['rating_score'].min()}/100")
    else:
        print("❌ No games scraped")
    
    return all_game_data

if __name__ == "__main__":
    start = time.perf_counter()
    
    # Install: python -m playwright install chromium
    
    # RECOMMENDED: Start with 100 games and 5 workers
    scrape_steam_games(
        max_games=1200,
        num_workers=20,
        scrape_details=True,
        download_media_files=True
    )
    
    end = time.perf_counter()
    print(f"\n⏱️  Total execution: {end - start:.1f}s")