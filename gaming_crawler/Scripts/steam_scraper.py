import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
import re
import json

data_lock = Lock()
all_game_data = []

def convert_steam_rating_to_score(review_text):
    """
    Convert Steam's text ratings to numerical scores (0-100).
    
    Steam Rating Scale:
    - Overwhelmingly Positive: 95
    - Very Positive: 85
    - Positive: 75
    - Mostly Positive: 70
    - Mixed: 50
    - Mostly Negative: 30
    - Negative: 25
    - Very Negative: 15
    - Overwhelmingly Negative: 5
    """
    if not review_text or review_text == "N/A":
        return None
    
    review_lower = review_text.lower()
    
    # Define rating mappings
    rating_map = {
        'overwhelmingly positive': 95,
        'very positive': 85,
        'positive': 75,
        'mostly positive': 70,
        'mixed': 50,
        'mostly negative': 30,
        'negative': 25,
        'very negative': 15,
        'overwhelmingly negative': 5
    }
    
    # Check for each rating type
    for rating_text, score in rating_map.items():
        if rating_text in review_lower:
            return score
    
    # If no match found, return None
    return None

def extract_review_percentage(review_text):
    """
    Extract the percentage from Steam's review tooltip.
    Example: "Very Positive<br>85% of the 12,345 user reviews are positive."
    Returns the percentage as an integer.
    """
    if not review_text or review_text == "N/A":
        return None
    
    # Look for percentage pattern
    match = re.search(r'(\d+)%', review_text)
    if match:
        return int(match.group(1))
    
    return None

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def download_media(url, save_dir, filename):
    try:
        # Special handling for video manifests (m3u8, mpd)
        if url.endswith('.m3u8') or url.endswith('.mpd'):
            # Save the manifest URL to a text file instead
            filepath = os.path.join(save_dir, filename.replace('.mp4', '.txt').replace('.webm', '.txt'))
            with open(filepath, 'w') as f:
                f.write(f"Video Manifest URL:\n{url}\n\n")
                f.write("Note: This is an HLS/DASH manifest. Use a video player that supports streaming (VLC, ffmpeg) to download/play.\n")
                f.write(f"\nTo download with ffmpeg:\nffmpeg -i \"{url}\" -c copy \"{filename.replace('.txt', '.mp4')}\"\n")
            return filepath
        
        # Regular download for images and direct video files
        response = requests.get(url, timeout=15, stream=True, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        if response.status_code == 200:
            filepath = os.path.join(save_dir, filename)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return filepath
    except Exception as e:
        print(f"   Error downloading {filename}: {e}")
    return None

def handle_age_gate(driver):
    try:
        age_gate = driver.find_elements(By.CSS_SELECTOR, ".agegate_birthday_selector")
        if age_gate:
            year_select = driver.find_element(By.ID, "ageYear")
            year_select.click()
            time.sleep(0.3)
            year_options = driver.find_elements(By.CSS_SELECTOR, "#ageYear option")
            if len(year_options) > 10:
                year_options[10].click()
            driver.find_element(By.CSS_SELECTOR, "#age_gate_btn_continue").click()
            time.sleep(2)
    except:
        pass

def convert_hls_to_direct_url(hls_url):
    """Convert HLS manifest URL to direct video URLs."""
    try:
        # Pattern: https://video.akamai.steamstatic.com/store_trailers/252490/824633/.../hls_264_master.m3u8
        # Steam structure: base_url + video_id + hash + timestamp + filename
        
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
            hls_url  # Keep original as fallback
        ]
        
        return possible_formats
    except:
        return [hls_url]

def extract_video_urls(driver):
    """Extract game trailer URLs from Steam's JSON data-props and embedded videos."""
    video_urls = []
    try:
        # Wait for the page to fully load
        time.sleep(3)
        
        # Method 0: Extract embedded videos from game description (BEST - actual video files!)
        try:
            # Find all <video> tags with <source> elements
            video_elements = driver.find_elements(By.CSS_SELECTOR, "video source[src*='.webm'], video source[src*='.mp4']")
            
            for video_elem in video_elements[:3]:  # Limit to 3
                try:
                    video_url = video_elem.get_attribute("src")
                    if video_url and 'store_item_assets' in video_url:
                        # These are direct video files from game description
                        video_urls.append(video_url)
                        print(f"   Found embedded video: {video_url[:100]}...")
                except:
                    continue
            
            if video_urls:
                print(f"   Total embedded videos: {len(video_urls)}")
        except Exception as e:
            print(f"   Embedded video search error: {e}")
        
        # Method 1: Parse the data-props JSON for trailers
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
                        carousel = driver.find_element(By.CSS_SELECTOR, selector)
                        if carousel:
                            break
                    except:
                        continue
                
                if carousel:
                    data_props = carousel.get_attribute("data-props")
                    
                    if data_props:
                        # Unescape HTML entities
                        data_props = data_props.replace('&quot;', '"').replace('&amp;', '&').replace('\\/', '/')
                        
                        # Parse the JSON data
                        data = json.loads(data_props)
                        
                        # Extract trailer URLs from the "trailers" array
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
                                            print(f"   Added converted HLS: {url[:100]}...")
                                            break
                                    else:
                                        # If no direct URL, keep HLS as last resort
                                        video_urls.append(hls_url)
                                        print(f"   Added HLS manifest: {hls_url[:100]}...")
                                        
                                # Fallback to DASH manifest
                                elif "dashManifests" in trailer and trailer["dashManifests"] and len(trailer["dashManifests"]) > 0:
                                    url = trailer["dashManifests"][0].replace('\\/', '/')
                                    video_urls.append(url)
                                    print(f"   Added DASH: {url[:100]}...")
                        
                        if len(video_urls) > 0:
                            print(f"   Total from data-props: {len(video_urls)} trailer(s)")
                    
            except json.JSONDecodeError as e:
                print(f"   JSON decode error: {e}")
            except Exception as e:
                print(f"   data-props error: {e}")
        
        # Method 2: Regex search for embedded video URLs in page source
        if len(video_urls) < 3:
            try:
                page_source = driver.page_source
                
                # Pattern for embedded game description videos (direct files!)
                embedded_pattern = r'https?://shared\.fastly\.steamstatic\.com/store_item_assets/steam/apps/\d+/extras/[^"\'<>\s]+\.webm[^"\'<>\s]*'
                embedded_matches = re.findall(embedded_pattern, page_source)
                
                for url in embedded_matches[:3]:
                    if url not in video_urls:
                        video_urls.append(url)
                        print(f"   Found via regex: {url[:100]}...")
                        if len(video_urls) >= 3:
                            break
                
                # Also search for direct trailer videos
                video_patterns = [
                    r'https?://video\.[^"\'<>\s]+/store_trailers/[^"\'<>\s]+/movie480_vp9\.webm',
                    r'https?://video\.[^"\'<>\s]+/store_trailers/[^"\'<>\s]+/movie_max_vp9\.webm',
                    r'https?://video\.[^"\'<>\s]+/store_trailers/[^"\'<>\s]+/movie480\.webm',
                    r'https?://cdn\.[^"\'<>\s]+/steam/apps/\d+/movie480\.webm',
                ]
                
                exclude_keywords = ['steamdeck', 'hardware']
                
                for pattern in video_patterns:
                    matches = re.findall(pattern, page_source)
                    for url in matches:
                        if not any(kw in url.lower() for kw in exclude_keywords):
                            if url not in video_urls:
                                video_urls.append(url)
                                print(f"   Found trailer: {url[:100]}...")
                                if len(video_urls) >= 3:
                                    break
                    if len(video_urls) >= 3:
                        break
                
            except Exception as e:
                print(f"   Regex error: {e}")
        
        # Method 3: Construct URLs from app ID as last resort
        if len(video_urls) == 0:
            try:
                current_url = driver.current_url
                app_id_match = re.search(r'/app/(\d+)/', current_url)
                
                if app_id_match:
                    app_id = app_id_match.group(1)
                    
                    constructed_urls = [
                        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/movie480.webm",
                        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/movie480.webm",
                    ]
                    
                    video_urls.append(constructed_urls[0])
                    print(f"   Added constructed: {constructed_urls[0]}")
            except Exception as e:
                print(f"   Construction error: {e}")
            
    except Exception as e:
        print(f"   Fatal error: {e}")
    
    # Return unique URLs, limit to 3
    unique_urls = []
    for url in video_urls:
        if url not in unique_urls:
            unique_urls.append(url)
    
    return unique_urls[:3]

def has_media_content(screenshots, videos):
    """Check if game has valid screenshots or videos."""
    has_screenshots = screenshots != "N/A" and screenshots.strip() != ""
    has_videos = videos != "N/A" and videos.strip() != ""
    return has_screenshots or has_videos

def scrape_game_details(driver, game_url, game_title, download_media_files=True):
    details = {
        "genres": "N/A", "categories": "N/A", "multiplayer": "No", "singleplayer": "No",
        "system_requirements_windows": "N/A", "system_requirements_mac": "N/A",
        "system_requirements_linux": "N/A", "header_image": "N/A",
        "screenshots": "N/A", "videos": "N/A", "downloaded_images": [],
        "downloaded_videos": []
    }
    
    try:
        driver.get(game_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(2)
        handle_age_gate(driver)
        
        # Genres
        try:
            genres = [g.text.strip() for g in driver.find_elements(By.CSS_SELECTOR, ".details_block a[href*='genre']") if g.text.strip()]
            details["genres"] = ", ".join(genres) if genres else "N/A"
        except:
            pass
        
        # Categories & Multiplayer/Singleplayer
        try:
            categories = []
            for cat in driver.find_elements(By.CSS_SELECTOR, ".game_area_features_list a, .label"):
                cat_text = cat.text.strip()
                if cat_text:
                    categories.append(cat_text)
                    if "multi-player" in cat_text.lower() or "multiplayer" in cat_text.lower():
                        details["multiplayer"] = "Yes"
                    if "single-player" in cat_text.lower() or "singleplayer" in cat_text.lower():
                        details["singleplayer"] = "Yes"
            details["categories"] = ", ".join(set(categories)) if categories else "N/A"
        except:
            pass
        
        # System Requirements
        try:
            win_req = driver.find_element(By.CSS_SELECTOR, ".game_area_sys_req_leftCol, .game_area_sys_req_full")
            details["system_requirements_windows"] = win_req.text.strip() or "N/A"
        except:
            pass
        
        try:
            mac_req = driver.find_element(By.CSS_SELECTOR, ".game_area_sys_req_rightCol")
            req_text = mac_req.text.strip()
            if "mac" in req_text.lower() or "os x" in req_text.lower():
                details["system_requirements_mac"] = req_text
        except:
            pass
        
        try:
            for elem in driver.find_elements(By.CSS_SELECTOR, ".game_area_sys_req"):
                req_text = elem.text.strip()
                if "linux" in req_text.lower() or "steamos" in req_text.lower():
                    details["system_requirements_linux"] = req_text
                    break
        except:
            pass
        
        # Media
        if download_media_files:
            safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50]
            script_dir = os.path.dirname(os.path.abspath(__file__))
            game_media_dir = os.path.join(script_dir, "scraped_data", "steam_media", safe_title)
            os.makedirs(game_media_dir, exist_ok=True)
            
            # Header Image
            try:
                header_img = driver.find_element(By.CSS_SELECTOR, ".game_header_image_full")
                img_url = header_img.get_attribute("src")
                details["header_image"] = img_url
                downloaded = download_media(img_url, game_media_dir, "header.jpg")
                if downloaded:
                    details["downloaded_images"].append(downloaded)
            except:
                pass
            
            # Screenshots
            try:
                screenshot_urls = []
                for idx, img in enumerate(driver.find_elements(By.CSS_SELECTOR, ".highlight_screenshot_link img, .screenshot_holder a img")[:5]):
                    try:
                        img_url = img.get_attribute("src")
                        if img_url:
                            screenshot_urls.append(img_url)
                            downloaded = download_media(img_url, game_media_dir, f"screenshot_{idx+1}.jpg")
                            if downloaded:
                                details["downloaded_images"].append(downloaded)
                    except:
                        continue
                details["screenshots"] = ", ".join(screenshot_urls) if screenshot_urls else "N/A"
            except:
                pass
            
            # Videos - Enhanced extraction
            try:
                video_urls = extract_video_urls(driver)
                
                if video_urls:
                    details["videos"] = ", ".join(video_urls)
                    
                    # Download videos
                    for idx, video_url in enumerate(video_urls):
                        try:
                            ext = ".webm" if ".webm" in video_url else ".mp4"
                            downloaded = download_media(video_url, game_media_dir, f"video_{idx+1}{ext}")
                            if downloaded:
                                details["downloaded_videos"].append(downloaded)
                        except Exception as e:
                            print(f"   Failed to download video {idx+1}: {e}")
            except Exception as e:
                print(f"   Video extraction error: {e}")
        
    except Exception as e:
        print(f"   Error scraping details for {game_title}: {e}")
    
    return details

def scrape_game_element(game):
    try:
        title = game.find_element(By.CSS_SELECTOR, ".title").text
        try:
            release_date = game.find_element(By.CSS_SELECTOR, ".search_released").text
        except:
            release_date = "N/A"
        
        price = discount_pct = original_price = "N/A"
        
        try:
            discount_block = game.find_element(By.CSS_SELECTOR, ".discount_block")
            try:
                discount_pct = discount_block.find_element(By.CSS_SELECTOR, ".discount_pct").text.strip()
                original_price = discount_block.find_element(By.CSS_SELECTOR, ".discount_original_price").text.strip()
                price = discount_block.find_element(By.CSS_SELECTOR, ".discount_final_price").text.strip()
            except:
                try:
                    price = discount_block.find_element(By.CSS_SELECTOR, ".discount_final_price").text.strip()
                except:
                    pass
        except:
            try:
                price_text = game.find_element(By.CSS_SELECTOR, ".search_price").text.strip()
                price = "Free" if "Free" in price_text else (price_text if price_text else "N/A")
            except:
                pass
        
        # Extract review data with numerical conversion
        review_summary_text = "N/A"
        rating_score = None
        rating_percentage = None
        
        try:
            review_summary_element = game.find_element(By.CSS_SELECTOR, ".search_review_summary")
            review_summary_text = review_summary_element.get_attribute("data-tooltip-html") or "N/A"
            
            # Convert text rating to numerical score
            rating_score = convert_steam_rating_to_score(review_summary_text)
            
            # Extract percentage from tooltip
            rating_percentage = extract_review_percentage(review_summary_text)
            
        except:
            pass
        
        try:
            game_url = game.get_attribute("href")
        except:
            game_url = "N/A"
        
        platforms = []
        if game.find_elements(By.CSS_SELECTOR, ".platform_img.win"):
            platforms.append("Windows")
        if game.find_elements(By.CSS_SELECTOR, ".platform_img.mac"):
            platforms.append("Mac")
        if game.find_elements(By.CSS_SELECTOR, ".platform_img.linux"):
            platforms.append("Linux")
        
        return {
            "title": title, 
            "release_date": release_date, 
            "original_price": original_price,
            "price": price, 
            "discount_percentage": discount_pct, 
            "review_summary": review_summary_text,
            "rating_score": rating_score,  # NEW: Numerical rating (0-100)
            "rating_percentage": rating_percentage,  # NEW: Exact percentage from Steam
            "url": game_url, 
            "platforms": ", ".join(platforms) if platforms else "N/A"
        }
    except:
        return None

def scrape_page_range(worker_id, start_page, end_page, scrape_details=True, download_media_files=True):
    driver = create_driver()
    local_data = []
    
    try:
        print(f"[Worker {worker_id}] Pages {start_page}-{end_page}")
        
        for page_num in range(start_page, end_page + 1):
            try:
                driver.get(f"https://store.steampowered.com/search/?filter=topsellers&page={page_num}")
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#search_resultsRows > a")))
                time.sleep(2)
                
                games = driver.find_elements(By.CSS_SELECTOR, "#search_resultsRows > a")
                
                for game in games:
                    game_data = scrape_game_element(game)
                    if game_data:
                        if scrape_details and game_data["url"] != "N/A":
                            print(f"[Worker {worker_id}] Scraping: {game_data['title']} (Rating: {game_data['rating_score']})")
                            details = scrape_game_details(driver, game_data["url"], game_data["title"], download_media_files)
                            game_data.update(details)
                            
                            # Filter: Only keep games with screenshots or videos
                            if not has_media_content(game_data.get("screenshots", "N/A"), game_data.get("videos", "N/A")):
                                print(f"[Worker {worker_id}] ⚠️  Skipping {game_data['title']} - No media content")
                                continue
                        
                        local_data.append(game_data)
                
                print(f"[Worker {worker_id}] Page {page_num}: {len(games)} games (Total: {len(local_data)})")
                time.sleep(1)
                
            except Exception as e:
                print(f"[Worker {worker_id}] Error page {page_num}: {e}")
                continue
        
        print(f"[Worker {worker_id}] ✓ Done: {len(local_data)} games")
        
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal: {e}")
    finally:
        driver.quit()
    
    with data_lock:
        all_game_data.extend(local_data)
    
    return local_data

def scrape_steam_games(max_games=100, num_workers=5, scrape_details=True, download_media_files=True):
    """Scrape Steam games using multithreading."""
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Starting with {num_workers} workers | Target: {max_games} games")
    print(f"🔍 Details: {'ON' if scrape_details else 'OFF'} | Media: {'ON' if download_media_files else 'OFF'}")
    print(f"🎬 Filter: Games WITHOUT screenshots/videos will be DROPPED")
    print(f"⭐ Ratings will be converted to numerical scores (0-100)\n")
    
    start_time = time.time()
    
    games_per_page = 25
    total_pages_needed = (max_games + games_per_page - 1) // games_per_page
    pages_per_worker = max(1, total_pages_needed // num_workers)
    
    print(f"📄 Pages needed: {total_pages_needed} | Per worker: {pages_per_worker}\n")
    
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
                print(f"Worker error: {e}")
    
    elapsed = time.time() - start_time
    
    if all_game_data:
        df = pd.DataFrame(all_game_data)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        
        # Additional filter at DataFrame level (safety check)
        before_filter = len(df)
        df = df[df.apply(lambda row: has_media_content(row.get("screenshots", "N/A"), row.get("videos", "N/A")), axis=1)]
        after_filter = len(df)
        dropped_count = before_filter - after_filter
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "scraped_data", "steam_games_detailed.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE | {len(df)} games | {elapsed:.1f}s | {len(df)/elapsed:.2f} games/s")
        if dropped_count > 0:
            print(f"🗑️  Dropped {dropped_count} games with no media content")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*60}\n")
        
        print(df[['title', 'price', 'rating_score', 'rating_percentage', 'genres']].head(10).to_string(index=False))
        
        if scrape_details:
            print(f"\n📊 Stats:")
            print(f"   Single-player: {len(df[df['singleplayer'] == 'Yes'])}")
            print(f"   Multi-player: {len(df[df['multiplayer'] == 'Yes'])}")
            print(f"   Free: {len(df[df['price'] == 'Free'])}")
            print(f"   On sale: {len(df[df['discount_percentage'] != 'N/A'])}")
            print(f"   With screenshots: {len(df[df['screenshots'] != 'N/A'])}")
            print(f"   With videos: {len(df[df['videos'] != 'N/A'])}")
            
            # Rating statistics
            rated_games = df[df['rating_score'].notna()]
            if len(rated_games) > 0:
                print(f"   With ratings: {len(rated_games)}")
                print(f"   Average rating: {rated_games['rating_score'].mean():.1f}/100")
                print(f"   Highest rated: {rated_games['rating_score'].max()}/100")
    else:
        print("❌ No games scraped")
    
    return all_game_data

if __name__ == "__main__":
    start = time.perf_counter()
    # Full scrape with media (start small!)
    scrape_steam_games(max_games=1000, num_workers=10, scrape_details=True, download_media_files=True)
    end = time.perf_counter()
    print(f"Total execution time: {end - start:.4f} seconds")
    # Increase number of workers above for faster scraping on powerful machines
    # Quick scrape
    # scrape_steam_games(max_games=1000, num_workers=10, scrape_details=False, download_media_files=False)