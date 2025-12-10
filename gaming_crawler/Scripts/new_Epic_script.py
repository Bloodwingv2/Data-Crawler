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
import random

data_lock = Lock()
all_game_data = []

def create_driver():
    options = webdriver.ChromeOptions()
    # Running visible for better success rate
    # options.add_argument('--headless')
    
    # Enhanced stealth options
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--start-maximized')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Additional anti-detection
    prefs = {
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    }
    options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Remove webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

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

def handle_cookie_consent(driver):
    """Handle Epic's cookie consent popup (equivalent to Steam's age gate)"""
    try:
        time.sleep(2)
        cookie_selectors = [
            "button[id*='onetrust-accept']",
            "button[id='onetrust-accept-btn-handler']",
            "button.onetrust-close-btn-handler",
            "button[aria-label*='Accept']",
            "#onetrust-accept-btn-handler"
        ]
        
        for selector in cookie_selectors:
            try:
                cookie_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                cookie_btn.click()
                time.sleep(1)
                print("   ✓ Accepted cookies")
                return
            except:
                continue
    except:
        pass

def extract_video_urls(driver):
    """Extract game trailer URLs from Epic Games Store."""
    video_urls = []
    try:
        # Wait for the page to fully load
        time.sleep(3)
        
        # Method 0: Extract embedded videos from game page (BEST - actual video files!)
        try:
            # Find all <video> tags with <source> elements
            video_elements = driver.find_elements(By.CSS_SELECTOR, "video source[src*='.webm'], video source[src*='.mp4']")
            
            for video_elem in video_elements[:3]:  # Limit to 3
                try:
                    video_url = video_elem.get_attribute("src")
                    if video_url:
                        # These are direct video files
                        video_urls.append(video_url)
                        print(f"   Found embedded video: {video_url[:100]}...")
                except:
                    continue
            
            if video_urls:
                print(f"   Total embedded videos: {len(video_urls)}")
        except Exception as e:
            print(f"   Embedded video search error: {e}")
        
        # Method 1: Search page source for video URLs
        if len(video_urls) < 3:
            try:
                page_source = driver.page_source
                
                # Pattern for Epic Games CDN videos
                video_patterns = [
                    r'https?://[^"\'<>\s]*cdn[^"\'<>\s]*epicgames[^"\'<>\s]*\.mp4[^"\'<>\s]*',
                    r'https?://[^"\'<>\s]*unrealengine[^"\'<>\s]*\.mp4[^"\'<>\s]*',
                    r'https?://[^"\'<>\s]*\.cloudfront\.net[^"\'<>\s]*\.mp4[^"\'<>\s]*',
                    r'https?://media[^"\'<>\s]*epicgames[^"\'<>\s]*\.(mp4|webm)[^"\'<>\s]*',
                ]
                
                exclude_keywords = ['thumbnail', 'icon', 'logo']
                
                for pattern in video_patterns:
                    matches = re.findall(pattern, page_source)
                    for url in matches:
                        if not any(kw in url.lower() for kw in exclude_keywords):
                            if url not in video_urls:
                                video_urls.append(url)
                                print(f"   Found via regex: {url[:100]}...")
                                if len(video_urls) >= 3:
                                    break
                    if len(video_urls) >= 3:
                        break
                
            except Exception as e:
                print(f"   Regex error: {e}")
        
        # Method 2: Look for YouTube embeds as fallback
        if len(video_urls) < 3:
            try:
                youtube_iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='youtube'], iframe[src*='youtu.be']")
                for iframe in youtube_iframes[:3]:
                    iframe_url = iframe.get_attribute("src")
                    if iframe_url and iframe_url not in video_urls:
                        video_urls.append(iframe_url)
                        print(f"   Found YouTube: {iframe_url[:100]}...")
            except:
                pass
                
    except Exception as e:
        print(f"   Fatal error: {e}")
    
    # Return unique URLs, limit to 3
    unique_urls = []
    for url in video_urls:
        if url not in unique_urls:
            unique_urls.append(url)
    
    return unique_urls[:3]

def scrape_game_details(driver, game_url, game_title, download_media_files=True):
    details = {
        "genres": "N/A", "developer": "N/A", "publisher": "N/A",
        "multiplayer": "No", "singleplayer": "No",
        "features": "N/A", "rating": "N/A",
        "description": "N/A", "header_image": "N/A",
        "screenshots": "N/A", "videos": "N/A", "downloaded_images": [],
        "downloaded_videos": []
    }
    
    try:
        driver.get(game_url)
        time.sleep(random.uniform(3, 5))  # Random delay
        
        # Wait for page load
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except:
            pass
        
        handle_cookie_consent(driver)
        
        # Scroll to load lazy content
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2)
        
        # Genres - Multiple approaches
        try:
            genres = []
            
            # Try data-testid approach
            try:
                genre_container = driver.find_element(By.CSS_SELECTOR, "[data-testid='genres']")
                genre_elements = genre_container.find_elements(By.TAG_NAME, "a")
                genres = [g.text.strip() for g in genre_elements if g.text.strip()]
            except:
                pass
            
            # Try link-based approach
            if not genres:
                try:
                    genre_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/browse?genre=']")
                    genres = [g.text.strip() for g in genre_links if g.text.strip() and len(g.text.strip()) > 2]
                except:
                    pass
            
            details["genres"] = ", ".join(genres[:5]) if genres else "N/A"
        except:
            pass
        
        # Developer & Publisher
        try:
            # Find all text elements and search for Developer/Publisher patterns
            all_text = driver.find_elements(By.XPATH, "//*[contains(text(), 'Developer') or contains(text(), 'Publisher')]")
            
            for elem in all_text:
                text = elem.text.strip()
                if 'Developer' in text:
                    try:
                        # Try to get next sibling or parent's next element
                        parent = elem.find_element(By.XPATH, "./..")
                        siblings = parent.find_elements(By.XPATH, ".//*")
                        for sib in siblings:
                            sib_text = sib.text.strip()
                            if sib_text and 'Developer' not in sib_text and len(sib_text) > 2:
                                details["developer"] = sib_text
                                break
                    except:
                        pass
                
                if 'Publisher' in text:
                    try:
                        parent = elem.find_element(By.XPATH, "./..")
                        siblings = parent.find_elements(By.XPATH, ".//*")
                        for sib in siblings:
                            sib_text = sib.text.strip()
                            if sib_text and 'Publisher' not in sib_text and len(sib_text) > 2:
                                details["publisher"] = sib_text
                                break
                    except:
                        pass
        except:
            pass
        
        # Features and multiplayer detection
        try:
            # Look for features in various places
            page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
            
            if any(word in page_text for word in ["multiplayer", "co-op", "online play", "pvp"]):
                details["multiplayer"] = "Yes"
            
            if any(word in page_text for word in ["single player", "single-player", "singleplayer"]):
                details["singleplayer"] = "Yes"
            
            # Try to extract feature list
            feature_keywords = ["cloud saves", "achievements", "controller support", "leaderboards"]
            found_features = [kw for kw in feature_keywords if kw in page_text]
            if found_features:
                details["features"] = ", ".join(found_features)
        except:
            pass
        
        # Description
        try:
            # Try multiple selectors for description
            desc_selectors = [
                "[data-testid='about']",
                "[data-testid='description']",
                "div[class*='Description']",
                "p[class*='description']"
            ]
            
            for selector in desc_selectors:
                try:
                    desc_elem = driver.find_element(By.CSS_SELECTOR, selector)
                    desc_text = desc_elem.text.strip()
                    if desc_text and len(desc_text) > 20:
                        details["description"] = desc_text[:500] + "..." if len(desc_text) > 500 else desc_text
                        break
                except:
                    continue
        except:
            pass
        
        # Media
        if download_media_files:
            safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50]
            script_dir = os.path.dirname(os.path.abspath(__file__))
            game_media_dir = os.path.join(script_dir, "scraped_data", "game_media_epic", safe_title)
            os.makedirs(game_media_dir, exist_ok=True)
            
            # Header Image - look for large images
            try:
                all_images = driver.find_elements(By.TAG_NAME, "img")
                for img in all_images:
                    img_url = img.get_attribute("src")
                    if img_url and any(kw in img_url.lower() for kw in ['keyart', 'hero', 'portrait', '1280', '1920']):
                        details["header_image"] = img_url
                        downloaded = download_media(img_url, game_media_dir, "header.jpg")
                        if downloaded:
                            details["downloaded_images"].append(downloaded)
                        break
            except:
                pass
            
            # Screenshots
            try:
                screenshot_urls = []
                driver.execute_script("window.scrollTo(0, 1500);")
                time.sleep(1)
                
                # Find screenshot gallery
                all_images = driver.find_elements(By.TAG_NAME, "img")
                for idx, img in enumerate(all_images):
                    try:
                        img_url = img.get_attribute("src")
                        if img_url and 'screenshot' in img_url.lower() and img_url not in screenshot_urls:
                            screenshot_urls.append(img_url)
                            downloaded = download_media(img_url, game_media_dir, f"screenshot_{len(screenshot_urls)}.jpg")
                            if downloaded:
                                details["downloaded_images"].append(downloaded)
                            if len(screenshot_urls) >= 5:
                                break
                    except:
                        continue
                
                details["screenshots"] = ", ".join(screenshot_urls) if screenshot_urls else "N/A"
            except:
                pass
            
            # Videos
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
    """Extract data from a single game card element"""
    try:
        # Get the URL first - this is most reliable
        game_url = "N/A"
        try:
            game_url = game.get_attribute("href")
            if game_url and not game_url.startswith("http"):
                game_url = "https://store.epicgames.com" + game_url
        except:
            pass
        
        # Get all text content from the card
        try:
            card_text = game.text.strip()
        except:
            card_text = ""
        
        # Split into lines for easier parsing
        lines = [line.strip() for line in card_text.split('\n') if line.strip()]
        
        # Initialize defaults
        title = "N/A"
        release_date = "N/A"
        price = "N/A"
        discount_pct = "N/A"
        original_price = "N/A"
        review_summary = "N/A"
        platforms = "Windows"
        
        # Extract title - usually the longest line that's not a price or date
        for line in lines:
            # Skip lines that look like prices or discounts
            if any(char in line for char in ['$', '€', '£', '%']) or 'free' in line.lower():
                continue
            # Skip very short lines or very long lines
            if len(line) > 3 and len(line) < 80:
                # This is likely the title
                title = line
                break
        
        # If no title found from text, try to extract from URL
        if title == "N/A" and game_url != "N/A":
            try:
                # URL pattern: /p/game-name-slug
                url_parts = game_url.split('/p/')
                if len(url_parts) > 1:
                    slug = url_parts[1].split('?')[0].split('/')[0]
                    # Convert slug to title
                    title = slug.replace('-', ' ').title()
            except:
                pass
        
        # Look for "Free" or prices in the text
        for line in lines:
            line_lower = line.lower()
            
            # Check for Free
            if line_lower == 'free' or 'free to play' in line_lower:
                price = "Free"
                continue
            
            # Check for discount percentage
            if '%' in line and 'off' in line_lower:
                discount_pct = line
                continue
            
            # Check for prices
            price_match = re.search(r'([\$€£][\d,]+\.?\d*)', line)
            if price_match:
                found_price = price_match.group(1)
                # If we already have a price, this might be original price
                if price != "N/A" and price != "Free":
                    original_price = price
                    price = found_price
                else:
                    price = found_price
        
        # Look for release date patterns
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # 12/25/2024
            r'\d{4}-\d{2}-\d{2}',          # 2024-12-25
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}',  # Dec 25, 2024
            r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}'    # 25 Dec 2024
        ]
        
        for line in lines:
            for pattern in date_patterns:
                date_match = re.search(pattern, line, re.IGNORECASE)
                if date_match:
                    release_date = date_match.group(0)
                    break
            if release_date != "N/A":
                break
        
        # Try to find review/rating info
        for line in lines:
            if any(word in line.lower() for word in ['review', 'rating', 'star', 'score']):
                review_summary = line
                break
        
        return {
            "title": title,
            "release_date": release_date,
            "original_price": original_price,
            "price": price,
            "discount_percentage": discount_pct,
            "review_summary": review_summary,
            "url": game_url,
            "platforms": platforms
        }
    except Exception as e:
        print(f"   Error in scrape_game_element: {e}")
        return None

def scrape_page_range(worker_id, start_page, end_page, scrape_details=True, download_media_files=True):
    driver = create_driver()
    local_data = []
    
    try:
        print(f"[Worker {worker_id}] Pages {start_page}-{end_page}")
        
        for page_num in range(start_page, end_page + 1):
            try:
                # Epic Games browse URL with pagination
                start_index = (page_num - 1) * 40
                url = f"https://store.epicgames.com/en-US/browse?sortBy=releaseDate&sortDir=DESC&category=Game&count=40&start={start_index}"
                
                print(f"[Worker {worker_id}] Loading page {page_num}...")
                driver.get(url)
                
                # Wait for page load with random delay
                time.sleep(random.uniform(4, 6))
                
                # Handle cookie consent
                handle_cookie_consent(driver)
                
                # Scroll to load lazy content
                for scroll in range(4):
                    driver.execute_script(f"window.scrollTo(0, {(scroll + 1) * 800});")
                    time.sleep(random.uniform(1, 2))
                
                # Find all links that go to game pages
                print(f"[Worker {worker_id}] Looking for game links...")
                
                # Try multiple selectors
                game_links = []
                selectors_to_try = [
                    "a[href*='/p/']",
                    "a[role='link'][href*='/p/']",
                    "li a[href*='/p/']",
                    "section a[href*='/p/']",
                    "[data-component='OfferCard'] a",
                ]
                
                for selector in selectors_to_try:
                    try:
                        found_links = driver.find_elements(By.CSS_SELECTOR, selector)
                        if found_links:
                            game_links = found_links
                            print(f"[Worker {worker_id}] Found {len(game_links)} links with selector: {selector}")
                            break
                    except Exception as e:
                        continue
                
                if not game_links:
                    print(f"[Worker {worker_id}] ⚠ No games found on page {page_num}")
                    continue
                
                # Process each game
                processed = 0
                for game in game_links:
                    game_data = scrape_game_element(game)
                    if game_data and game_data["title"] != "N/A":
                        processed += 1
                        if scrape_details and game_data["url"] != "N/A":
                            print(f"[Worker {worker_id}] Scraping details: {game_data['title']}")
                            details = scrape_game_details(driver, game_data["url"], game_data["title"], download_media_files)
                            game_data.update(details)
                            # Go back to listing
                            driver.get(url)
                            time.sleep(random.uniform(2, 3))
                        local_data.append(game_data)
                
                print(f"[Worker {worker_id}] Page {page_num}: {processed}/{len(game_links)} games processed (Total: {len(local_data)})")
                time.sleep(random.uniform(2, 3))
                
            except Exception as e:
                print(f"[Worker {worker_id}] Error page {page_num}: {e}")
                continue
        
        print(f"[Worker {worker_id}] ✓ Done: {len(local_data)} games")
        
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass
    
    with data_lock:
        all_game_data.extend(local_data)
    
    return local_data

def scrape_epic_games(max_games=100, num_workers=2, scrape_details=True, download_media_files=True):
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Starting with {num_workers} workers | Target: {max_games} games")
    print(f"📝 Details: {'ON' if scrape_details else 'OFF'} | Media: {'ON' if download_media_files else 'OFF'}")
    print(f"⚠️  Running in VISIBLE mode to avoid detection\n")
    
    start_time = time.time()
    
    games_per_page = 40
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
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "scraped_data", "epic_games_detailed.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE | {len(df)} games | {elapsed:.1f}s | {len(df)/elapsed:.2f} games/s")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*60}\n")
        
        available_cols = [col for col in ['title', 'price', 'genres', 'singleplayer', 'multiplayer'] if col in df.columns]
        if available_cols:
            print(df[available_cols].head(10).to_string(index=False))
        
        if scrape_details:
            print(f"\n📊 Stats:")
            if 'singleplayer' in df.columns:
                print(f"   Single-player: {len(df[df['singleplayer'] == 'Yes'])}")
            if 'multiplayer' in df.columns:
                print(f"   Multi-player: {len(df[df['multiplayer'] == 'Yes'])}")
            print(f"   Free: {len(df[df['price'] == 'Free'])}")
            print(f"   On sale: {len(df[df['discount_percentage'] != 'N/A'])}")
    else:
        print("❌ No games scraped")
        print("\n💡 Tips:")
        print("   1. Epic Games Store may be blocking automated access")
        print("   2. Try running with num_workers=1 for better stability")
        print("   3. Ensure Chrome browser windows open and pages load properly")
    
    return all_game_data

if __name__ == "__main__":
    # Start with small test - VISIBLE MODE for debugging
    scrape_epic_games(max_games=20, num_workers=1, scrape_details=False, download_media_files=False)
    
    # After confirming it works, try with details
    # scrape_epic_games(max_games=50, num_workers=2, scrape_details=True, download_media_files=True)