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

def extract_video_urls(driver):
    """Extract video URLs from Steam's video player data."""
    video_urls = []
    try:
        # Method 1: Look for video data in page source
        page_source = driver.page_source
        
        # Find MP4 and WEBM URLs in the page source
        mp4_pattern = r'(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)'
        webm_pattern = r'(https?://[^\s"\'<>]+\.webm[^\s"\'<>]*)'
        
        mp4_urls = re.findall(mp4_pattern, page_source)
        webm_urls = re.findall(webm_pattern, page_source)
        
        # Prefer HD versions
        for url in mp4_urls + webm_urls:
            if 'cdn.akamai.steamstatic.com' in url or 'cdn.cloudflare.steamstatic.com' in url:
                if url not in video_urls:
                    video_urls.append(url)
        
        # Method 2: Check for highlight_player_item data
        try:
            video_elements = driver.find_elements(By.CSS_SELECTOR, ".highlight_player_item")
            for elem in video_elements:
                try:
                    onclick = elem.get_attribute("onclick")
                    if onclick:
                        # Extract video ID from onclick
                        match = re.search(r'(\d+)', onclick)
                        if match:
                            video_id = match.group(1)
                            # Try to construct video URL
                            potential_urls = [
                                f"https://cdn.akamai.steamstatic.com/steam/apps/{video_id}/movie480.webm",
                                f"https://cdn.akamai.steamstatic.com/steam/apps/{video_id}/movie480.mp4",
                                f"https://cdn.cloudflare.steamstatic.com/steam/apps/{video_id}/movie480.webm",
                                f"https://cdn.cloudflare.steamstatic.com/steam/apps/{video_id}/movie480.mp4"
                            ]
                            for url in potential_urls:
                                if url not in video_urls:
                                    video_urls.append(url)
                except:
                    continue
        except:
            pass
        
        # Method 3: Check data attributes on movie elements
        try:
            movie_elements = driver.find_elements(By.CSS_SELECTOR, "[data-webm-source], [data-mp4-source], [data-webm-hd-source], [data-mp4-hd-source]")
            for elem in movie_elements:
                for attr in ['data-webm-hd-source', 'data-mp4-hd-source', 'data-webm-source', 'data-mp4-source']:
                    url = elem.get_attribute(attr)
                    if url and url not in video_urls:
                        video_urls.append(url)
        except:
            pass
            
    except Exception as e:
        print(f"   Error extracting videos: {e}")
    
    return video_urls[:3]  # Limit to 3 videos

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
            game_media_dir = os.path.join(script_dir, "scraped_data", "game_media", safe_title)
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
        
        try:
            review_summary_element = game.find_element(By.CSS_SELECTOR, ".search_review_summary")
            review_summary = review_summary_element.get_attribute("data-tooltip-html") or "N/A"
        except:
            review_summary = "N/A"
        
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
            "title": title, "release_date": release_date, "original_price": original_price,
            "price": price, "discount_percentage": discount_pct, "review_summary": review_summary,
            "url": game_url, "platforms": ", ".join(platforms) if platforms else "N/A"
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
                            print(f"[Worker {worker_id}] Scraping: {game_data['title']}")
                            details = scrape_game_details(driver, game_data["url"], game_data["title"], download_media_files)
                            game_data.update(details)
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
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Starting with {num_workers} workers | Target: {max_games} games")
    print(f"🔍 Details: {'ON' if scrape_details else 'OFF'} | Media: {'ON' if download_media_files else 'OFF'}\n")
    
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
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "scraped_data", "steam_games_detailed.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE | {len(df)} games | {elapsed:.1f}s | {len(df)/elapsed:.2f} games/s")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*60}\n")
        
        print(df[['title', 'price', 'genres', 'singleplayer', 'multiplayer']].head(10).to_string(index=False))
        
        if scrape_details:
            print(f"\n📊 Stats:")
            print(f"   Single-player: {len(df[df['singleplayer'] == 'Yes'])}")
            print(f"   Multi-player: {len(df[df['multiplayer'] == 'Yes'])}")
            print(f"   Free: {len(df[df['price'] == 'Free'])}")
            print(f"   On sale: {len(df[df['discount_percentage'] != 'N/A'])}")
    else:
        print("❌ No games scraped")
    
    return all_game_data

if __name__ == "__main__":
    # Full scrape with media (start small!)
    scrape_steam_games(max_games=50, num_workers=5, scrape_details=True, download_media_files=True)
    
    # Quick scrape
    # scrape_steam_games(max_games=1000, num_workers=10, scrape_details=False, download_media_files=Fals