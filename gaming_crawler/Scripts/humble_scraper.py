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
    """Create a headless Chrome driver with anti-detection measures."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def download_media(url, save_dir, filename):
    """Download images and videos from URLs."""
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

def extract_video_urls(driver):
    """Extract video URLs from Humble Bundle product pages."""
    video_urls = []
    try:
        time.sleep(2)
        
        # Method 1: Look for YouTube embeds in iframes
        try:
            iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='youtube']")
            for iframe in iframes[:3]:
                src = iframe.get_attribute("src")
                if src:
                    # Extract video ID
                    match = re.search(r'embed/([a-zA-Z0-9_-]+)', src)
                    if match:
                        video_id = match.group(1)
                        video_urls.append(f"https://www.youtube.com/watch?v={video_id}")
                        print(f"   Found YouTube video: {video_id}")
        except Exception as e:
            print(f"   YouTube iframe error: {e}")
        
        # Method 2: Search page source for video patterns
        try:
            page_source = driver.page_source
            
            # YouTube patterns
            youtube_patterns = [
                r'youtube\.com/embed/([a-zA-Z0-9_-]+)',
                r'youtube\.com/watch\?v=([a-zA-Z0-9_-]+)',
                r'youtu\.be/([a-zA-Z0-9_-]+)'
            ]
            
            for pattern in youtube_patterns:
                matches = re.findall(pattern, page_source)
                for video_id in matches[:3]:
                    url = f"https://www.youtube.com/watch?v={video_id}"
                    if url not in video_urls:
                        video_urls.append(url)
                        print(f"   Found via regex: {video_id}")
        except Exception as e:
            print(f"   Regex error: {e}")
        
        # Method 3: Look for video elements with direct sources
        try:
            video_elements = driver.find_elements(By.CSS_SELECTOR, "video source")
            for video_elem in video_elements[:3]:
                src = video_elem.get_attribute("src")
                if src and src not in video_urls:
                    video_urls.append(src)
                    print(f"   Found direct video: {src[:100]}...")
        except:
            pass
            
    except Exception as e:
        print(f"   Video extraction error: {e}")
    
    return video_urls[:3]

def has_media_content(screenshots, videos):
    """Check if game has valid screenshots or videos."""
    has_screenshots = screenshots != "N/A" and screenshots.strip() != ""
    has_videos = videos != "N/A" and videos.strip() != ""
    return has_screenshots or has_videos

def scrape_game_details(driver, game_url, game_title, download_media_files=True):
    """Scrape detailed information from a Humble Bundle product page."""
    details = {
        "developer": "N/A",
        "publisher": "N/A",
        "platforms": "N/A",
        "operating_systems": "N/A",
        "system_requirements": "N/A",
        "description": "N/A",
        "header_image": "N/A",
        "screenshots": "N/A",
        "videos": "N/A",
        "downloaded_images": [],
        "downloaded_videos": []
    }
    
    try:
        driver.get(game_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(2)
        
        # Developer
        try:
            dev_elem = driver.find_element(By.CSS_SELECTOR, ".developers-view a, [href*='developer=']")
            details["developer"] = dev_elem.text.strip()
        except:
            pass
        
        # Publisher
        try:
            pub_elem = driver.find_element(By.CSS_SELECTOR, ".publishers-view a, [href*='publisher=']")
            details["publisher"] = pub_elem.text.strip()
        except:
            pass
        
        # Platforms (Steam, Windows, Mac, Linux, etc.)
        try:
            platforms = []
            platform_icons = driver.find_elements(By.CSS_SELECTOR, ".platform-delivery-container i[class*='hb-'], .platforms i[class*='hb-']")
            for icon in platform_icons:
                class_attr = icon.get_attribute("class")
                if "hb-steam" in class_attr:
                    platforms.append("Steam")
                elif "hb-windows" in class_attr:
                    platforms.append("Windows")
                elif "hb-mac" in class_attr:
                    platforms.append("Mac")
                elif "hb-linux" in class_attr:
                    platforms.append("Linux")
            details["platforms"] = ", ".join(set(platforms)) if platforms else "N/A"
        except:
            pass
        
        # Operating Systems (alternative method)
        try:
            os_elements = driver.find_elements(By.CSS_SELECTOR, ".operating-systems li, .OSes li")
            os_list = [elem.get_attribute("title") or elem.text.strip() for elem in os_elements]
            os_list = [os for os in os_list if os]
            if os_list:
                details["operating_systems"] = ", ".join(os_list)
        except:
            pass
        
        # System Requirements
        try:
            sys_req = driver.find_element(By.CSS_SELECTOR, ".system_requirements-view, #system-requirements")
            details["system_requirements"] = sys_req.text.strip() or "N/A"
        except:
            pass
        
        # Description
        try:
            desc_elem = driver.find_element(By.CSS_SELECTOR, ".description-view, .property-content")
            desc_text = desc_elem.text.strip()
            details["description"] = desc_text[:500] + "..." if len(desc_text) > 500 else desc_text
        except:
            pass
        
        # Media handling
        if download_media_files:
            safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50]
            script_dir = os.path.dirname(os.path.abspath(__file__))
            game_media_dir = os.path.join(script_dir, "scraped_data", "game_media", safe_title)
            os.makedirs(game_media_dir, exist_ok=True)
            
            # Header/Capsule Image
            try:
                header_selectors = [
                    ".large_capsule-view img",
                    ".property-view img[src*='imgix']",
                    ".entity-image"
                ]
                
                for selector in header_selectors:
                    try:
                        header_img = driver.find_element(By.CSS_SELECTOR, selector)
                        img_url = header_img.get_attribute("src")
                        if img_url:
                            details["header_image"] = img_url
                            downloaded = download_media(img_url, game_media_dir, "header.jpg")
                            if downloaded:
                                details["downloaded_images"].append(downloaded)
                            break
                    except:
                        continue
            except:
                pass
            
            # Screenshots
            try:
                screenshot_urls = []
                screenshot_selectors = [
                    ".carousel-item img[src*='imgix']",
                    ".single-media.image",
                    ".thumbnail-image"
                ]
                
                for selector in screenshot_selectors:
                    try:
                        images = driver.find_elements(By.CSS_SELECTOR, selector)
                        for idx, img in enumerate(images[:10]):
                            try:
                                img_url = img.get_attribute("src") or img.get_attribute("data-lazy")
                                if img_url and "imgix" in img_url:
                                    screenshot_urls.append(img_url)
                                    downloaded = download_media(img_url, game_media_dir, f"screenshot_{len(screenshot_urls)}.jpg")
                                    if downloaded:
                                        details["downloaded_images"].append(downloaded)
                            except:
                                continue
                        if screenshot_urls:
                            break
                    except:
                        continue
                
                details["screenshots"] = ", ".join(screenshot_urls) if screenshot_urls else "N/A"
            except Exception as e:
                print(f"   Screenshot error: {e}")
            
            # Videos
            try:
                video_urls = extract_video_urls(driver)
                
                if video_urls:
                    details["videos"] = ", ".join(video_urls)
                    
                    # Save video URLs (YouTube links)
                    for idx, video_url in enumerate(video_urls):
                        try:
                            filepath = os.path.join(game_media_dir, f"video_{idx+1}_url.txt")
                            with open(filepath, 'w') as f:
                                f.write(video_url)
                            details["downloaded_videos"].append(filepath)
                        except:
                            continue
            except Exception as e:
                print(f"   Video error: {e}")
        
    except Exception as e:
        print(f"   Error scraping details for {game_title}: {e}")
    
    return details

def scrape_product_from_search_page(product):
    """Extract basic info from search result item."""
    try:
        title = product.find_element(By.CSS_SELECTOR, ".entity-title, .human_name-view").text.strip()
        
        try:
            url = product.find_element(By.CSS_SELECTOR, "a.entity-link").get_attribute("href")
        except:
            try:
                url = product.get_attribute("href")
            except:
                url = "N/A"
        
        original_price = price = discount = "N/A"
        
        try:
            # Check for discount
            discount_elem = product.find_element(By.CSS_SELECTOR, ".discount-amount")
            discount = discount_elem.text.strip()
            
            try:
                original_price = product.find_element(By.CSS_SELECTOR, ".full-price").text.strip()
            except:
                pass
            
            try:
                price = product.find_element(By.CSS_SELECTOR, ".current-price, .price").text.strip()
            except:
                pass
        except:
            # No discount, try regular price
            try:
                price = product.find_element(By.CSS_SELECTOR, ".price, .current-price").text.strip()
            except:
                pass
        
        return {
            "title": title,
            "url": url,
            "original_price": original_price,
            "price": price,
            "discount_percentage": discount
        }
    except Exception as e:
        print(f"   Error parsing product: {e}")
        return None

def scrape_page_range(worker_id, start_page, end_page, scrape_details=True, download_media_files=True):
    """Scrape a range of pages from Humble Bundle store."""
    driver = create_driver()
    local_data = []
    
    try:
        print(f"[Worker {worker_id}] Pages {start_page}-{end_page}")
        
        for page_num in range(start_page, end_page + 1):
            try:
                # Humble Bundle uses different URL patterns
                # Try bestselling first, then on sale
                urls_to_try = [
                    f"https://www.humblebundle.com/store/search?sort=bestselling&page={page_num}",
                    f"https://www.humblebundle.com/store/search?filter=onsale&sort=bestselling&page={page_num}"
                ]
                
                driver.get(urls_to_try[0])
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
                time.sleep(3)
                
                # Find product containers
                products = driver.find_elements(By.CSS_SELECTOR, ".entity-block-container, .entity, a[href*='/store/']")
                
                if not products:
                    print(f"[Worker {worker_id}] No products found on page {page_num}")
                    continue
                
                for product in products:
                    game_data = scrape_product_from_search_page(product)
                    if game_data and game_data.get("url") != "N/A":
                        if scrape_details:
                            print(f"[Worker {worker_id}] Scraping: {game_data['title']}")
                            details = scrape_game_details(driver, game_data["url"], game_data["title"], download_media_files)
                            game_data.update(details)
                            
                            # Filter: Only keep games with media
                            if not has_media_content(game_data.get("screenshots", "N/A"), game_data.get("videos", "N/A")):
                                print(f"[Worker {worker_id}] ⚠️  Skipping {game_data['title']} - No media")
                                continue
                        
                        local_data.append(game_data)
                
                print(f"[Worker {worker_id}] Page {page_num}: {len(products)} products (Total: {len(local_data)})")
                time.sleep(2)
                
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

def scrape_humble_bundle(max_games=500, num_workers=5, scrape_details=True, download_media_files=True):
    """Main function to scrape Humble Bundle store."""
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Humble Bundle Scraper")
    print(f"   Workers: {num_workers} | Target: {max_games} games")
    print(f"   Details: {'ON' if scrape_details else 'OFF'} | Media: {'ON' if download_media_files else 'OFF'}")
    print(f"   Filter: Games WITHOUT media will be DROPPED\n")
    
    start_time = time.time()
    
    # Estimate pages needed (Humble Bundle typically shows ~20-30 items per page)
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
        
        # Filter games without media
        before_filter = len(df)
        df = df[df.apply(lambda row: has_media_content(row.get("screenshots", "N/A"), row.get("videos", "N/A")), axis=1)]
        after_filter = len(df)
        dropped_count = before_filter - after_filter
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "scraped_data", "humble_bundle_games.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE | {len(df)} games | {elapsed:.1f}s | {len(df)/elapsed:.2f} games/s")
        if dropped_count > 0:
            print(f"🗑️  Dropped {dropped_count} games with no media")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*60}\n")
        
        print(df[['title', 'price', 'developer', 'platforms']].head(10).to_string(index=False))
        
        if scrape_details:
            print(f"\n📊 Stats:")
            print(f"   Free: {len(df[df['price'].str.contains('Free', na=False)])}")
            print(f"   On sale: {len(df[df['discount_percentage'] != 'N/A'])}")
            print(f"   With screenshots: {len(df[df['screenshots'] != 'N/A'])}")
            print(f"   With videos: {len(df[df['videos'] != 'N/A'])}")
    else:
        print("❌ No games scraped")
    
    return all_game_data

if __name__ == "__main__":
    start = time.perf_counter()
    
    # Full scrape with media download
    scrape_humble_bundle(max_games=500, num_workers=10, scrape_details=True, download_media_files=True)
    
    # Quick scrape without details (uncomment to use)
    # scrape_humble_bundle(max_games=500, num_workers=10, scrape_details=False, download_media_files=False)
    
    end = time.perf_counter()
    print(f"\nTotal execution time: {end - start:.4f} seconds")