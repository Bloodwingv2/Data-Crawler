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
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
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

def extract_video_urls(driver):
    """Extract game trailer URLs from Epic Games Store."""
    video_urls = []
    try:
        time.sleep(3)
        
        # Method 1: Look for video elements in the page
        try:
            video_elements = driver.find_elements(By.CSS_SELECTOR, "video source[src*='.mp4'], video source[src*='.webm']")
            for video_elem in video_elements[:3]:
                try:
                    video_url = video_elem.get_attribute("src")
                    if video_url:
                        video_urls.append(video_url)
                        print(f"   Found video: {video_url[:100]}...")
                except:
                    continue
        except Exception as e:
            print(f"   Video element search error: {e}")
        
        # Method 2: Search page source for video URLs
        if len(video_urls) < 3:
            try:
                page_source = driver.page_source
                
                # Epic Games typically uses various CDN patterns
                video_patterns = [
                    r'https?://[^"\'<>\s]+epicgames[^"\'<>\s]+\.mp4[^"\'<>\s]*',
                    r'https?://[^"\'<>\s]+\.cloudfront\.net[^"\'<>\s]+\.mp4[^"\'<>\s]*',
                    r'https?://cdn[^"\'<>\s]+/[^"\'<>\s]+\.mp4[^"\'<>\s]*',
                    r'https?://[^"\'<>\s]+\.webm[^"\'<>\s]*',
                ]
                
                for pattern in video_patterns:
                    matches = re.findall(pattern, page_source)
                    for url in matches[:3]:
                        if url not in video_urls:
                            video_urls.append(url)
                            print(f"   Found via regex: {url[:100]}...")
                            if len(video_urls) >= 3:
                                break
                    if len(video_urls) >= 3:
                        break
            except Exception as e:
                print(f"   Regex error: {e}")
        
        # Method 3: Check for iframe embeds
        if len(video_urls) < 3:
            try:
                iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='youtube'], iframe[src*='vimeo']")
                for iframe in iframes[:3]:
                    iframe_url = iframe.get_attribute("src")
                    if iframe_url and iframe_url not in video_urls:
                        video_urls.append(iframe_url)
                        print(f"   Found iframe: {iframe_url[:100]}...")
            except:
                pass
                
    except Exception as e:
        print(f"   Fatal video error: {e}")
    
    return video_urls[:3]

def scrape_game_details(driver, game_url, game_title, download_media_files=True):
    details = {
        "genres": "N/A", "developer": "N/A", "publisher": "N/A",
        "release_date_detailed": "N/A", "rating": "N/A",
        "features": "N/A", "description": "N/A",
        "header_image": "N/A", "screenshots": "N/A", "videos": "N/A",
        "downloaded_images": [], "downloaded_videos": []
    }
    
    try:
        driver.get(game_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(3)
        
        # Handle cookie consent if present
        try:
            cookie_button = driver.find_element(By.CSS_SELECTOR, "button[data-testid='acceptAllCookies'], button[aria-label*='Accept']")
            cookie_button.click()
            time.sleep(1)
        except:
            pass
        
        # Scroll to load content
        driver.execute_script("window.scrollTo(0, 800);")
        time.sleep(2)
        
        # Genres/Tags
        try:
            genre_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='genres'] span, a[href*='browse?genre=']")
            genres = [g.text.strip() for g in genre_elements if g.text.strip()]
            if not genres:
                # Alternative selector
                genre_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Genre')]/following-sibling::*//span")
                genres = [g.text.strip() for g in genre_elements if g.text.strip()]
            details["genres"] = ", ".join(genres[:5]) if genres else "N/A"
        except Exception as e:
            print(f"   Genre error: {e}")
        
        # Developer
        try:
            dev_elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Developer')]/following-sibling::*")
            details["developer"] = dev_elem.text.strip()
        except:
            try:
                dev_elem = driver.find_element(By.CSS_SELECTOR, "div[data-testid='developer']")
                details["developer"] = dev_elem.text.strip()
            except:
                pass
        
        # Publisher
        try:
            pub_elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Publisher')]/following-sibling::*")
            details["publisher"] = pub_elem.text.strip()
        except:
            try:
                pub_elem = driver.find_element(By.CSS_SELECTOR, "div[data-testid='publisher']")
                details["publisher"] = pub_elem.text.strip()
            except:
                pass
        
        # Release Date
        try:
            date_elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Release Date')]/following-sibling::*")
            details["release_date_detailed"] = date_elem.text.strip()
        except:
            try:
                date_elem = driver.find_element(By.CSS_SELECTOR, "time, span[data-testid='release-date']")
                details["release_date_detailed"] = date_elem.text.strip()
            except:
                pass
        
        # Rating
        try:
            rating_elem = driver.find_element(By.CSS_SELECTOR, "div[data-testid='rating'], span[aria-label*='rating']")
            details["rating"] = rating_elem.text.strip()
        except:
            pass
        
        # Features (multiplayer, achievements, etc.)
        try:
            feature_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='features'] span, ul[aria-label*='Features'] li")
            features = [f.text.strip() for f in feature_elements if f.text.strip()]
            details["features"] = ", ".join(features) if features else "N/A"
        except:
            pass
        
        # Description
        try:
            desc_elem = driver.find_element(By.CSS_SELECTOR, "div[data-testid='description'], div.description")
            desc_text = desc_elem.text.strip()
            details["description"] = desc_text[:500] + "..." if len(desc_text) > 500 else desc_text
        except:
            pass
        
        # Media
        if download_media_files:
            safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50]
            script_dir = os.path.dirname(os.path.abspath(__file__))
            game_media_dir = os.path.join(script_dir, "scraped_data", "game_media_epic", safe_title)
            os.makedirs(game_media_dir, exist_ok=True)
            
            # Header/Key Art Image
            try:
                # Epic uses various selectors for main images
                img_selectors = [
                    "img[alt*='Key Art'], img[data-testid='keyArt']",
                    "div[data-component='Picture'] img",
                    "picture source",
                    "img[src*='keyart'], img[src*='landscape']"
                ]
                
                for selector in img_selectors:
                    try:
                        img_elem = driver.find_element(By.CSS_SELECTOR, selector)
                        img_url = img_elem.get_attribute("src") or img_elem.get_attribute("srcset")
                        if img_url:
                            # Get highest quality from srcset if available
                            if 'srcset' in str(img_url):
                                img_url = img_url.split(',')[-1].strip().split(' ')[0]
                            details["header_image"] = img_url
                            downloaded = download_media(img_url, game_media_dir, "header.jpg")
                            if downloaded:
                                details["downloaded_images"].append(downloaded)
                            break
                    except:
                        continue
            except Exception as e:
                print(f"   Header image error: {e}")
            
            # Screenshots
            try:
                screenshot_urls = []
                # Look for gallery images
                img_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='gallery'] img, div[role='img'] img, picture img")
                
                for idx, img in enumerate(img_elements[:5]):
                    try:
                        img_url = img.get_attribute("src")
                        if img_url and 'screenshot' in img_url.lower() or 'gallery' in img_url.lower():
                            screenshot_urls.append(img_url)
                            downloaded = download_media(img_url, game_media_dir, f"screenshot_{idx+1}.jpg")
                            if downloaded:
                                details["downloaded_images"].append(downloaded)
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
        # Title
        title = "N/A"
        try:
            title_elem = game.find_element(By.CSS_SELECTOR, "div[data-testid='offer-title-info-title'], h3, span[data-testid='title']")
            title = title_elem.text.strip()
        except:
            try:
                title_elem = game.find_element(By.CSS_SELECTOR, "span[class*='ProductTitle']")
                title = title_elem.text.strip()
            except:
                pass
        
        # Price
        price = discount_pct = original_price = "N/A"
        try:
            # Check for free games
            free_elem = game.find_elements(By.XPATH, ".//*[contains(text(), 'Free') or contains(text(), 'FREE')]")
            if free_elem:
                price = "Free"
            else:
                # Look for current price
                price_elem = game.find_element(By.CSS_SELECTOR, "span[data-testid='price'], div[class*='Price']")
                price = price_elem.text.strip()
                
                # Look for discount
                try:
                    discount_elem = game.find_element(By.CSS_SELECTOR, "span[data-testid='discount-percentage'], span[class*='Discount']")
                    discount_pct = discount_elem.text.strip()
                except:
                    pass
                
                # Look for original price
                try:
                    original_elem = game.find_element(By.CSS_SELECTOR, "span[data-testid='original-price'], s, del")
                    original_price = original_elem.text.strip()
                except:
                    pass
        except:
            pass
        
        # URL
        try:
            game_url = game.get_attribute("href")
            if not game_url.startswith("http"):
                game_url = "https://store.epicgames.com" + game_url
        except:
            game_url = "N/A"
        
        # Platform (Epic is primarily Windows, but check for others)
        platforms = ["Windows"]  # Default for Epic
        
        return {
            "title": title,
            "price": price,
            "original_price": original_price,
            "discount_percentage": discount_pct,
            "url": game_url,
            "platforms": ", ".join(platforms)
        }
    except Exception as e:
        print(f"   Error scraping game element: {e}")
        return None

def scrape_page_range(worker_id, category, num_pages, scrape_details=True, download_media_files=True):
    driver = create_driver()
    local_data = []
    
    try:
        print(f"[Worker {worker_id}] Category: {category}, Pages: {num_pages}")
        
        # Epic uses different URLs for browsing
        base_urls = {
            "all": "https://store.epicgames.com/en-US/browse",
            "sale": "https://store.epicgames.com/en-US/sales",
            "free": "https://store.epicgames.com/en-US/free-games"
        }
        
        url = base_urls.get(category, base_urls["all"])
        
        for page_num in range(1, num_pages + 1):
            try:
                # Navigate with pagination
                page_url = f"{url}?page={page_num}" if page_num > 1 else url
                driver.get(page_url)
                
                # Wait for content to load
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/'], section[data-testid], div[role='list']"))
                )
                time.sleep(3)
                
                # Scroll to load more content (Epic uses lazy loading)
                for _ in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                
                # Find game cards - Epic uses various selectors
                game_selectors = [
                    "a[href*='/p/'][href*='-']",  # Product pages
                    "section[data-testid='offer-card-layout']",
                    "li[role='listitem'] a"
                ]
                
                games = []
                for selector in game_selectors:
                    games = driver.find_elements(By.CSS_SELECTOR, selector)
                    if games:
                        break
                
                print(f"[Worker {worker_id}] Found {len(games)} games on page {page_num}")
                
                for game in games:
                    game_data = scrape_game_element(game)
                    if game_data and game_data["title"] != "N/A":
                        if scrape_details and game_data["url"] != "N/A":
                            print(f"[Worker {worker_id}] Scraping: {game_data['title']}")
                            details = scrape_game_details(driver, game_data["url"], game_data["title"], download_media_files)
                            game_data.update(details)
                        local_data.append(game_data)
                
                print(f"[Worker {worker_id}] Page {page_num}: {len(games)} games (Total: {len(local_data)})")
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

def scrape_epic_games(max_games=100, num_workers=3, category="all", scrape_details=True, download_media_files=True):
    """
    Scrape Epic Games Store
    
    Args:
        max_games: Target number of games to scrape
        num_workers: Number of parallel workers
        category: "all", "sale", or "free"
        scrape_details: Whether to scrape detailed game info
        download_media_files: Whether to download images and videos
    """
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Starting Epic Games Scraper")
    print(f"   Workers: {num_workers} | Target: {max_games} games")
    print(f"   Category: {category.upper()}")
    print(f"   Details: {'ON' if scrape_details else 'OFF'} | Media: {'ON' if download_media_files else 'OFF'}\n")
    
    start_time = time.time()
    
    # Estimate pages needed (Epic shows ~20-30 games per page)
    games_per_page = 25
    total_pages_needed = (max_games + games_per_page - 1) // games_per_page
    pages_per_worker = max(1, total_pages_needed // num_workers)
    
    print(f"📄 Pages needed: ~{total_pages_needed} | Per worker: ~{pages_per_worker}\n")
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            future = executor.submit(
                scrape_page_range, 
                i + 1, 
                category, 
                pages_per_worker,
                scrape_details, 
                download_media_files
            )
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Worker error: {e}")
    
    elapsed = time.time() - start_time
    
    if all_game_data:
        df = pd.DataFrame(all_game_data)
        df = df.drop_duplicates(subset=['url'], keep='first')
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, "scraped_data", "epic_games_detailed.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE | {len(df)} games | {elapsed:.1f}s | {len(df)/elapsed:.2f} games/s")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*60}\n")
        
        print(df[['title', 'price', 'genres', 'developer']].head(10).to_string(index=False))
        
        if scrape_details:
            print(f"\n📊 Stats:")
            print(f"   Free games: {len(df[df['price'] == 'Free'])}")
            print(f"   On sale: {len(df[df['discount_percentage'] != 'N/A'])}")
            if 'genres' in df.columns:
                print(f"   With genres: {len(df[df['genres'] != 'N/A'])}")
    else:
        print("❌ No games scraped")
    
    return all_game_data

if __name__ == "__main__":
    # Example: Scrape 50 games with full details
    scrape_epic_games(
        max_games=50, 
        num_workers=3, 
        category="all",  # Options: "all", "sale", "free"
        scrape_details=True, 
        download_media_files=True
    )
    
    # Quick scrape without details
    # scrape_epic_games(max_games=100, num_workers=5, scrape_details=False, download_media_files=False)
    
    # Scrape only free games
    # scrape_epic_games(max_games=30, num_workers=2, category="free", scrape_details=True)