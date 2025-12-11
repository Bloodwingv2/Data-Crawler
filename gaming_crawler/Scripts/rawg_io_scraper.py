"""
RAWG.io Game Database Advanced Web Scraper
Multi-threaded scraping with media downloads
Uses Selenium in headless mode (no GUI)
"""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
import time
import os
import re
from datetime import datetime
from urllib.parse import urljoin

# Global data storage
data_lock = Lock()
all_game_data = []

def create_driver():
    """Create a headless Chrome WebDriver instance"""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def download_media(url, save_dir, filename):
    """Download images and media files"""
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

def scroll_page_incrementally(driver, scroll_times=3, pause=2):
    """Scroll the page to load dynamic content"""
    for i in range(scroll_times):
        driver.execute_script("window.scrollBy(0, window.innerHeight);")
        time.sleep(pause)

def scrape_game_element(game_element, base_url):
    """Extract basic game data from a game card element"""
    game_data = {
        "title": "N/A",
        "release_date": "N/A",
        "platforms": "N/A",
        "rating": "N/A",
        "metacritic_score": "N/A",
        "added_by_users": "N/A",
        "url": "N/A",
        "image_url": "N/A"
    }
    
    try:
        # Extract game title and URL
        try:
            title_link = game_element.find_element(By.CSS_SELECTOR, "a[href*='/games/']")
            game_data["title"] = title_link.text.strip()
            game_data["url"] = urljoin(base_url, title_link.get_attribute('href'))
        except:
            pass
        
        # Extract release date
        try:
            date_elem = game_element.find_element(By.CSS_SELECTOR, "[class*='date'], .released")
            game_data["release_date"] = date_elem.text.strip()
        except:
            pass
        
        # Extract platforms
        try:
            platform_elems = game_element.find_elements(By.CSS_SELECTOR, "[class*='platform']")
            platforms = []
            for p in platform_elems:
                title = p.get_attribute('title')
                if title:
                    platforms.append(title)
            game_data["platforms"] = ', '.join(platforms) if platforms else "N/A"
        except:
            pass
        
        # Extract rating
        try:
            rating_elem = game_element.find_element(By.CSS_SELECTOR, "[class*='rating']")
            game_data["rating"] = rating_elem.text.strip()
        except:
            pass
        
        # Extract metacritic score
        try:
            meta_elem = game_element.find_element(By.CSS_SELECTOR, "[class*='metacritic'], [class*='metascore']")
            game_data["metacritic_score"] = meta_elem.text.strip()
        except:
            pass
        
        # Extract added by users count
        try:
            added_elem = game_element.find_element(By.XPATH, ".//*[contains(text(), 'added')]")
            game_data["added_by_users"] = added_elem.text.strip()
        except:
            pass
        
        # Extract image URL
        try:
            img_elem = game_element.find_element(By.CSS_SELECTOR, "img")
            game_data["image_url"] = img_elem.get_attribute('src')
        except:
            pass
            
    except Exception as e:
        pass
    
    return game_data if game_data["title"] != "N/A" else None

def scrape_game_details(driver, game_url, game_title, download_media_files=True):
    """Scrape detailed information from individual game page"""
    details = {
        "developer": "N/A",
        "publisher": "N/A",
        "genres": "N/A",
        "tags": "N/A",
        "esrb_rating": "N/A",
        "description": "N/A",
        "website": "N/A",
        "average_playtime": "N/A",
        "achievements_count": "N/A",
        "reddit_count": "N/A",
        "screenshots": "N/A",
        "downloaded_images": []
    }
    
    try:
        driver.get(game_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(2)
        
        # Extract developer
        try:
            dev_elem = driver.find_element(By.XPATH, "//div[contains(text(), 'Developer')]/..//a")
            details["developer"] = dev_elem.text.strip()
        except:
            pass
        
        # Extract publisher
        try:
            pub_elem = driver.find_element(By.XPATH, "//div[contains(text(), 'Publisher')]/..//a")
            details["publisher"] = pub_elem.text.strip()
        except:
            pass
        
        # Extract genres
        try:
            genre_elems = driver.find_elements(By.XPATH, "//div[contains(text(), 'Genre')]/..//a")
            genres = [g.text.strip() for g in genre_elems if g.text.strip()]
            details["genres"] = ', '.join(genres) if genres else "N/A"
        except:
            pass
        
        # Extract ESRB rating
        try:
            esrb_elem = driver.find_element(By.XPATH, "//div[contains(text(), 'ESRB') or contains(text(), 'Age rating')]/..//a")
            details["esrb_rating"] = esrb_elem.text.strip()
        except:
            pass
        
        # Extract tags
        try:
            tag_elems = driver.find_elements(By.CSS_SELECTOR, "a[href*='/tags/']")
            tags = [t.text.strip() for t in tag_elems if t.text.strip()][:10]
            details["tags"] = ', '.join(tags) if tags else "N/A"
        except:
            pass
        
        # Extract description
        try:
            desc_elem = driver.find_element(By.CSS_SELECTOR, "[class*='description'], [class*='game-description']")
            details["description"] = desc_elem.text.strip()[:500]
        except:
            pass
        
        # Extract website
        try:
            website_elem = driver.find_element(By.XPATH, "//a[contains(@href, 'http') and not(contains(@href, 'rawg.io'))]")
            details["website"] = website_elem.get_attribute('href')
        except:
            pass
        
        # Extract average playtime
        try:
            playtime_elem = driver.find_element(By.XPATH, "//*[contains(text(), 'playtime') or contains(text(), 'Playtime')]")
            details["average_playtime"] = playtime_elem.text.strip()
        except:
            pass
        
        # Extract achievements
        try:
            achievements_elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Achievements')]")
            details["achievements_count"] = achievements_elem.text.strip()
        except:
            pass
        
        # Extract Reddit mentions
        try:
            reddit_elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Reddit')]")
            details["reddit_count"] = reddit_elem.text.strip()
        except:
            pass
        
        # Download media files
        if download_media_files:
            safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50]
            script_dir = os.path.dirname(os.path.abspath(__file__))
            game_media_dir = os.path.join(script_dir, "scraped_data", "rawg_media", safe_title)
            os.makedirs(game_media_dir, exist_ok=True)
            
            # Download screenshots
            try:
                screenshot_urls = []
                screenshot_elems = driver.find_elements(By.CSS_SELECTOR, "img[class*='screenshot'], img[alt*='screenshot']")
                
                for idx, img in enumerate(screenshot_elems[:5]):
                    try:
                        img_url = img.get_attribute('src')
                        if img_url and 'screenshot' in img_url.lower():
                            screenshot_urls.append(img_url)
                            downloaded = download_media(img_url, game_media_dir, f"screenshot_{idx+1}.jpg")
                            if downloaded:
                                details["downloaded_images"].append(downloaded)
                    except:
                        continue
                
                details["screenshots"] = ", ".join(screenshot_urls) if screenshot_urls else "N/A"
            except:
                pass
        
    except Exception as e:
        print(f"   Error scraping details for {game_title}: {e}")
    
    return details

def scrape_page_range(worker_id, start_page, end_page, base_url, scrape_details=True, download_media_files=True):
    """Scrape a range of pages using a single driver instance"""
    driver = create_driver()
    local_data = []
    
    try:
        print(f"[Worker {worker_id}] Starting pages {start_page}-{end_page}")
        
        for page_num in range(start_page, end_page + 1):
            try:
                # Construct page URL
                if '?' in base_url:
                    page_url = f"{base_url}&page={page_num}"
                else:
                    page_url = f"{base_url}?page={page_num}"
                
                driver.get(page_url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
                time.sleep(2)
                
                # Scroll to load dynamic content
                scroll_page_incrementally(driver, scroll_times=3, pause=2)
                
                # Find game cards
                game_selectors = [
                    "div[class*='game-card']",
                    "div.game",
                    "article",
                    "div[class*='search-result']"
                ]
                
                games = []
                for selector in game_selectors:
                    try:
                        games = driver.find_elements(By.CSS_SELECTOR, selector)
                        if games and len(games) > 5:
                            break
                    except:
                        continue
                
                if not games:
                    print(f"[Worker {worker_id}] Page {page_num}: No games found")
                    continue
                
                # Process each game
                for game in games:
                    try:
                        game_data = scrape_game_element(game, "https://rawg.io")
                        
                        if game_data:
                            # Scrape detailed info if enabled
                            if scrape_details and game_data["url"] != "N/A":
                                print(f"[Worker {worker_id}] Scraping: {game_data['title']}")
                                details = scrape_game_details(driver, game_data["url"], game_data["title"], download_media_files)
                                game_data.update(details)
                            
                            local_data.append(game_data)
                    except StaleElementReferenceException:
                        continue
                    except Exception as e:
                        continue
                
                print(f"[Worker {worker_id}] Page {page_num}: {len(games)} games (Total: {len(local_data)})")
                time.sleep(2)  # Be respectful to the server
                
            except Exception as e:
                print(f"[Worker {worker_id}] Error on page {page_num}: {e}")
                continue
        
        print(f"[Worker {worker_id}] ✓ Done: {len(local_data)} games")
        
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal error: {e}")
    finally:
        driver.quit()
    
    # Add to global data with thread lock
    with data_lock:
        all_game_data.extend(local_data)
    
    return local_data

def scrape_rawg_games(max_games=100, num_workers=5, scrape_details=True, download_media_files=True, base_url="https://rawg.io/"):
    """
    Main scraping function using multithreading
    
    Args:
        max_games: Target number of games to scrape
        num_workers: Number of parallel threads
        scrape_details: Whether to scrape detailed info from individual pages
        download_media_files: Whether to download images
        base_url: Starting URL for scraping
    """
    global all_game_data
    all_game_data = []
    
    print(f"🚀 RAWG.io Scraper Starting")
    print(f"   Workers: {num_workers} | Target: {max_games} games")
    print(f"   Details: {'ON' if scrape_details else 'OFF'} | Media: {'ON' if download_media_files else 'OFF'}\n")
    
    start_time = time.time()
    
    # Calculate pages needed (assume ~40 games per page)
    games_per_page = 40
    total_pages_needed = (max_games + games_per_page - 1) // games_per_page
    pages_per_worker = max(1, total_pages_needed // num_workers)
    
    print(f"📄 Pages needed: {total_pages_needed} | Per worker: {pages_per_worker}\n")
    
    # Create thread pool and distribute work
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        
        for i in range(num_workers):
            start_page = (i * pages_per_worker) + 1
            end_page = start_page + pages_per_worker - 1
            
            # Last worker gets any remaining pages
            if i == num_workers - 1:
                end_page = total_pages_needed
            
            future = executor.submit(
                scrape_page_range, 
                i + 1, 
                start_page, 
                end_page, 
                base_url,
                scrape_details, 
                download_media_files
            )
            futures.append(future)
        
        # Wait for all workers to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Worker error: {e}")
    
    elapsed = time.time() - start_time
    
    # Save results to CSV
    if all_game_data:
        df = pd.DataFrame(all_game_data)
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        # Save to file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(script_dir, "scraped_data", f"rawg_games_{timestamp}.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # Print summary
        print(f"\n{'='*70}")
        print(f"✅ SCRAPING COMPLETE")
        print(f"   Total games: {len(df)}")
        print(f"   Duplicates removed: {duplicates_removed}")
        print(f"   Time: {elapsed:.1f}s")
        print(f"   Speed: {len(df)/elapsed:.2f} games/sec")
        print(f"💾 Saved: {output_file}")
        print(f"{'='*70}\n")
        
        # Display preview
        print("📋 Preview (first 10 games):")
        preview_cols = ['title', 'release_date', 'rating', 'platforms']
        available_cols = [col for col in preview_cols if col in df.columns]
        print(df[available_cols].head(10).to_string(index=False))
        
        # Print statistics
        if scrape_details:
            print(f"\n📊 Statistics:")
            
            if 'developer' in df.columns:
                dev_counts = df[df['developer'] != 'N/A']['developer'].value_counts()
                if not dev_counts.empty:
                    print(f"   Top developer: {dev_counts.index[0]} ({dev_counts.iloc[0]} games)")
            
            if 'genres' in df.columns:
                non_na_genres = df[df['genres'] != 'N/A']
                print(f"   Games with genre info: {len(non_na_genres)}")
            
            if 'platforms' in df.columns:
                non_na_platforms = df[df['platforms'] != 'N/A']
                print(f"   Games with platform info: {len(non_na_platforms)}")
            
            if 'metacritic_score' in df.columns:
                scored_games = df[df['metacritic_score'] != 'N/A']
                print(f"   Games with Metacritic score: {len(scored_games)}")
            
            if download_media_files and 'downloaded_images' in df.columns:
                total_images = sum(len(eval(x)) if isinstance(x, str) and x.startswith('[') else 0 for x in df['downloaded_images'])
                print(f"   Images downloaded: {total_images}")
    else:
        print("❌ No games scraped!")
    
    return all_game_data

if __name__ == "__main__":
    # CONFIGURATION OPTIONS
    
    # Option 1: Quick scrape (basic info only, no media)
    # scrape_rawg_games(max_games=100, num_workers=5, scrape_details=False, download_media_files=False)
    
    # Option 2: Detailed scrape with media (slower)
    scrape_rawg_games(
        max_games=500, 
        num_workers=8, 
        scrape_details=True, 
        download_media_files=True,
        base_url="https://rawg.io/"
    )
    
    # Option 3: High-volume scrape (for powerful machines)
    # scrape_rawg_games(max_games=1000, num_workers=10, scrape_details=True, download_media_files=True)
    
    # Option 4: Different starting URLs
    # scrape_rawg_games(max_games=200, num_workers=5, base_url="https://rawg.io/games?dates=2024-01-01,2024-12-31")
    # scrape_rawg_games(max_games=200, num_workers=5, base_url="https://rawg.io/games?ordering=-rating")