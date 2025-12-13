import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import requests
import re
from threading import Lock

data_lock = Lock()
all_game_data = []

def download_media(url, save_dir, filename):
    """Download images from URLs."""
    if not url or url == "N/A" or not url.startswith('http'): 
        return None
        
    try:
        response = requests.get(url, timeout=15, stream=True, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        if response.status_code == 200:
            filepath = os.path.join(save_dir, filename)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"      ✓ Downloaded: {filename}")
            return filepath
        else:
            print(f"      ✗ Failed ({response.status_code}): {filename}")
    except Exception as e:
        print(f"      ✗ Error: {str(e)[:40]}")
    return None

def scrape_game_details(driver, game_url, game_title, download_media_files=True):
    """Scrape game details from Instant Gaming with EXACT selectors."""
    details = {
        "title": game_title,
        "url": game_url,
        "developer": "N/A",
        "publisher": "N/A",
        "platforms": "N/A",
        "genre": "N/A",
        "release_date": "N/A",
        "languages": "N/A",
        "description": "N/A",
        "video_url": "N/A",
        "header_image": "N/A",
        "screenshots": [],
        "downloaded_images": [],
    }
    
    try:
        print(f"\n{'='*70}")
        print(f"SCRAPING: {game_title}")
        print(f"{'='*70}")
        
        driver.get(game_url)
        time.sleep(3)
        
        # Create media directory
        safe_title = re.sub(r'[<>:"/\\|?*]', '', game_title)[:50].strip()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        game_media_dir = os.path.join(script_dir, "scraped_data", "instant_gaming_media", safe_title)
        os.makedirs(game_media_dir, exist_ok=True)
        
        # --- DEVELOPER (From meta tag: itemprop="author") ---
        print("\n[1] DEVELOPER:")
        try:
            dev_meta = driver.find_element(By.CSS_SELECTOR, 'meta[itemprop="author"]')
            details["developer"] = dev_meta.get_attribute("content")
            print(f"   ✓ Found: {details['developer']}")
        except:
            # Fallback: Look in table with th "Entwickler:" or "Developer:"
            try:
                dev_row = driver.find_element(By.XPATH, "//th[contains(text(), 'Developer') or contains(text(), 'Entwickler')]/following-sibling::th")
                dev_link = dev_row.find_element(By.TAG_NAME, "a")
                details["developer"] = dev_link.text.strip()
                print(f"   ✓ Found (table): {details['developer']}")
            except:
                print("   ✗ NOT FOUND")
        
        # --- PUBLISHER (From meta tag: itemprop="publisher") ---
        print("\n[2] PUBLISHER:")
        try:
            pub_meta = driver.find_element(By.CSS_SELECTOR, 'meta[itemprop="publisher"]')
            details["publisher"] = pub_meta.get_attribute("content")
            print(f"   ✓ Found: {details['publisher']}")
        except:
            # Fallback: Look in table with th "Herausgeber:" or "Editor:"
            try:
                pub_row = driver.find_element(By.XPATH, "//th[contains(text(), 'Editor') or contains(text(), 'Herausgeber') or contains(text(), 'Publisher')]/following-sibling::th")
                pub_link = pub_row.find_element(By.TAG_NAME, "a")
                details["publisher"] = pub_link.text.strip()
                print(f"   ✓ Found (table): {details['publisher']}")
            except:
                print("   ✗ NOT FOUND")
        
        # --- GENRE (From table row with class="genres") ---
        print("\n[3] GENRE:")
        try:
            genre_row = driver.find_element(By.CSS_SELECTOR, "tr.genres th:nth-child(2)")
            genre_link = genre_row.find_element(By.TAG_NAME, "a")
            details["genre"] = genre_link.text.strip()
            print(f"   ✓ Found: {details['genre']}")
        except:
            print("   ✗ NOT FOUND")
        
        # --- RELEASE DATE (From table row with class="release-date") ---
        print("\n[4] RELEASE DATE:")
        try:
            date_row = driver.find_element(By.CSS_SELECTOR, "tr.release-date th:nth-child(2)")
            details["release_date"] = date_row.text.strip()
            print(f"   ✓ Found: {details['release_date']}")
        except:
            print("   ✗ NOT FOUND")
        
        # --- PLATFORMS (From meta tag: data-platform) ---
        print("\n[5] PLATFORMS:")
        try:
            platform_meta = driver.find_element(By.CSS_SELECTOR, 'meta[itemprop="category"]')
            platform = platform_meta.get_attribute("data-platform")
            details["platforms"] = platform if platform else "N/A"
            print(f"   ✓ Found: {details['platforms']}")
        except:
            # Fallback: check platform container
            try:
                platform_elem = driver.find_element(By.CSS_SELECTOR, ".platform-container span")
                details["platforms"] = platform_elem.text.strip()
                print(f"   ✓ Found (span): {details['platforms']}")
            except:
                print("   ✗ NOT FOUND")
        
        # --- DESCRIPTION (From span[itemprop='description']) ---
        print("\n[6] DESCRIPTION:")
        try:
            desc_elem = driver.find_element(By.CSS_SELECTOR, "span[itemprop='description']")
            desc = desc_elem.text.strip()
            details["description"] = desc[:500] if desc else "N/A"
            print(f"   ✓ Found: {len(desc)} chars")
        except:
            print("   ✗ NOT FOUND")
        
        # --- HEADER IMAGE (From meta itemprop="image") ---
        print("\n[7] HEADER/COVER IMAGE:")
        try:
            img_meta = driver.find_element(By.CSS_SELECTOR, 'meta[itemprop="image"]')
            img_url = img_meta.get_attribute("content")
            details["header_image"] = img_url
            print(f"   ✓ Found: {img_url[:60]}...")
            
            if download_media_files:
                download_media(img_url, game_media_dir, "cover.jpg")
        except:
            print("   ✗ NOT FOUND")
        
        # --- VIDEO (From iframe with id="ig-vimeo-player") ---
        print("\n[8] VIDEO:")
        try:
            video_iframe = driver.find_element(By.CSS_SELECTOR, "#ig-vimeo-player")
            video_src = video_iframe.get_attribute("src")
            details["video_url"] = video_src
            print(f"   ✓ Found: {video_src[:60]}...")
        except:
            print("   ✗ NOT FOUND")
        
        # --- SCREENSHOTS (From .screenshots a[itemprop='screenshot']) ---
        print("\n[9] SCREENSHOTS:")
        screenshot_urls = []
        try:
            # Find all screenshot links
            screenshot_links = driver.find_elements(By.CSS_SELECTOR, ".screenshots a[itemprop='screenshot']")
            
            if screenshot_links:
                print(f"   Found {len(screenshot_links)} screenshots")
                
                for idx, link in enumerate(screenshot_links[:10]):  # Limit to 10
                    try:
                        # Get high-res URL from href attribute
                        href = link.get_attribute("href")
                        
                        if href and any(ext in href.lower() for ext in ['.jpg', '.png', '.jpeg']):
                            screenshot_urls.append(href)
                            print(f"      [{idx+1}] {href[:50]}...")
                            
                            if download_media_files:
                                ext = "jpg"
                                if ".png" in href.lower():
                                    ext = "png"
                                
                                downloaded = download_media(
                                    href,
                                    game_media_dir,
                                    f"screenshot_{idx+1}.{ext}"
                                )
                                if downloaded:
                                    details["downloaded_images"].append(downloaded)
                    except Exception as e:
                        print(f"      ✗ Screenshot {idx+1} error: {e}")
                        continue
        except Exception as e:
            print(f"   ✗ Error: {e}")
        
        if screenshot_urls:
            details["screenshots"] = screenshot_urls
            print(f"   ✓ Total: {len(screenshot_urls)} screenshots")
        else:
            print("   ✗ NOT FOUND")
        
        # Save debug HTML if needed
        if details["developer"] == "N/A" or details["publisher"] == "N/A":
            debug_file = os.path.join(game_media_dir, "page_source.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print(f"\n⚠ Debug HTML saved: {debug_file}")
        
        # Print summary
        print(f"\n{'='*70}")
        print(f"SUMMARY: {game_title}")
        print(f"  Developer: {details['developer']}")
        print(f"  Publisher: {details['publisher']}")
        print(f"  Genre: {details['genre']}")
        print(f"  Release: {details['release_date']}")
        print(f"  Platform: {details['platforms']}")
        print(f"  Images: {len(details['downloaded_images'])} downloaded")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"\n✗✗✗ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    return details

def scrape_products_from_page(driver, base_url, max_games):
    """Scrape game listings from Instant Gaming homepage."""
    print(f"\n{'#'*70}")
    print(f"LOADING INSTANT GAMING HOMEPAGE")
    print(f"{'#'*70}\n")
    
    driver.get(base_url)
    time.sleep(5)
    
    # Scroll to load more games
    print("Scrolling to load games...")
    for i in range(5):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        print(f"  Scroll {i+1}/5")
    
    print("\nExtracting game links...")
    
    game_links = []
    
    try:
        # Find all game items
        game_items = driver.find_elements(By.CSS_SELECTOR, ".listing-items article.item")
        
        print(f"  Found {len(game_items)} game items")
        
        for item in game_items[:max_games]:
            try:
                # Get the link
                link = item.find_element(By.CSS_SELECTOR, "a.cover")
                href = link.get_attribute("href")
                
                # Get the title
                try:
                    title_elem = item.find_element(By.CSS_SELECTOR, ".name .title")
                    title = title_elem.get_attribute("title") or title_elem.text.strip()
                except:
                    title = "Unknown"
                
                if href and "instant-gaming.com" in href:
                    game_links.append({"url": href, "title": title})
                    
            except Exception as e:
                print(f"  ✗ Error extracting item: {e}")
                continue
    
    except Exception as e:
        print(f"✗ Error finding game items: {e}")
    
    # Remove duplicates
    unique_games = []
    seen_urls = set()
    for game in game_links:
        if game["url"] not in seen_urls:
            seen_urls.add(game["url"])
            unique_games.append(game)
    
    print(f"\n✓ Found {len(unique_games)} unique games")
    return unique_games[:max_games]

def create_driver(headless):
    """Create Chrome driver."""
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def run_scraper(base_url, max_games, download_media, headless):
    """
    Main scraper function.
    
    Args:
        base_url (str): URL of the Instant Gaming page to scrape
        max_games (int): Maximum number of games to scrape
        download_media (bool): Whether to download images and media
        headless (bool): Whether to run browser in headless mode
    
    Returns:
        pd.DataFrame: DataFrame containing scraped game data
    """
    print("\n" + "="*70)
    print("INSTANT GAMING SCRAPER")
    print("="*70 + "\n")
    print(f"Configuration:")
    print(f"  Base URL: {base_url}")
    print(f"  Max Games: {max_games}")
    print(f"  Download Media: {download_media}")
    print(f"  Headless Mode: {headless}")
    print("="*70 + "\n")
    
    # Create driver
    driver = create_driver(headless=headless)
    
    try:
        # Step 1: Get game list from homepage
        games = scrape_products_from_page(driver, base_url, max_games)
        
        if not games:
            print("✗ No games found! Check selectors.")
            return None
        
        # Step 2: Scrape each game
        all_results = []
        
        for idx, game in enumerate(games, 1):
            print(f"\n\n[{idx}/{len(games)}] Processing: {game['title']}")
            
            details = scrape_game_details(
                driver,
                game['url'],
                game['title'],
                download_media_files=download_media
            )
            
            all_results.append(details)
            
            # Small delay between requests
            time.sleep(2)
        
        # Final save
        df = pd.DataFrame(all_results)
        df.to_csv("scraped_data/instant_gaming_data.csv", index=False, encoding='utf-8')
        
        print("\n" + "="*70)
        print("SCRAPING COMPLETE!")
        print(f"Total games scraped: {len(all_results)}")
        print(f"Results saved to: scraped_data/instant_gaming_data.csv")
        print("="*70)
        
        return df
    
    except Exception as e:
        print(f"\n✗✗✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        driver.quit()
        print("\nBrowser closed.")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Configuration variables
    INSTANT_GAMING_URL = "https://www.instant-gaming.com/de/"
    MAX_GAMES = 20
    DOWNLOAD_MEDIA = True
    HEADLESS_MODE = True
    
    # Run the scraper
    results_df = run_scraper(
        base_url=INSTANT_GAMING_URL,
        max_games=MAX_GAMES,
        download_media=DOWNLOAD_MEDIA,
        headless=HEADLESS_MODE
    )