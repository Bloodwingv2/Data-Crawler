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
import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import re

# Thread-safe lock for shared data
data_lock = Lock()
all_game_data = []

def create_driver():
    """Create a Chrome driver instance with optimal settings."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def scrape_game_element(game, driver):
    """Extract data from a single game element, including media from the game's page."""
    try:
        # --- Basic data from list page ---
        title = game.find_element(By.CSS_SELECTOR, "a.c-finderProductCard_container").get_attribute('aria-label')
        
        try:
            critic_score_element = game.find_element(By.CSS_SELECTOR, ".c-siteReviewScore_background-positive span")
            critic_score = critic_score_element.text
        except NoSuchElementException:
            critic_score = "N/A"

        try:
            user_score_element = game.find_element(By.CSS_SELECTOR, ".c-siteReviewScore_background-user span")
            user_score = user_score_element.text
        except NoSuchElementException:
            user_score = "N/A"

        try:
            platform = game.find_element(By.CSS_SELECTOR, ".c-finderProductCard_platform").text
        except NoSuchElementException:
            platform = "N/A"

        try:
            release_date = game.find_element(By.CSS_SELECTOR, ".c-finderProductCard_releaseDate span:nth-child(2)").text
        except NoSuchElementException:
            release_date = "N/A"

        try:
            game_url = game.find_element(By.CSS_SELECTOR, "a.c-finderProductCard_container").get_attribute("href")
        except NoSuchElementException:
            game_url = "N/A"

        # --- Media scraping from game page ---
        images = []
        videos = []
        
        if game_url and game_url.startswith("https://www.metacritic.com/game/"):
            original_window = driver.current_window_handle
            
            try:
                driver.execute_script("window.open(arguments[0], '_blank');", game_url)
                driver.switch_to.window(driver.window_handles[1])

                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.c-productionDetails_screenshots, div.c-videoPlayer")))

                # Scrape images
                try:
                    screenshot_elements = driver.find_elements(By.CSS_SELECTOR, ".c-imageGallery_thumbnails picture > img, .c-imageGallery_image picture > img")
                    for elem in screenshot_elements:
                        if img_url := elem.get_attribute("src"):
                            # Metacritic uses responsive images, get a high-quality version
                            img_url = re.sub(r'\/fit-in\/\d+x\d+\/', '/fit-in/1920x1080/', img_url)
                            images.append(img_url)
                except NoSuchElementException:
                    pass

                # Scrape videos
                try:
                    video_player = driver.find_element(By.CSS_SELECTOR, "div[data-mcvideourl]")
                    if video_url := video_player.get_attribute("data-mcvideourl"):
                        videos.append(video_url)
                except NoSuchElementException:
                    pass

            except TimeoutException:
                print(f"[Media Scraper] Timeout loading media page for: {title}")
            except Exception as e:
                print(f"[Media Scraper] Error scraping media for {title}: {e}")
            finally:
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(original_window)
        
        return {
            "title": title,
            "critic_score": critic_score,
            "user_score": user_score,
            "platform": platform,
            "release_date": release_date,
            "url": game_url,
            "images": ", ".join(list(set(images))) if images else "N/A",
            "videos": ", ".join(list(set(videos))) if videos else "N/A",
        }

    except Exception as e:
        print(f"Error scraping game element: {e}")
        return None

def scrape_page_range(worker_id, start_page, end_page):
    """Scrape a range of Metacritic pages."""
    driver = create_driver()
    local_data = []
    
    try:
        total_pages = end_page - start_page + 1
        print(f"[Worker {worker_id}] Starting - Pages {start_page} to {end_page} ({total_pages} pages)")
        
        for page_num in range(start_page, end_page + 1):
            try:
                # Metacritic uses a 'page' query parameter
                url = f"https://www.metacritic.com/browse/game/?page={page_num}"
                driver.get(url)

                # Handle cookie consent if it appears
                try:
                    cookie_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                    cookie_button.click()
                except TimeoutException:
                    pass # Cookie button not found, continue

                wait = WebDriverWait(driver, 15)
                games = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".c-finderProductCard")))
                
                page_games = 0
                for game in games:
                    game_data = scrape_game_element(game, driver)
                    if game_data:
                        local_data.append(game_data)
                        page_games += 1
                
                print(f"[Worker {worker_id}] Page {page_num}: Scraped {page_games} games (Total for worker: {len(local_data)})")
                
                if not games:
                    print(f"[Worker {worker_id}] No games found on page {page_num}. Ending worker.")
                    break
                
                time.sleep(2) # Be respectful to Metacritic's servers
                
            except Exception as e:
                print(f"[Worker {worker_id}] Error on page {page_num}: {e}")
                continue
        
        print(f"[Worker {worker_id}] ✓ Completed - Total scraped by worker: {len(local_data)} games")
        
    except Exception as e:
        print(f"[Worker {worker_id}] ✗ Fatal error: {e}")
    finally:
        driver.quit()
    
    with data_lock:
        all_game_data.extend(local_data)
    
    return local_data

async def scrape_metacritic_async(total_games=100, num_workers=4):
    """Scrape Metacritic games asynchronously, including media."""
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Starting async Metacritic scraper with {num_workers} workers")
    print(f"📊 Target: {total_games} games")
    
    start_time = time.time()
    
    # Metacritic shows 20 games per page
    games_per_page = 20
    total_pages_needed = (total_games + games_per_page - 1) // games_per_page
    pages_per_worker = max(1, total_pages_needed // num_workers)
    
    print(f"📄 Estimated pages needed: {total_pages_needed}")
    print(f"📋 Pages per worker: {pages_per_worker}\n")
    
    loop = asyncio.get_event_loop()
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        tasks = []
        page_chunks = [range(i, min(i + pages_per_worker, total_pages_needed + 1)) 
                       for i in range(1, total_pages_needed + 1, pages_per_worker)]

        for i, page_range in enumerate(page_chunks):
            if not page_range: continue
            start_page = page_range.start
            end_page = page_range.stop - 1

            task = loop.run_in_executor(
                executor,
                scrape_page_range,
                i + 1,
                start_page,
                end_page
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    if all_game_data:
        df = pd.DataFrame(all_game_data)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first').reset_index(drop=True)
        duplicates_removed = initial_count - len(df)
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        scraped_data_dir = os.path.join(script_dir, "scraped_data")
        os.makedirs(scraped_data_dir, exist_ok=True)
        
        output_file = os.path.join(scraped_data_dir, "metacritic_games_media.csv")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ METACRITIC SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"📦 Total games processed: {initial_count}")
        print(f"🔄 Duplicates removed: {duplicates_removed}")
        print(f"🎯 Unique games saved: {len(df)}")
        print(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
        if elapsed_time > 0:
            print(f"⚡ Speed: {len(df) / elapsed_time:.2f} games/second")
        print(f"💾 Saved to: {output_file}")
        print(f"{'='*60}\n")
        
        print("Sample of scraped data:")
        print(df[['title', 'critic_score', 'user_score', 'images', 'videos']].head(10).to_string(index=False))
        
    else:
        print("❌ No games were scraped.")
    
    return all_game_data

def scrape_metacritic_games(max_games=100, num_workers=4):
    """Synchronous wrapper for the async media scraper."""
    return asyncio.run(scrape_metacritic_async(max_games, num_workers))

if __name__ == "__main__":
    # Scrape 40 games using 4 workers as an example.
    # NOTE: Scraping with media is significantly slower per game.
    scrape_metacritic_games(max_games=500, num_workers=4)