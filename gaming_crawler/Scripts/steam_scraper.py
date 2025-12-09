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

# Thread-safe lock for shared data
data_lock = Lock()
all_game_data = []

def create_driver():
    """Create a Chrome driver instance with optimal settings."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def scrape_game_element(game):
    """Extract data from a single game element."""
    try:
        # Title
        title = game.find_element(By.CSS_SELECTOR, ".title").text
        
        # Release date
        try:
            release_date = game.find_element(By.CSS_SELECTOR, ".search_released").text
        except NoSuchElementException:
            release_date = "N/A"
        
        # Price handling
        price = "N/A"
        discount_pct = "N/A"
        original_price = "N/A"
        
        try:
            discount_block = game.find_element(By.CSS_SELECTOR, ".discount_block")
            try:
                discount_pct = discount_block.find_element(By.CSS_SELECTOR, ".discount_pct").text.strip()
                original_price = discount_block.find_element(By.CSS_SELECTOR, ".discount_original_price").text.strip()
                price = discount_block.find_element(By.CSS_SELECTOR, ".discount_final_price").text.strip()
            except NoSuchElementException:
                try:
                    price_element = discount_block.find_element(By.CSS_SELECTOR, ".discount_final_price")
                    price = price_element.text.strip()
                except NoSuchElementException:
                    pass
        except NoSuchElementException:
            try:
                price_element = game.find_element(By.CSS_SELECTOR, ".search_price")
                price_text = price_element.text.strip()
                
                if not price_text or price_text == "":
                    price = "N/A"
                elif "Free" in price_text or "Free to Play" in price_text:
                    price = "Free"
                else:
                    price = price_text
            except NoSuchElementException:
                price = "N/A"
        
        # Clean up empty strings
        if price == "": price = "N/A"
        if original_price == "": original_price = "N/A"
        if discount_pct == "": discount_pct = "N/A"
        
        # Review summary
        try:
            review_summary_element = game.find_element(By.CSS_SELECTOR, ".search_review_summary")
            review_summary = review_summary_element.get_attribute("data-tooltip-html")
            if not review_summary:
                review_summary = "N/A"
        except NoSuchElementException:
            review_summary = "N/A"
        
        # Game URL
        try:
            game_url = game.get_attribute("href")
        except:
            game_url = "N/A"
        
        # Platforms
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
            "review_summary": review_summary,
            "url": game_url,
            "platforms": ", ".join(platforms) if platforms else "N/A"
        }
    except Exception as e:
        return None

def scrape_page_range(worker_id, start_page, end_page):
    """Scrape a range of Steam pages. Each page has ~25 games."""
    driver = create_driver()
    local_data = []
    
    try:
        total_pages = end_page - start_page + 1
        print(f"[Worker {worker_id}] Starting - Pages {start_page} to {end_page} ({total_pages} pages)")
        
        for page_num in range(start_page, end_page + 1):
            try:
                # Steam uses page parameter in URL
                url = f"https://store.steampowered.com/search/?filter=topsellers&page={page_num}"
                driver.get(url)
                
                # Wait for results to load
                wait = WebDriverWait(driver, 15)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#search_resultsRows > a")))
                time.sleep(2)
                
                # Get all games on this page
                games = driver.find_elements(By.CSS_SELECTOR, "#search_resultsRows > a")
                
                page_games = 0
                for game in games:
                    game_data = scrape_game_element(game)
                    if game_data:
                        local_data.append(game_data)
                        page_games += 1
                
                print(f"[Worker {worker_id}] Page {page_num}: Scraped {page_games} games (Total: {len(local_data)})")
                
                # Small delay between pages to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"[Worker {worker_id}] Error on page {page_num}: {e}")
                continue
        
        print(f"[Worker {worker_id}] ✓ Completed - Total scraped: {len(local_data)} games")
        
    except Exception as e:
        print(f"[Worker {worker_id}] ✗ Fatal error: {e}")
    finally:
        driver.quit()
    
    # Add to shared data with thread safety
    with data_lock:
        all_game_data.extend(local_data)
    
    return local_data

async def scrape_steam_games_async(total_games=1000, num_workers=10):
    """
    Scrape Steam games asynchronously using multiple worker threads.
    
    Args:
        total_games (int): Total number of games to scrape (default: 1000)
        num_workers (int): Number of concurrent workers (default: 10)
    """
    global all_game_data
    all_game_data = []
    
    print(f"🚀 Starting async scraper with {num_workers} workers")
    print(f"📊 Target: {total_games} games\n")
    
    start_time = time.time()
    
    # Steam shows approximately 25 games per page
    games_per_page = 25
    total_pages_needed = (total_games + games_per_page - 1) // games_per_page  # Ceiling division
    pages_per_worker = total_pages_needed // num_workers
    
    print(f"📄 Estimated pages needed: {total_pages_needed}")
    print(f"📋 Pages per worker: {pages_per_worker}\n")
    
    # Create tasks for each worker with page ranges
    loop = asyncio.get_event_loop()
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        tasks = []
        for i in range(num_workers):
            start_page = (i * pages_per_worker) + 1
            end_page = start_page + pages_per_worker - 1
            
            # Last worker takes any remaining pages
            if i == num_workers - 1:
                end_page = total_pages_needed
            
            task = loop.run_in_executor(
                executor,
                scrape_page_range,
                i + 1,
                start_page,
                end_page
            )
            tasks.append(task)
        
        # Wait for all workers to complete
        await asyncio.gather(*tasks)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Save results
    if all_game_data:
        # Remove duplicates based on URL (more reliable than title)
        df = pd.DataFrame(all_game_data)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        # Create output directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        scraped_data_dir = os.path.join(script_dir, "scraped_data")
        os.makedirs(scraped_data_dir, exist_ok=True)
        
        output_file = os.path.join(scraped_data_dir, "steam_games_new.csv")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n{'='*60}")
        print(f"✅ SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"📦 Total games scraped: {initial_count}")
        print(f"🔄 Duplicates removed: {duplicates_removed}")
        print(f"🎯 Unique games saved: {len(df)}")
        print(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
        print(f"⚡ Speed: {len(df) / elapsed_time:.2f} games/second")
        print(f"💾 Saved to: {output_file}")
        print(f"{'='*60}\n")
        
        # Display sample
        print("Sample of scraped data:")
        print(df[['title', 'price', 'discount_percentage']].head(15).to_string(index=False))
        
        # Price distribution
        print(f"\n📊 Price Distribution:")
        print(f"   Free games: {len(df[df['price'] == 'Free'])}")
        print(f"   Paid games: {len(df[df['price'] != 'Free'])}")
        print(f"   Games on discount: {len(df[df['discount_percentage'] != 'N/A'])}")
        
    else:
        print("❌ No games were scraped.")
    
    return all_game_data

def scrape_steam_games(max_games=1000, num_workers=10):
    """
    Synchronous wrapper for the async scraper.
    
    Args:
        max_games (int): Total number of games to scrape (default: 1000)
        num_workers (int): Number of concurrent workers (default: 10)
    """
    return asyncio.run(scrape_steam_games_async(max_games, num_workers))

if __name__ == "__main__":
    # Scrape 1000 games using 10 workers
    scrape_steam_games(max_games=1000, num_workers=10)
    
    # Other examples:
    # scrape_steam_games(max_games=500, num_workers=5)   # 500 games, 5 workers
    # scrape_steam_games(max_games=100, num_workers=5)   # 100 games, 5 workers
    # scrape_steam_games(max_games=2000, num_workers=10) # 2000 games, 10 workers