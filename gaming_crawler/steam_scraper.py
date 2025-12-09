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

def scrape_steam_games(max_games=10):
    """
    Scrape Steam top sellers with configurable number of games.
    
    Args:
        max_games (int): Maximum number of games to scrape. Default is 10.
    """
    # Setup Chrome driver with options
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run browser in background (no window)
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Open Steam search page
        print("Loading Steam top sellers page...")
        driver.get("https://store.steampowered.com/search/?filter=topsellers")
        
        # Wait for search results to load
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#search_resultsRows > a")))
        time.sleep(3)  # Additional wait for dynamic content

        # Scroll until we have enough games loaded
        games_loaded = 0
        scroll_attempts = 0
        max_scroll_attempts = 10
        
        while games_loaded < max_games and scroll_attempts < max_scroll_attempts:
            games = driver.find_elements(By.CSS_SELECTOR, "#search_resultsRows > a")
            games_loaded = len(games)
            
            if games_loaded >= max_games:
                break
                
            # Scroll down
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            scroll_attempts += 1
        
        print(f"Loaded {games_loaded} games on page")
        
        # Get game list (limit to max_games)
        games = driver.find_elements(By.CSS_SELECTOR, "#search_resultsRows > a")[:max_games]
        
        game_data = []
        for idx, game in enumerate(games, 1):
            try:
                print(f"Scraping game {idx}/{len(games)}...")
                
                # Title
                title = game.find_element(By.CSS_SELECTOR, ".title").text
                
                # Release date
                try:
                    release_date = game.find_element(By.CSS_SELECTOR, ".search_released").text
                except NoSuchElementException:
                    release_date = "N/A"
                
                # Price handling - improved logic
                price = "N/A"
                discount_pct = "N/A"
                original_price = "N/A"
                
                try:
                    # Check for discount block first
                    discount_block = game.find_element(By.CSS_SELECTOR, ".discount_block")
                    
                    # Check if there's an active discount
                    try:
                        discount_pct = discount_block.find_element(By.CSS_SELECTOR, ".discount_pct").text.strip()
                        original_price = discount_block.find_element(By.CSS_SELECTOR, ".discount_original_price").text.strip()
                        price = discount_block.find_element(By.CSS_SELECTOR, ".discount_final_price").text.strip()
                    except NoSuchElementException:
                        # Discount block exists but no discount (regular price)
                        try:
                            price_element = discount_block.find_element(By.CSS_SELECTOR, ".discount_final_price")
                            price = price_element.text.strip()
                        except NoSuchElementException:
                            pass
                            
                except NoSuchElementException:
                    # No discount block, check for regular price
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
                
                # Clean up price fields
                if price == "":
                    price = "N/A"
                if original_price == "":
                    original_price = "N/A"
                if discount_pct == "":
                    discount_pct = "N/A"
                
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
                
                game_data.append({
                    "title": title,
                    "release_date": release_date,
                    "original_price": original_price,
                    "price": price,
                    "discount_percentage": discount_pct,
                    "review_summary": review_summary,
                    "url": game_url,
                    "platforms": ", ".join(platforms) if platforms else "N/A"
                })
                
            except Exception as e:
                print(f"Error scraping game {idx}: {e}")
                continue

    finally:
        # Close the driver
        driver.quit()

    # Save to CSV
    if game_data:
        # Create directory if it doesn't exist
        os.makedirs("scraped_data", exist_ok=True)
        
        df = pd.DataFrame(game_data)
        output_file = "scraped_data/steam_games.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ Successfully scraped {len(game_data)} games and saved to {output_file}")
        
        # Display summary
        print("\nSummary:")
        print(df[['title', 'price', 'discount_percentage']].to_string(index=False))
    else:
        print("No games were scraped.")
    
    return game_data

if __name__ == "__main__":
    # Default: scrape 10 games
    scrape_steam_games(max_games=10)
    
    # To scrape a different number, change the parameter:
    # scrape_steam_games(max_games=25)  # For 25 games
    # scrape_steam_games(max_games=50)  # For 50 games