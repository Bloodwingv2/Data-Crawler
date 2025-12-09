

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time

def scrape_epic_games():
    # Setup Chrome driver
    options = Options()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Open Epic Games store page
    driver.get("https://store.epicgames.com/en-US/browse")
    
    try:
        # Handle cookie consent
        wait = WebDriverWait(driver, 10)
        cookie_button = wait.until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
        driver.execute_script("arguments[0].click();", cookie_button)
    except TimeoutException:
        print("Cookie consent button not found or not clickable.")

    game_data = []
    try:
        # Wait for the game list to be present
        wait = WebDriverWait(driver, 10)
        games = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[data-testid="offer-card-image-portrait"]')))
        
        for i in range(min(10, len(games))): # Scrape first 10 games
            game = games[i]
            try:
                title = game.get_attribute("aria-label")
                game_url = game.get_attribute("href")

                # The price is not directly available on the browse page, 
                # so we will navigate to the game page to get it.
                
                game_data.append({
                    "title": title,
                    "url": game_url,
                    "price": "N/A" # Placeholder
                })
            except Exception as e:
                print(f"Error scraping game: {e}")
    except TimeoutException:
        print("Timed out waiting for page to load")

    # Close the driver
    driver.quit()

    # Save to CSV
    df = pd.DataFrame(game_data)
    df.to_csv("epic_games.csv", index=False)
    print("Scraped 10 games and saved to epic_games.csv")

if __name__ == "__main__":
    scrape_epic_games()
