import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
import time
import re

def scrape_steam_games():
    # Setup Chrome driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    # Open Steam search page
    driver.get("https://store.steampowered.com/search/?filter=topsellers")
    time.sleep(5)  # Wait for page to load

    # Get game list
    games = driver.find_elements(By.CSS_SELECTOR, "#search_resultsRows > a")
    
    game_data = []
    for i in range(min(10, len(games))): # Scrape first 10 games
        game = games[i]
        try:
            title = game.find_element(By.CSS_SELECTOR, ".title").text
            release_date = game.find_element(By.CSS_SELECTOR, ".search_released").text
            
            price = "N/A"
            discount_pct = "N/A"
            try:
                price_element = game.find_element(By.CSS_SELECTOR, ".search_price")
                price_text = price_element.text.strip()
                if price_text:
                    if '\n' in price_text:
                        prices = price_text.split('\n')
                        original_price = prices[0]
                        discounted_price = prices[1]
                        price = f"{original_price} -> {discounted_price}"
                    else:
                        price = price_text
            except NoSuchElementException:
                price = "N/A"

            try:
                discount_pct_element = game.find_element(By.CSS_SELECTOR, ".search_discount span")
                discount_pct = discount_pct_element.text
            except NoSuchElementException:
                discount_pct = "N/A"

            try:
                review_summary_element = game.find_element(By.CSS_SELECTOR, ".search_review_summary")
                review_summary = review_summary_element.get_attribute("data-tooltip-html") if review_summary_element else "N/A"
            except NoSuchElementException:
                review_summary = "N/A"

            try:
                game_url = game.get_attribute("href")
            except NoSuchElementException:
                game_url = "N/A"

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
                "price": price,
                "review_summary": review_summary,
                "url": game_url,
                "discount_percentage": discount_pct,
                "platforms": ", ".join(platforms)
            })
        except Exception as e:
            print(f"Error scraping game: {e}")

    # Close the driver
    driver.quit()

    # Save to CSV
    df = pd.DataFrame(game_data)
    df.to_csv("steam_games_new.csv", index=False)
    print("Scraped 10 games and saved to steam_games_new.csv")

if __name__ == "__main__":
    scrape_steam_games()
