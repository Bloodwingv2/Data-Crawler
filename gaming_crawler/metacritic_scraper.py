
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

def scrape_metacritic_games():
    # Setup Chrome driver
    options = Options()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Open Metacritic game page
    driver.get("https://www.metacritic.com/browse/game/")
    
    try:
        # Handle cookie consent
        wait = WebDriverWait(driver, 10)
        cookie_button = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        cookie_button.click()
    except TimeoutException:
        print("Cookie consent button not found or not clickable.")

    game_data = []
    try:
        # Wait for the game list to be present
        wait = WebDriverWait(driver, 10)
        games = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".c-finderProductCard")))
        
        for i in range(min(10, len(games))): # Scrape first 10 games
            game = games[i]
            try:
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
                
                game_data.append({
                    "title": title,
                    "critic_score": critic_score,
                    "user_score": user_score,
                    "platform": platform,
                    "release_date": release_date,
                    "url": game_url
                })
            except Exception as e:
                print(f"Error scraping game: {e}")
    except TimeoutException:
        print("Timed out waiting for page to load")

    # Close the driver
    driver.quit()

    # Save to CSV
    df = pd.DataFrame(game_data)
    df.to_csv("scraped_data/metacritic_games.csv", index=False)
    print("Scraped 10 games and saved to metacritic_games.csv")

if __name__ == "__main__":
    scrape_metacritic_games()
