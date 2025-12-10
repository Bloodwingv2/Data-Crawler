"""
Metacritic Game Scraper with Selenium
Extracts all game entries from Metacritic game pages and outputs to CSV
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import csv
import time
from typing import List, Dict
import re


class MetacriticGameScraper:
    def __init__(self, headless: bool = True):
        """Initialize the scraper with Selenium WebDriver"""
        self.base_url = "https://www.metacritic.com"
        self.driver = self._setup_driver(headless)
        
    def _setup_driver(self, headless: bool):
        """Setup Chrome WebDriver with options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    
    def wait_for_page_load(self, timeout: int = 10):
        """Wait for page to fully load"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            time.sleep(2)  # Additional wait for dynamic content
        except TimeoutException:
            print("Page load timeout")
    
    def extract_game_from_element(self, element, section: str = '') -> Dict:
        """Extract game information from a web element"""
        game_data = {'section': section}
        
        try:
            # Extract title
            try:
                title_elem = element.find_element(By.CSS_SELECTOR, 'a[href*="/game/"]')
                game_data['title'] = title_elem.text.strip()
                game_data['url'] = title_elem.get_attribute('href')
            except NoSuchElementException:
                return None
            
            # Extract score
            try:
                # Look for score in various possible locations
                score_text = element.text
                score_match = re.search(r'\b(\d{1,3})\b', score_text)
                if score_match:
                    score = int(score_match.group(1))
                    if 0 <= score <= 100:  # Valid Metacritic score range
                        game_data['score'] = score
                else:
                    game_data['score'] = 'TBD'
            except:
                game_data['score'] = 'TBD'
            
            # Extract rating category
            rating_keywords = [
                'Universal Acclaim', 'Generally Favorable', 
                'Mixed or Average', 'Generally Unfavorable', 
                'Overwhelming Dislike'
            ]
            for rating in rating_keywords:
                if rating.lower() in element.text.lower():
                    game_data['rating'] = rating
                    break
            
            if 'rating' not in game_data:
                game_data['rating'] = 'N/A'
            
            # Extract image URL
            try:
                img_elem = element.find_element(By.TAG_NAME, 'img')
                game_data['image_url'] = img_elem.get_attribute('src')
            except NoSuchElementException:
                game_data['image_url'] = ''
            
            # Extract platform if available
            try:
                platform_elem = element.find_element(By.CSS_SELECTOR, '[class*="platform"]')
                game_data['platform'] = platform_elem.text.strip()
            except NoSuchElementException:
                game_data['platform'] = 'Multiple/Unknown'
            
            return game_data if game_data.get('title') else None
            
        except Exception as e:
            print(f"Error extracting game data: {e}")
            return None
    
    def scrape_main_page(self) -> List[Dict]:
        """Scrape the main /game/ page"""
        print("Navigating to Metacritic game page...")
        self.driver.get(f"{self.base_url}/game/")
        self.wait_for_page_load()
        
        all_games = []
        
        # Define sections to scrape
        sections = {
            'New Releases': 'new_releases',
            'Upcoming Games': 'upcoming_games',
            'Best Games': 'best_games',
            'New on PlayStation Plus': 'playstation_plus',
            'New on Xbox Game Pass': 'xbox_game_pass'
        }
        
        # Scroll to load more content
        self.scroll_page()
        
        # Find all game links
        try:
            game_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/game/"]')
            print(f"Found {len(game_links)} game links")
            
            # Get current section context
            current_section = 'general'
            processed_urls = set()
            
            for link in game_links:
                try:
                    url = link.get_attribute('href')
                    
                    # Skip if already processed
                    if url in processed_urls:
                        continue
                    
                    # Get parent container for full context
                    parent = link.find_element(By.XPATH, './ancestor::*[position()<=3]')
                    
                    # Determine section from nearby headers
                    try:
                        page_text = self.driver.find_element(By.TAG_NAME, 'body').text
                        link_position = page_text.find(link.text)
                        
                        for section_name, section_key in sections.items():
                            section_position = page_text.find(section_name)
                            if section_position != -1 and section_position < link_position:
                                current_section = section_key
                    except:
                        pass
                    
                    game_data = self.extract_game_from_element(parent, current_section)
                    
                    if game_data:
                        all_games.append(game_data)
                        processed_urls.add(url)
                        
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"Error finding game links: {e}")
        
        return all_games
    
    def scrape_browse_page(self, platform: str = 'all', 
                          sort: str = 'metascore',
                          limit: int = 100) -> List[Dict]:
        """Scrape the browse page for a specific platform"""
        url = f"{self.base_url}/browse/game/{platform}/all/all-time/{sort}/"
        print(f"Navigating to browse page: {url}")
        
        self.driver.get(url)
        self.wait_for_page_load()
        
        games = []
        
        # Scroll to load more games
        for _ in range(5):  # Scroll multiple times to load more content
            self.scroll_page()
            time.sleep(1)
        
        # Find game elements
        try:
            game_elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/game/"]')
            print(f"Found {len(game_elements)} game elements")
            
            processed_urls = set()
            
            for elem in game_elements[:limit]:
                try:
                    url = elem.get_attribute('href')
                    
                    if url in processed_urls:
                        continue
                    
                    parent = elem.find_element(By.XPATH, './ancestor::*[position()<=3]')
                    game_data = self.extract_game_from_element(parent, f'browse_{platform}')
                    
                    if game_data:
                        games.append(game_data)
                        processed_urls.add(url)
                        
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"Error scraping browse page: {e}")
        
        return games
    
    def scroll_page(self):
        """Scroll page to load dynamic content"""
        try:
            # Scroll down in increments
            scroll_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            scroll_increment = 500
            
            while current_position < scroll_height:
                current_position += scroll_increment
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(0.3)
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error scrolling: {e}")
    
    def save_to_csv(self, games: List[Dict], filename: str = 'metacritic_games.csv'):
        """Save games to CSV file"""
        if not games:
            print("No games to save!")
            return
        
        # Get all unique keys
        all_keys = set()
        for game in games:
            all_keys.update(game.keys())
        
        fieldnames = sorted(all_keys)
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(games)
        
        print(f"\n✓ Successfully saved {len(games)} games to {filename}")
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Main execution"""
    print("=" * 60)
    print("Metacritic Game Scraper with Selenium")
    print("=" * 60)
    
    # Use context manager to ensure driver closes
    with MetacriticGameScraper(headless=True) as scraper:
        
        # Scrape main page
        print("\n[1/3] Scraping main game page...")
        main_page_games = scraper.scrape_main_page()
        print(f"Found {len(main_page_games)} games from main page")
        
        # Scrape PS5 browse page
        print("\n[2/3] Scraping PS5 browse page...")
        ps5_games = scraper.scrape_browse_page(platform='ps5', limit=50)
        print(f"Found {len(ps5_games)} PS5 games")
        
        # Scrape PC browse page
        print("\n[3/3] Scraping PC browse page...")
        pc_games = scraper.scrape_browse_page(platform='pc', limit=50)
        print(f"Found {len(pc_games)} PC games")
        
        # Combine all games and remove duplicates
        all_games = main_page_games + ps5_games + pc_games
        
        # Remove duplicates based on URL
        unique_games = []
        seen_urls = set()
        for game in all_games:
            url = game.get('url', '')
            if url and url not in seen_urls:
                unique_games.append(game)
                seen_urls.add(url)
        
        print(f"\n" + "=" * 60)
        print(f"Total unique games collected: {len(unique_games)}")
        print("=" * 60)
        
        # Save to CSV
        scraper.save_to_csv(unique_games, 'metacritic_games.csv')
        
        # Display sample
        print("\nSample of collected games:")
        print("-" * 60)
        for i, game in enumerate(unique_games[:10], 1):
            print(f"{i}. {game.get('title', 'N/A')}")
            print(f"   Score: {game.get('score', 'N/A')} | Rating: {game.get('rating', 'N/A')}")
            print(f"   Section: {game.get('section', 'N/A')}")
            print()


if __name__ == "__main__":
    main()
    
# scrape data to scraped_data folder inside which there is meta_critic data folder
# Scrape Images as well 
