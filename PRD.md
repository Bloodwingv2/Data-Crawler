# 🎮 Gaming Industry Crawler - Project Summary

## *Topic: Multi-Platform Gaming Market Analysis*

### *Data Sources (3 Dynamic Websites):*

1. *Steam* (store.steampowered.com) - Largest PC gaming platform
2. *Metacritic* (metacritic.com/game) - Review aggregator with critic + user scores
3. *Epic Games Store* (store.epicgames.com) - Major competitor to Steam

---

## *Crawlable Data Points:*

### *Product Info:*

- Game title, description, URL
- Developer/Publisher names
- Genre/category tags
- Release date
- Platform (Windows/Mac/Linux)
- System requirements

### *Pricing:*

- Current price
- Original price (if discounted)
- Discount percentage
- Currency
- Availability status

### *Reviews/Ratings:*

- Overall review score
- Total review count
- User rating (0-10 or percentage)
- Critic score (Metacritic 0-100)
- Recent reviews (last 30 days)

### *Features/Tags:*

- Multiplayer/Single-player
- VR support
- Controller support
- Game features (Open World, Crafting, Roguelike, etc.)

### *Media:*

- Cover art/header images
- Game screenshots (3-10 per game)
- Video thumbnails

*Target:* 1,000+ game records across 3 platforms

---

## *Database Schema (5 Tables):*

1. *products* - Game info (ID, name, genre, developer, description, URL, source)
2. *pricing* - Price data (current price, original price, discount %, currency, date)
3. *reviews* - Rating data (overall score, review count, user score, critic score)
4. *media* - Images/videos (type, URL, HDFS path, file size)
5. *product_features* - Game tags/features (multiplayer, VR, etc.)

---

## *7 Business Questions (SQL Queries):*

### *1. Price Distribution by Genre*

"Which game genres have the highest/lowest average prices?"

- Identifies premium vs budget genres
- Shows market positioning strategies

### *2. Review Score vs Price Correlation*

"Do more expensive games receive better ratings?"

- Tests value-for-money perception
- Finds optimal price-to-quality ratio

### *3. Current Sales/Discount Analysis*

"What types of games are currently on sale, and by how much?"

- Shows promotional strategies by genre
- Compares discount depth across platforms

### *4. Critic vs User Score Discrepancy*

"Which games have the biggest gap between professional critics and users?"

- Identifies overhyped or underrated games
- Shows audience vs industry preference differences

### *5. Publisher/Developer Performance*

"Which publishers consistently release highly-rated games?"

- Publisher reputation analysis
- Investment decision support

### *6. Feature Popularity Analysis*

"Which game features (multiplayer, VR, open-world) correlate with higher ratings?"

- Identifies successful game mechanics
- Guides development priorities

### *7. New Releases vs Catalog Games*

"How do recently released games compare to older titles in pricing and ratings?"

- Price depreciation analysis
- Release year performance comparison

---

## *Business Value:*

- *Market Analysis:* Understand pricing strategies across platforms
- *Competitive Intelligence:* Compare Steam vs Epic Games positioning
- *Investment Insights:* Identify successful genres and publishers
- *Consumer Behavior:* Analyze rating patterns and preferences
- *Product Development:* Feature correlation with success metrics

---

## *Technical Highlights:*

- *Dynamic Scraping:* Selenium/Playwright required (JavaScript-heavy sites)
- *HDFS Storage:* Game data (JSON) + images organized in distributed file system
- *Redundancy Test:* Read/write operations with 1 DataNode offline
- *Multi-source:* Cross-platform data aggregation and comparison
- *Media Rich:* 2,000-3,000 images stored in HDFS

---

## *Project Scope:*

- 400-500 games from Steam
- 300-400 games from Metacritic
- 300 games from Epic Games Store
- *Total: 1,000-1,200 game records*
- *Storage: ~500MB-2GB* in HDFS

## *Requirements:*

- Use Selenium for scraping