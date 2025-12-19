import sqlite3
import csv
import os
import shutil
from pathlib import Path
from datetime import datetime

DB_PATH = os.path.join('Database_files', 'Games_Database.db')
MEDIA_DIR = Path('media')

def init_db():
    """Initialize database and create 5 tables"""
    os.makedirs('Database_files', exist_ok=True)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    (MEDIA_DIR / 'images').mkdir(exist_ok=True)
    (MEDIA_DIR / 'videos').mkdir(exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Table 1: Games (main table)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source TEXT,
            game_title TEXT NOT NULL,
            release_date TEXT,
            rating INTEGER,
            review_count INTEGER,
            discounted_price REAL,
            original_price REAL,
            discount_percentage REAL,
            platform TEXT,
            developer TEXT,
            publisher TEXT,
            description TEXT,
            release_status TEXT,
            game_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Table 2: Media files
    cur.execute('''
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_size INTEGER,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games (id) ON DELETE CASCADE
        )
    ''')
    
    # Table 3: Genres (normalized genre table)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT UNIQUE NOT NULL,
            description TEXT
        )
    ''')
    
    # Table 4: Game_Genres (junction table for many-to-many relationship)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS game_genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            genre_id INTEGER NOT NULL,
            FOREIGN KEY (game_id) REFERENCES games (id) ON DELETE CASCADE,
            FOREIGN KEY (genre_id) REFERENCES genres (id) ON DELETE CASCADE,
            UNIQUE(game_id, genre_id)
        )
    ''')
    
    # Table 5: User reviews/ratings
    cur.execute('''
        CREATE TABLE IF NOT EXISTS user_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            user_name TEXT NOT NULL,
            rating INTEGER CHECK(rating >= 0 AND rating <= 100),
            review_text TEXT,
            helpful_count INTEGER DEFAULT 0,
            review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games (id) ON DELETE CASCADE
        )
    ''')
    
    # Create indexes for better query performance
    cur.execute('CREATE INDEX IF NOT EXISTS idx_games_title ON games(game_title)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_games_developer ON games(developer)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_games_rating ON games(rating)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_media_game ON media_files(game_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_genres_game ON game_genres(game_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_reviews_game ON user_reviews(game_id)')
    
    conn.commit()
    conn.close()
    print(f"✓ Database initialized with 5 tables: {DB_PATH}")
    print(f"  1. games")
    print(f"  2. media_files")
    print(f"  3. genres")
    print(f"  4. game_genres")
    print(f"  5. user_reviews")
    print(f"✓ Media directory: {MEDIA_DIR}")

def get_record_count(table='games'):
    """Get total number of records in specified table"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    count = cur.fetchone()[0]
    conn.close()
    return count

def import_csv(csv_file='Master_Dataset_Final.csv'):
    """Import CSV data into database if empty"""
    if get_record_count('games') > 0:
        print(f"✓ Database already has {get_record_count('games')} records. Skipping import.")
        return
    
    print("Starting CSV import...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    imported = 0
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Insert game data
                cur.execute('''
                    INSERT INTO games (data_source, game_title, release_date, rating, review_count,
                                     discounted_price, original_price, discount_percentage,
                                     platform, developer, publisher, description, release_status, game_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('data_source', ''),
                    row.get('game_title', ''),
                    row.get('release_date', ''),
                    int(row.get('rating') or 0) or None,
                    int(row.get('review_count') or 0) or None,
                    float(row.get('discounted_price') or 0) or None,
                    float(row.get('original_price') or 0) or None,
                    float(row.get('discount_percentage') or 0) or None,
                    row.get('platform', ''),
                    row.get('developer', ''),
                    row.get('publisher', ''),
                    row.get('description', ''),
                    row.get('release_status', ''),
                    row.get('game_url', '')
                ))
                
                game_id = cur.lastrowid
                
                # Process genres from CSV and populate genre tables
                genres_str = row.get('genres', '')
                if genres_str:
                    genres_list = [g.strip() for g in genres_str.split(',') if g.strip()]
                    for genre_name in genres_list:
                        # Insert genre if not exists
                        cur.execute('INSERT OR IGNORE INTO genres (genre_name) VALUES (?)', (genre_name,))
                        
                        # Get genre_id
                        cur.execute('SELECT id FROM genres WHERE genre_name = ?', (genre_name,))
                        genre_id = cur.fetchone()[0]
                        
                        # Link game to genre
                        cur.execute('INSERT OR IGNORE INTO game_genres (game_id, genre_id) VALUES (?, ?)', 
                                  (game_id, genre_id))
                
                imported += 1
                if imported % 500 == 0:
                    print(f"  ✓ Imported {imported} records...")
        
        conn.commit()
        print(f"✓ Successfully imported {imported} games")
        print(f"✓ Created {get_record_count('genres')} unique genres")
    except Exception as e:
        print(f"✗ Import error: {e}")
    finally:
        conn.close()

def add_user_review(game_id, user_name, rating, review_text=""):
    """Add a user review for a game"""
    if rating < 0 or rating > 100:
        print("✗ Rating must be between 0 and 100")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    try:
        cur.execute('''
            INSERT INTO user_reviews (game_id, user_name, rating, review_text)
            VALUES (?, ?, ?, ?)
        ''', (game_id, user_name, rating, review_text))
        
        conn.commit()
        print(f"✓ Added review from {user_name} for game ID {game_id}")
        return True
    except Exception as e:
        print(f"✗ Error adding review: {e}")
        return False
    finally:
        conn.close()

def get_game_with_genres(game_id):
    """Get game details with all its genres"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('SELECT * FROM games WHERE id = ?', (game_id,))
    game = cur.fetchone()
    
    cur.execute('''
        SELECT g.genre_name FROM genres g
        JOIN game_genres gg ON g.id = gg.genre_id
        WHERE gg.game_id = ?
    ''', (game_id,))
    genres = [row[0] for row in cur.fetchall()]
    
    conn.close()
    return game, genres

def get_games_by_genre(genre_name, limit=10):
    """Get all games in a specific genre"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT g.* FROM games g
        JOIN game_genres gg ON g.id = gg.game_id
        JOIN genres gen ON gg.genre_id = gen.id
        WHERE gen.genre_name = ?
        LIMIT ?
    ''', (genre_name, limit))
    
    results = cur.fetchall()
    conn.close()
    return results

def get_top_rated_games(limit=10):
    """Get top rated games based on rating and review count"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT id, game_title, rating, review_count, developer, platform
        FROM games
        WHERE rating IS NOT NULL AND review_count > 100
        ORDER BY rating DESC, review_count DESC
        LIMIT ?
    ''', (limit,))
    
    results = cur.fetchall()
    conn.close()
    return results

def get_game_reviews(game_id):
    """Get all reviews for a specific game"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT user_name, rating, review_text, helpful_count, review_date
        FROM user_reviews
        WHERE game_id = ?
        ORDER BY helpful_count DESC, review_date DESC
    ''', (game_id,))
    
    results = cur.fetchall()
    conn.close()
    return results

def add_media_file(game_id, media_file_path, media_type='image'):
    """Add a media file (image or video) to the database"""
    source_path = Path(media_file_path)
    
    if not source_path.exists():
        print(f"✗ File not found: {media_file_path}")
        return False
    
    print(f"→ Processing {media_type}: {source_path.name}")
    
    subdir = 'images' if media_type == 'image' else 'videos'
    dest_filename = f"game_{game_id}_{source_path.name}"
    dest_path = MEDIA_DIR / subdir / dest_filename
    
    try:
        shutil.copy2(source_path, dest_path)
        file_size = dest_path.stat().st_size
        
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        cur.execute('''
            INSERT INTO media_files (game_id, media_type, file_name, file_path, file_size)
            VALUES (?, ?, ?, ?, ?)
        ''', (game_id, media_type, source_path.name, str(dest_path.relative_to(Path.cwd())), file_size))
        
        conn.commit()
        conn.close()
        
        print(f"  ✓ Added {media_type}: {dest_filename} ({file_size / 1024:.2f} KB)")
        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

def get_developer_stats(developer_name):
    """Get statistics for a specific developer"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT 
            COUNT(*) as game_count,
            AVG(rating) as avg_rating,
            SUM(review_count) as total_reviews,
            MIN(release_date) as first_release,
            MAX(release_date) as latest_release
        FROM games
        WHERE developer = ?
    ''', (developer_name,))
    
    stats = cur.fetchone()
    conn.close()
    return stats

def get_genre_distribution():
    """Get distribution of games across genres"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT g.genre_name, COUNT(gg.game_id) as game_count
        FROM genres g
        LEFT JOIN game_genres gg ON g.id = gg.genre_id
        GROUP BY g.id
        ORDER BY game_count DESC
    ''')
    
    results = cur.fetchall()
    conn.close()
    return results

def show_stats():
    """Display comprehensive database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('SELECT COUNT(*) FROM games')
    game_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM genres')
    genre_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM user_reviews')
    review_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*), SUM(file_size) FROM media_files WHERE media_type = "image"')
    img_count, img_size = cur.fetchone()
    
    cur.execute('SELECT COUNT(*), SUM(file_size) FROM media_files WHERE media_type = "video"')
    vid_count, vid_size = cur.fetchone()
    
    conn.close()
    
    print(f"\n{'='*80}")
    print("DATABASE STATISTICS (5 TABLES)")
    print('='*80)
    print(f"Games            : {game_count}")
    print(f"Genres           : {genre_count}")
    print(f"User Reviews     : {review_count}")
    print(f"Images           : {img_count or 0} files ({(img_size or 0) / (1024*1024):.2f} MB)")
    print(f"Videos           : {vid_count or 0} files ({(vid_size or 0) / (1024*1024):.2f} MB)")
    print('='*80 + '\n')

def find_game_by_title(game_title):
    """Find game ID by matching title"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('SELECT id, game_title FROM games WHERE game_title = ?', (game_title,))
    result = cur.fetchone()
    
    if not result:
        cur.execute('SELECT id, game_title FROM games WHERE LOWER(game_title) LIKE LOWER(?)', 
                   (f'%{game_title}%',))
        result = cur.fetchone()
    
    conn.close()
    return result

def business_query_1_top_rated_games():
    """Top 5 Rated Games with significant review counts"""
    print("\n[BUSINESS QUERY 1] Top 5 Highest Rated Games (>1000 reviews)")
    print("-" * 80)
    
    query = """
    SELECT game_title, rating, review_count 
    FROM games 
    WHERE review_count > 1000 
        AND rating = (
            SELECT MAX(rating) 
            FROM games 
            WHERE review_count > 1000
        ) 
    ORDER BY rating DESC, review_count DESC 
    LIMIT 5
    """
    
    results = query_db(query)
    if results:
        for i, (title, rating, reviews) in enumerate(results, 1):
            print(f"{i}. {title}")
            print(f"   Rating: {rating}/100 | Reviews: {reviews:,}")
    else:
        print("  No games found with >1000 reviews")
    return results

def business_query_2_top_performers():
    """Top Performing Games by Rating and Popularity"""
    print("\n[BUSINESS QUERY 2] Top 10 High-Quality Popular Games (Rating ≥ 85, Reviews ≥ 50)")
    print("-" * 80)
    
    query = """
    SELECT 
        game_title, 
        rating, 
        review_count, 
        data_source,
        platform
    FROM games 
    WHERE rating >= 85 
        AND review_count >= 50 
    ORDER BY rating DESC, review_count DESC 
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (title, rating, reviews, source, platform) in enumerate(results, 1):
        print(f"{i}. {title}")
        print(f"   Rating: {rating}/100 | Reviews: {reviews:,} | Source: {source} | Platform: {platform}")
    return results

def business_query_3_hidden_gems():
    """Hidden Gems - High Rating but Low Visibility"""
    print("\n[BUSINESS QUERY 3] Hidden Gems (Rating ≥ 85, Reviews < 20)")
    print("-" * 80)
    
    query = """
    SELECT 
        game_title, 
        rating, 
        review_count, 
        data_source,
        developer
    FROM games 
    WHERE rating >= 85 
        AND review_count < 20 
    ORDER BY rating DESC 
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (title, rating, reviews, source, dev) in enumerate(results, 1):
        print(f"{i}. {title}")
        print(f"   Rating: {rating}/100 | Reviews: {reviews} | Developer: {dev} | Source: {source}")
    return results

def business_query_4_consistent_publishers():
    """Publishers Delivering Consistent Quality"""
    print("\n[BUSINESS QUERY 4] Top 10 Publishers with Consistent Quality (≥5 games)")
    print("-" * 80)
    
    query = """
    SELECT 
        publisher, 
        ROUND(AVG(rating), 2) AS avg_rating, 
        COUNT(*) AS total_games,
        MIN(rating) AS min_rating,
        MAX(rating) AS max_rating
    FROM games 
    WHERE publisher IS NOT NULL 
        AND publisher != '' 
        AND rating IS NOT NULL
    GROUP BY publisher 
    HAVING COUNT(*) >= 5 
    ORDER BY avg_rating DESC 
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (pub, avg_rating, total, min_r, max_r) in enumerate(results, 1):
        print(f"{i}. {pub}")
        print(f"   Avg Rating: {avg_rating}/100 | Games: {total} | Range: {min_r}-{max_r}")
    return results

def business_query_5_platform_pricing():
    """Platform with Highest Average Discounted Price"""
    print("\n[BUSINESS QUERY 5] Platform with Highest Average Discounted Price")
    print("-" * 80)
    
    query = """
    SELECT 
        platform, 
        ROUND(AVG(discounted_price), 2) AS avg_discounted_price,
        COUNT(*) AS game_count
    FROM games 
    WHERE discounted_price IS NOT NULL 
        AND discounted_price > 0
    GROUP BY platform 
    HAVING AVG(discounted_price) = (
        SELECT MAX(avg_price) 
        FROM (
            SELECT AVG(discounted_price) AS avg_price 
            FROM games 
            WHERE discounted_price IS NOT NULL 
                AND discounted_price > 0
            GROUP BY platform
        )
    )
    """
    
    results = query_db(query)
    for platform, avg_price, count in results:
        print(f"Platform: {platform}")
        print(f"  Average Discounted Price: ${avg_price:.2f}")
        print(f"  Total Games: {count}")
    return results

def business_query_6_discount_analysis():
    """Best Discount Opportunities - High Quality Games with Deep Discounts"""
    print("\n[BUSINESS QUERY 6] Best Discount Opportunities (Rating ≥ 80, Discount ≥ 50%)")
    print("-" * 80)
    
    query = """
    SELECT 
        game_title,
        rating,
        original_price,
        discounted_price,
        discount_percentage,
        platform
    FROM games
    WHERE rating >= 80
        AND discount_percentage >= 50
        AND discounted_price IS NOT NULL
    ORDER BY discount_percentage DESC, rating DESC
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (title, rating, orig, disc, discount, platform) in enumerate(results, 1):
        savings = orig - disc if orig and disc else 0
        print(f"{i}. {title} ({platform})")
        print(f"   Rating: {rating}/100 | ${orig:.2f} → ${disc:.2f} ({discount:.0f}% off) | Save: ${savings:.2f}")
    return results

def business_query_7_genre_revenue_potential():
    """Genre Revenue Potential - Average Price by Genre"""
    print("\n[BUSINESS QUERY 7] Top 10 Genres by Average Game Price (Revenue Potential)")
    print("-" * 80)
    
    query = """
    SELECT 
        g.genre_name,
        ROUND(AVG(games.original_price), 2) AS avg_price,
        COUNT(DISTINCT games.id) AS game_count,
        ROUND(AVG(games.rating), 2) AS avg_rating
    FROM genres g
    JOIN game_genres gg ON g.id = gg.genre_id
    JOIN games ON gg.game_id = games.id
    WHERE games.original_price IS NOT NULL 
        AND games.original_price > 0
    GROUP BY g.genre_name
    HAVING COUNT(DISTINCT games.id) >= 10
    ORDER BY avg_price DESC
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (genre, avg_price, count, avg_rating) in enumerate(results, 1):
        print(f"{i}. {genre}")
        print(f"   Avg Price: ${avg_price:.2f} | Games: {count} | Avg Rating: {avg_rating}/100")
    return results

def business_query_8_developer_efficiency():
    """Developer Efficiency - High Rating per Game Count"""
    print("\n[BUSINESS QUERY 8] Most Efficient Developers (Quality vs Quantity)")
    print("-" * 80)
    
    query = """
    SELECT 
        developer,
        COUNT(*) AS total_games,
        ROUND(AVG(rating), 2) AS avg_rating,
        SUM(review_count) AS total_reviews,
        ROUND(AVG(original_price), 2) AS avg_price
    FROM games
    WHERE developer IS NOT NULL 
        AND developer != ''
        AND rating IS NOT NULL
    GROUP BY developer
    HAVING COUNT(*) >= 3
    ORDER BY avg_rating DESC, total_games ASC
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (dev, games, avg_r, reviews, avg_p) in enumerate(results, 1):
        print(f"{i}. {dev}")
        print(f"   Games: {games} | Avg Rating: {avg_r}/100 | Total Reviews: {reviews:,} | Avg Price: ${avg_p:.2f}")
    return results

def business_query_9_cross_platform_analysis():
    """Cross-Platform Game Analysis"""
    print("\n[BUSINESS QUERY 9] Games Available on Multiple Platforms")
    print("-" * 80)
    
    query = """
    SELECT 
        game_title,
        COUNT(DISTINCT platform) AS platform_count,
        GROUP_CONCAT(DISTINCT platform) AS platforms,
        AVG(rating) AS avg_rating,
        AVG(review_count) AS avg_reviews
    FROM games
    WHERE platform IS NOT NULL 
        AND platform != ''
    GROUP BY game_title
    HAVING COUNT(DISTINCT platform) > 1
    ORDER BY platform_count DESC, avg_rating DESC
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (title, plat_count, platforms, avg_r, avg_rev) in enumerate(results, 1):
        print(f"{i}. {title}")
        print(f"   Platforms ({plat_count}): {platforms}")
        print(f"   Avg Rating: {avg_r:.1f}/100 | Avg Reviews: {avg_rev:.0f}")
    return results

def business_query_10_genre_popularity_trend():
    """Genre Popularity by Review Volume"""
    print("\n[BUSINESS QUERY 10] Most Popular Genres by Total Review Volume")
    print("-" * 80)
    
    query = """
    SELECT 
        g.genre_name,
        COUNT(DISTINCT games.id) AS game_count,
        SUM(games.review_count) AS total_reviews,
        ROUND(AVG(games.rating), 2) AS avg_rating,
        ROUND(AVG(games.review_count), 0) AS avg_reviews_per_game
    FROM genres g
    JOIN game_genres gg ON g.id = gg.genre_id
    JOIN games ON gg.game_id = games.id
    WHERE games.review_count IS NOT NULL
    GROUP BY g.genre_name
    HAVING COUNT(DISTINCT games.id) >= 5
    ORDER BY total_reviews DESC
    LIMIT 10
    """
    
    results = query_db(query)
    for i, (genre, games, total_rev, avg_r, avg_rev_per) in enumerate(results, 1):
        print(f"{i}. {genre}")
        print(f"   Games: {games} | Total Reviews: {total_rev:,} | Avg Rating: {avg_r}/100")
        print(f"   Avg Reviews per Game: {avg_rev_per:.0f}")
    return results

def perform_operations():
    """Perform comprehensive business intelligence queries"""
    print("\n" + "="*80)
    print("PERFORMING BUSINESS INTELLIGENCE QUERIES")
    print("="*80)
    
    # Execute all 10 business queries
    business_query_1_top_rated_games()
    business_query_2_top_performers()
    business_query_3_hidden_gems()
    business_query_4_consistent_publishers()
    business_query_5_platform_pricing()
    business_query_6_discount_analysis()
    business_query_7_genre_revenue_potential()
    business_query_8_developer_efficiency()
    business_query_9_cross_platform_analysis()
    business_query_10_genre_popularity_trend()
    
    print("\n" + "="*80)
    print("ALL BUSINESS QUERIES COMPLETED")
    print("="*80 + "\n")

def query_db(sql, params=()):
    """Execute SQL query and return results"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(sql, params)
    results = cur.fetchall()
    conn.close()
    return results

# Main execution
if __name__ == "__main__":
    init_db()
    import_csv()
    show_stats()
    
    # Perform various database operations
    perform_operations()
    
    print("\n=== Database ready with 5 tables ===")
    print("Tables: games, media_files, genres, game_genres, user_reviews")