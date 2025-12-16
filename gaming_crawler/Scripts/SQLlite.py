import sqlite3
import csv
import os
import shutil
from pathlib import Path

DB_PATH = os.path.join('Database_files', 'Games_Database.db')
MEDIA_DIR = Path('media_files')

def init_db():
    """Initialize database and create tables"""
    os.makedirs('Database_files', exist_ok=True)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    (MEDIA_DIR / 'images').mkdir(exist_ok=True)
    (MEDIA_DIR / 'videos').mkdir(exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Games table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source TEXT, game_title TEXT, release_date TEXT,
            rating INTEGER, review_count INTEGER, discounted_price REAL,
            original_price REAL, discount_percentage REAL, genres TEXT,
            platform TEXT, developer TEXT, publisher TEXT,
            description TEXT, release_status TEXT, game_url TEXT
        )
    ''')
    
    # Media files table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_size INTEGER,
            FOREIGN KEY (game_id) REFERENCES games (id)
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"✓ Database initialized: {DB_PATH}")
    print(f"✓ Media directory: {MEDIA_DIR}")

def get_record_count():
    """Get total number of records in database"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM games')
    count = cur.fetchone()[0]
    conn.close()
    return count

def import_csv(csv_file='Master_Dataset_Final.csv'):
    """Import CSV data into database if empty"""
    if get_record_count() > 0:
        print(f"✓ Database already has {get_record_count()} records. Skipping import.")
        return
    
    print("Starting CSV import...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    imported = 0
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                cur.execute('''
                    INSERT INTO games (data_source, game_title, release_date, rating, review_count,
                                     discounted_price, original_price, discount_percentage, genres,
                                     platform, developer, publisher, description, release_status, game_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('data_source', ''),
                    row.get('game_title', ''),
                    row.get('release_date', ''),
                    int(row.get('rating') or 0) or None,
                    int(row.get('review_count') or 0) or None,
                    float(row.get('discounted_price') or 0) or None,
                    float(row.get('original_price') or 0) or None,
                    float(row.get('discount_percentage') or 0) or None,
                    row.get('genres', ''),
                    row.get('platform', ''),
                    row.get('developer', ''),
                    row.get('publisher', ''),
                    row.get('description', ''),
                    row.get('release_status', ''),
                    row.get('game_url', '')
                ))
                imported += 1
                if imported % 500 == 0:
                    print(f"  ✓ Imported {imported} records...")
        
        conn.commit()
        print(f"✓ Successfully imported {imported} games")
    except Exception as e:
        print(f"✗ Import error: {e}")
    finally:
        conn.close()

def add_media_file(game_id, media_file_path, media_type='image'):
    """
    Add a media file (image or video) to the database
    
    Args:
        game_id: ID of the game in database
        media_file_path: Path to the media file
        media_type: 'image' or 'video'
    """
    source_path = Path(media_file_path)
    
    if not source_path.exists():
        print(f"✗ File not found: {media_file_path}")
        return False
    
    print(f"→ Processing {media_type}: {source_path.name}")
    
    # Determine destination
    subdir = 'images' if media_type == 'image' else 'videos'
    dest_filename = f"game_{game_id}_{source_path.name}"
    dest_path = MEDIA_DIR / subdir / dest_filename
    
    try:
        # Copy file
        print(f"  → Copying to {dest_path.relative_to(Path.cwd())}")
        shutil.copy2(source_path, dest_path)
        file_size = dest_path.stat().st_size
        
        # Store in database
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

def add_media_batch(game_id, media_folder):
    """
    Add multiple media files from a folder
    
    Args:
        game_id: ID of the game in database
        media_folder: Path to folder containing media files
    """
    folder = Path(media_folder)
    if not folder.exists():
        print(f"✗ Folder not found: {media_folder}")
        return
    
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    video_exts = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'}
    
    total_added = 0
    print(f"\n{'='*80}")
    print(f"Processing media files from: {media_folder}")
    print('='*80)
    
    for file_path in folder.iterdir():
        if file_path.is_file():
            ext = file_path.suffix.lower()
            
            if ext in image_exts:
                if add_media_file(game_id, file_path, 'image'):
                    total_added += 1
            elif ext in video_exts:
                if add_media_file(game_id, file_path, 'video'):
                    total_added += 1
    
    print(f"{'='*80}")
    print(f"✓ Added {total_added} media files for game ID {game_id}\n")

def find_game_by_title(game_title):
    """Find game ID by matching title"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Try exact match first (using ROWID instead of id)
    cur.execute('SELECT ROWID, game_title FROM games WHERE game_title = ?', (game_title,))
    result = cur.fetchone()
    
    # If no exact match, try case-insensitive partial match
    if not result:
        cur.execute('SELECT ROWID, game_title FROM games WHERE LOWER(game_title) LIKE LOWER(?)', (f'%{game_title}%',))
        result = cur.fetchone()
    
    conn.close()
    return result

def scan_and_import_all_media():
    """Scan nested media_files directory structure and import all media files"""
    
    if not MEDIA_DIR.exists():
        print(f"✗ Media directory not found: {MEDIA_DIR}")
        return
    
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    video_exts = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'}
    
    total_added = 0
    total_skipped = 0
    
    print(f"\n{'='*80}")
    print("SCANNING AND IMPORTING ALL MEDIA FILES")
    print('='*80)
    
    # Walk through all subdirectories in media_files
    for root, dirs, files in os.walk(MEDIA_DIR):
        root_path = Path(root)
        
        # Skip base images/videos folders
        if root_path.name in ['images', 'videos']:
            continue
        
        # Extract game title from folder structure
        # Expected: media_files/platform_folder/Game Title/
        folder_parts = root_path.relative_to(MEDIA_DIR).parts
        
        if len(folder_parts) < 2:
            continue
        
        game_title = folder_parts[1]  # Game name is second level
        
        if not files:
            continue
        
        print(f"\n→ Processing: {game_title}")
        
        # Find game in database
        game_match = find_game_by_title(game_title)
        
        if not game_match:
            print(f"  ✗ Not found in database: {game_title}")
            total_skipped += len(files)
            continue
        
        game_id, db_title = game_match
        print(f"  ✓ Matched to: {db_title} (ID: {game_id})")
        
        # Process each media file
        for file_name in files:
            file_path = root_path / file_name
            ext = file_path.suffix.lower()
            
            if ext in image_exts:
                media_type = 'image'
            elif ext in video_exts:
                media_type = 'video'
            else:
                continue
            
            try:
                file_size = file_path.stat().st_size
                
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                
                cur.execute('''
                    INSERT INTO media_files (game_id, media_type, file_name, file_path, file_size)
                    VALUES (?, ?, ?, ?, ?)
                ''', (game_id, media_type, file_name, str(file_path), file_size))
                
                conn.commit()
                conn.close()
                
                total_added += 1
                print(f"    ✓ Added {media_type}: {file_name}")
            except Exception as e:
                print(f"    ✗ Error: {file_name} - {e}")
                total_skipped += 1
    
    print(f"\n{'='*80}")
    print(f"✓ Import complete!")
    print(f"  Added: {total_added} files")
    print(f"  Skipped: {total_skipped} files")
    print('='*80 + '\n')

def get_game_media(game_id):
    """Get all media files for a specific game"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('SELECT * FROM media_files WHERE game_id = ?', (game_id,))
    media = cur.fetchall()
    
    conn.close()
    return media

def show_stats():
    """Display database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('SELECT COUNT(*) FROM games')
    game_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*), SUM(file_size) FROM media_files WHERE media_type = "image"')
    img_count, img_size = cur.fetchone()
    
    cur.execute('SELECT COUNT(*), SUM(file_size) FROM media_files WHERE media_type = "video"')
    vid_count, vid_size = cur.fetchone()
    
    conn.close()
    
    print(f"\n{'='*80}")
    print("DATABASE STATISTICS")
    print('='*80)
    print(f"Total games      : {game_count}")
    print(f"Total images     : {img_count or 0} files ({(img_size or 0) / (1024*1024):.2f} MB)")
    print(f"Total videos     : {vid_count or 0} files ({(vid_size or 0) / (1024*1024):.2f} MB)")
    print('='*80 + '\n')

def print_results(results, limit=None):
    """Pretty print query results"""
    if not results:
        print("No results found")
        return
    
    columns = ['id', 'data_source', 'game_title', 'release_date', 'rating', 'review_count',
               'discounted_price', 'original_price', 'discount_percentage', 'genres',
               'platform', 'developer', 'publisher', 'description', 'release_status', 'game_url']
    
    total = len(results)
    display = results[:limit] if limit else results
    
    print(f"\n{'='*100}")
    print(f"Found {total} results" + (f" (showing first {limit})" if limit and total > limit else ""))
    print('='*100)
    
    for i, row in enumerate(display, 1):
        game_title = row[2] if len(row) > 2 else 'N/A'
        print(f"\n[{i}] {game_title}")
        print('-'*100)
        
        cols_to_show = min(len(row), len(columns))
        
        for j in range(cols_to_show):
            col_name = columns[j]
            value = row[j]
            
            if value == '' or value is None or col_name == 'game_title':
                continue
            
            if col_name == 'description' and isinstance(value, str):
                value = value[:150] + "..." if len(value) > 150 else value
            elif col_name == 'game_url' and isinstance(value, str):
                value = value[:80] + "..." if len(value) > 80 else value
            elif col_name in ['discounted_price', 'original_price']:
                value = f"${value:.2f}" if value else "Free"
            elif col_name == 'discount_percentage':
                value = f"{value}% off" if value else "No discount"
            elif col_name == 'rating':
                value = f"{value}/100"
            
            print(f"  {col_name:20s}: {value}")
        
        # Show media files if game_id available
        if len(row) > 0:
            media = get_game_media(row[0])
            if media:
                print(f"\n  {'Media files':20s}: {len(media)} files")
                for m in media[:3]:  # Show first 3
                    print(f"    → {m[2]}: {m[3]} ({m[5]/1024:.1f} KB)")
    
    print('='*100 + '\n')

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
    
    # Scan and import all media files from nested directory structure
    scan_and_import_all_media()
    
    show_stats()
    
    # Example: Query and display results
    print("\n=== Sample Query: Valve Games ===")
    results = query_db('SELECT * FROM games WHERE developer = ?', ('Valve',))
    print_results(results, limit=3)
    
    # Example: Add media files (uncomment to use)
    # add_media_file(game_id=1, media_file_path='path/to/screenshot.jpg', media_type='image')
    # add_media_batch(game_id=1, media_folder='path/to/game_media_folder')