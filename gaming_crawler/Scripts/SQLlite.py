import sqlite3
import csv
import os

DB_PATH = os.path.join('Database_files', 'Games_Database.db')

def init_db():
    """Initialize database and create table if not exists"""
    os.makedirs('Database_files', exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS games (
            data_source TEXT, game_title TEXT, release_date TEXT,
            rating INTEGER, review_count INTEGER, discounted_price REAL,
            original_price REAL, discount_percentage REAL, genres TEXT,
            platform TEXT, developer TEXT, publisher TEXT,
            description TEXT, release_status TEXT, game_url TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print(f"✓ Database initialized: {DB_PATH}")

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
                    INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    print(f"  Imported {imported} records...")
        
        conn.commit()
        print(f"✓ Imported {imported} games successfully")
    except Exception as e:
        print(f"✗ Import error: {e}")
    finally:
        conn.close()

def show_stats():
    """Display database statistics"""
    count = get_record_count()
    print(f"\n=== Database Statistics ===")
    print(f"Total games: {count}")
    
    # Sample games
    samples = query_db('SELECT game_title, developer, rating FROM games LIMIT 5')
    print("\nSample games:")
    for title, dev, rating in samples:
        print(f"  • {title} by {dev} (Rating: {rating})")

def print_results(results, limit=None):
    """Pretty print query results in a formatted table"""
    if not results:
        print("No results found")
        return
    
    # Column names for the games table
    columns = ['data_source', 'game_title', 'release_date', 'rating', 'review_count',
               'discounted_price', 'original_price', 'discount_percentage', 'genres',
               'platform', 'developer', 'publisher', 'description', 'release_status', 'game_url']
    
    total = len(results)
    display = results[:limit] if limit else results
    
    print(f"\n{'='*100}")
    print(f"Found {total} results" + (f" (showing first {limit})" if limit and total > limit else ""))
    print('='*100)
    
    for i, row in enumerate(display, 1):
        print(f"\n[{i}] {row[1] if len(row) > 1 else 'N/A'}")  # Game title
        print('-'*100)
        
        # Determine which columns to show based on row length
        cols_to_show = min(len(row), len(columns))
        
        for j in range(cols_to_show):
            col_name = columns[j] if j < len(columns) else f"col_{j}"
            value = row[j]
            
            # Skip empty or redundant fields
            if value == '' or value is None:
                continue
            
            # Format specific columns
            if col_name == 'game_title':
                continue  # Already shown as header
            elif col_name == 'description' and isinstance(value, str):
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
    
    print('='*100 + '\n')

def query_db(sql):
    """Execute a SQL query and return results"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    conn.close()
    return results

# Main execution
if __name__ == "__main__":
    init_db()
    import_csv()
    #show_stats()
    results = query_db('SELECT * FROM games WHERE developer = "Valve"')
    print_results(results)