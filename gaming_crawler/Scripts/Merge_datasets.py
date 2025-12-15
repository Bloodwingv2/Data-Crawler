import pandas as pd
import re
import numpy as np
from datetime import datetime

def load_csv_safely(filepath, encoding='utf-8'):
    """Load CSV with fallback encoding"""
    try:
        return pd.read_csv(filepath, encoding=encoding)
    except UnicodeDecodeError:
        return pd.read_csv(filepath, encoding='latin-1')
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def clean_text(text):
    """Clean text from HTML artifacts and extra whitespace"""
    if pd.isna(text):
        return None
    text = str(text).strip()
    
    # Skip if empty or just "N/A"
    if text in ["N/A", "", "n/a", "NA"]:
        return None
    
    # Remove HTML entities and Unicode artifacts
    text = re.sub(r'&nbsp;|&amp;|&lt;|&gt;|&quot;', ' ', text)
    text = re.sub(r'[\u00a0\u200b\xa0]', ' ', text)
    text = re.sub(r'\\[nrt]', ' ', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text if text else None

def normalize_rating(rating_value, source):
    """Normalize ratings to 0-100 scale, preserve None for missing"""
    if pd.isna(rating_value) or rating_value in ["N/A", "--", "", "No user reviews"]:
        return None
    
    try:
        rating = float(rating_value)
        
        if source == 'instant_gaming' and 0 <= rating <= 10:
            return round(rating * 10, 1)
        elif source == 'GOG' and 0 <= rating <= 5:
            return round(rating * 20, 1)
        elif source == 'Steam' and 0 <= rating <= 100:
            return round(rating, 1)
        
        return None
    except (ValueError, TypeError):
        return None

def normalize_price(price_value):
    """Extract numerical price, keep None for missing"""
    if pd.isna(price_value) or price_value == "N/A":
        return None
    
    price_str = str(price_value).strip().lower()
    
    # Handle "free" explicitly
    if 'free' in price_str:
        return 0.0
    
    # Remove currency symbols
    price_str = re.sub(r'[€$£¥]', '', price_str).strip()
    
    # Handle European format (comma as decimal)
    if ',' in price_str and '.' not in price_str:
        price_str = price_str.replace(',', '.')
    elif ',' in price_str and '.' in price_str:
        # e.g., "1.234,56" -> "1234.56"
        price_str = price_str.replace('.', '').replace(',', '.')
    
    # Extract first numeric value
    match = re.search(r'\d+\.?\d*', price_str)
    if match:
        return float(match.group())
    
    return None

def normalize_date(date_value):
    """Standardize dates to YYYY-MM-DD format, preserve None for missing"""
    if pd.isna(date_value) or date_value in ["N/A", ""]:
        return None
    
    date_str = str(date_value).strip()
    
    # Try multiple date formats
    formats = [
        ('%d-%b-%y', r'^\d{1,2}-[A-Za-z]{3}-\d{2}$'),          # 30-Oct-25
        ('%B %d, %Y', r'^[A-Za-z]+\s+\d{1,2},\s+\d{4}$'),      # October 30, 2025
        ('%d-%b-%Y', r'^\d{1,2}-[A-Za-z]{3}-\d{4}$'),          # 30-Oct-2025
        ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}$'),                  # 2025-10-30
        ('%d %b, %Y', r'^\d{1,2}\s+[A-Za-z]{3},\s+\d{4}$'),    # 30 Oct, 2025
        ('%b %d, %Y', r'^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}$'),    # Oct 30, 2025
        ('%d-%m-%Y', r'^\d{1,2}-\d{2}-\d{4}$'),                # 30-10-2025
    ]
    
    for fmt, pattern in formats:
        if re.match(pattern, date_str):
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
    
    # Handle year-only format
    if re.match(r'^\d{4}$', date_str):
        return f"{date_str}-01-01"
    
    return None

def extract_genres(genres_value):
    """Clean and standardize genres"""
    if pd.isna(genres_value) or genres_value == "N/A":
        return None
    
    # Split by common delimiters
    genres = re.split(r'[,;|]', str(genres_value))
    cleaned = []
    
    for g in genres:
        g = clean_text(g)
        if g and len(g) > 1:
            cleaned.append(g.title())
    
    return ', '.join(cleaned) if cleaned else None

def clean_platform(platform_value):
    """Clean platform data"""
    if pd.isna(platform_value) or platform_value in ["N/A", ""]:
        return None
    
    platform_str = str(platform_value).strip()
    if platform_str.lower() in ['', 'n/a', 'none', 'null']:
        return None
    
    return platform_str

def calculate_discounted_price(row):
    """Calculate discounted price from original and discount percentage"""
    # Priority order: discounted_price > current_price > price
    for col in ['discounted_price', 'current_price', 'price']:
        if col in row and pd.notna(row.get(col)):
            return row.get(col)
    
    # Calculate from original price and discount
    original = row.get('original_price')
    discount = row.get('discount_percentage')
    
    if pd.notna(original) and pd.notna(discount):
        try:
            orig = float(original)
            disc = float(str(discount).replace('%', '').replace('-', '').strip())
            if disc > 0:
                return round(orig * (1 - disc / 100), 2)
            else:
                return orig
        except (ValueError, TypeError):
            pass
    
    return original  # Return original if no discount info

def map_columns_to_standard(df, source):
    """Map source-specific columns to standardized names"""
    mappings = {
        'Steam': {
            'title': 'game_title',
            'rating_score': 'rating_raw',
            'url': 'game_url',
            'original_price': 'original_price_raw',
            'price': 'current_price_raw',
            'review_count': 'review_count',
            'release_date': 'release_date',
            'genres': 'genres',
            'developer': 'developer',
            'publisher': 'publisher',
            'description': 'description',
            'platforms': 'platforms'
        },
        'instant_gaming': {
            'title': 'game_title',
            'ig_rating': 'rating_raw',
            'url': 'game_url',
            'genre': 'genres',
            'current_price': 'current_price_raw',
            'original_price': 'original_price_raw',
            'discount_percentage': 'discount_percentage',
            'platforms': 'platforms',
            'release_date': 'release_date',
            'developer': 'developer',
            'publisher': 'publisher',
            'description': 'description',
            'review_count': 'review_count'
        },
        'GOG': {
            'title': 'game_title',
            'rating': 'rating_raw',
            'rating_count': 'review_count',
            'url': 'game_url',
            'price': 'current_price_raw',
            'original_price': 'original_price_raw',
            'discount_percentage': 'discount_percentage',
            'release_date': 'release_date',
            'genres': 'genres',
            'platforms': 'platforms',
            'developer': 'developer',
            'publisher': 'publisher',
            'description': 'description',
            'status_tag': 'release_status'
        }
    }
    
    return df.rename(columns=mappings.get(source, {}))

def validate_and_clean_data(df, source):
    """Clean and validate data without data loss"""
    print(f"   Processing {source}...")
    initial_count = len(df)
    
    # Text cleaning
    text_columns = ['game_title', 'description', 'genres', 'developer', 'publisher']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    
    # Rating normalization
    if 'rating_raw' in df.columns:
        df['rating'] = df['rating_raw'].apply(lambda x: normalize_rating(x, source))
        df = df.drop(columns=['rating_raw'])
    
    # Price normalization
    price_mappings = {
        'original_price_raw': 'original_price',
        'current_price_raw': 'current_price'
    }
    
    for old_col, new_col in price_mappings.items():
        if old_col in df.columns:
            df[new_col] = df[old_col].apply(normalize_price)
            df = df.drop(columns=[old_col])
    
    # Discount percentage cleaning
    if 'discount_percentage' in df.columns:
        df['discount_percentage'] = df['discount_percentage'].apply(
            lambda v: float(str(v).replace('%', '').replace('-', '').strip()) 
            if pd.notna(v) and v not in ["N/A", ""] else None
        )
    
    # Calculate final price
    df['discounted_price'] = df.apply(calculate_discounted_price, axis=1)
    
    # Date normalization
    if 'release_date' in df.columns:
        df['release_date'] = df['release_date'].apply(normalize_date)
    
    # Genres extraction
    if 'genres' in df.columns:
        df['genres'] = df['genres'].apply(extract_genres)
    
    # Platform standardization
    if 'platforms' in df.columns:
        df['platform'] = df['platforms'].apply(clean_platform)
        df = df.drop(columns=['platforms'])
    elif 'platform' in df.columns:
        df['platform'] = df['platform'].apply(clean_platform)
    
    # Only drop rows with invalid game titles (essential field)
    if 'game_title' in df.columns:
        df = df.dropna(subset=['game_title'])
        df = df[df['game_title'].str.len() > 0]
    
    # Remove exact URL duplicates within source
    if 'game_url' in df.columns:
        df = df.drop_duplicates(subset=['game_url'], keep='first')
    
    print(f"      {initial_count} → {len(df)} rows (removed {initial_count - len(df)} invalid/duplicate)")
    
    return df

def create_unified_schema(df):
    """Ensure all standard columns exist"""
    standard_columns = [
        'data_source', 'game_title', 'release_date', 'rating', 'review_count',
        'discounted_price', 'original_price', 'discount_percentage',
        'genres', 'platform', 'developer', 'publisher', 'description',
        'release_status', 'game_url'
    ]
    
    for col in standard_columns:
        if col not in df.columns:
            df[col] = None
    
    return df[standard_columns]

def smart_deduplicate(df):
    """Intelligent deduplication preserving best data"""
    print("\n4. Smart deduplication...")
    initial = len(df)
    
    # Normalize titles for matching
    df['title_normalized'] = df['game_title'].str.lower().str.strip()
    df['title_normalized'] = df['title_normalized'].str.replace(r'[^\w\s]', '', regex=True)
    
    # Create quality score for each record
    df['quality_score'] = 0
    
    # Score based on data completeness
    for col in ['rating', 'release_date', 'developer', 'publisher', 'genres', 'description']:
        df['quality_score'] += df[col].notna().astype(int)
    
    # Prefer records with numeric ratings over None
    df['quality_score'] += (pd.to_numeric(df['rating'], errors='coerce').notna().astype(int) * 2)
    
    # Sort by quality score (descending) and keep first
    df = df.sort_values('quality_score', ascending=False)
    df = df.drop_duplicates(subset=['title_normalized'], keep='first')
    
    # Clean up temporary columns
    df = df.drop(columns=['title_normalized', 'quality_score'])
    
    print(f"   Removed {initial - len(df)} duplicate titles (kept best records)")
    
    return df

def generate_quality_report(df):
    """Generate data quality report"""
    print("\n" + "="*70)
    print("DATA QUALITY REPORT")
    print("="*70)
    print(f"\nTotal unique games: {len(df)}")
    
    # Source breakdown
    print("\nGames by source:")
    for source in df['data_source'].unique():
        count = len(df[df['data_source'] == source])
        pct = (count / len(df)) * 100
        print(f"   {source}: {count} ({pct:.1f}%)")
    
    # Rating statistics
    numeric_ratings = pd.to_numeric(df['rating'], errors='coerce')
    rated_games = numeric_ratings.notna().sum()
    unrated_games = len(df) - rated_games
    
    print(f"\nRating coverage:")
    print(f"   Games with ratings: {rated_games} ({rated_games/len(df)*100:.1f}%)")
    print(f"   Games without ratings: {unrated_games} ({unrated_games/len(df)*100:.1f}%)")
    
    if rated_games > 0:
        print(f"\n   Average rating: {numeric_ratings.mean():.1f}/100")
        print(f"   Rating range: {numeric_ratings.min():.1f} - {numeric_ratings.max():.1f}")
    
    # Price statistics
    print(f"\nPrice coverage:")
    priced_games = df['discounted_price'].notna().sum()
    free_games = (df['discounted_price'] == 0).sum()
    paid_games = ((df['discounted_price'] > 0) & df['discounted_price'].notna()).sum()
    
    print(f"   Games with price data: {priced_games} ({priced_games/len(df)*100:.1f}%)")
    print(f"   Free games: {free_games}")
    print(f"   Paid games: {paid_games}")
    
    if paid_games > 0:
        avg_price = df[df['discounted_price'] > 0]['discounted_price'].mean()
        print(f"   Average paid price: ${avg_price:.2f}")
    
    # Release date coverage
    dated_games = df['release_date'].notna().sum()
    print(f"\nRelease date coverage:")
    print(f"   Games with dates: {dated_games} ({dated_games/len(df)*100:.1f}%)")
    print(f"   Games without dates: {len(df) - dated_games} ({(len(df)-dated_games)/len(df)*100:.1f}%)")
    
    # Field completeness
    print("\nField completeness:")
    fields = ['game_title', 'rating', 'release_date', 'genres', 'platform',
              'developer', 'publisher', 'description', 'game_url']
    
    for field in fields:
        if field in df.columns:
            filled = df[field].notna().sum()
            pct = (filled / len(df)) * 100
            status = "✓" if pct >= 80 else "⚠" if pct >= 50 else "✗"
            print(f"   {status} {field:15s}: {filled:5d} / {len(df)} ({pct:5.1f}%)")

def merge_game_data(steam_path, instant_gaming_path, gog_path, output_path='Master_Dataset.csv'):
    """Main merge function - preserves all valid data"""
    print("="*70)
    print("GAME DATA MERGER - NO DATA LOSS VERSION")
    print("="*70)
    
    # 1. Load datasets
    print("\n1. Loading datasets...")
    steam_df = load_csv_safely(steam_path)
    ig_df = load_csv_safely(instant_gaming_path)
    gog_df = load_csv_safely(gog_path)
    
    if any(d is None for d in [steam_df, ig_df, gog_df]):
        print("Error: Failed to load one or more datasets")
        return None
    
    print(f"   Steam: {len(steam_df)} rows")
    print(f"   Instant Gaming: {len(ig_df)} rows")
    print(f"   GOG: {len(gog_df)} rows")
    
    # 2. Map columns to standard names
    print("\n2. Standardizing column names...")
    steam_df = map_columns_to_standard(steam_df, 'Steam')
    ig_df = map_columns_to_standard(ig_df, 'instant_gaming')
    gog_df = map_columns_to_standard(gog_df, 'GOG')
    
    # Tag source
    steam_df['data_source'] = 'Steam'
    ig_df['data_source'] = 'instant_gaming'
    gog_df['data_source'] = 'GOG'
    
    # 3. Clean and validate
    print("\n3. Cleaning and validating data...")
    steam_df = validate_and_clean_data(steam_df, 'Steam')
    ig_df = validate_and_clean_data(ig_df, 'instant_gaming')
    gog_df = validate_and_clean_data(gog_df, 'GOG')
    
    # 4. Merge all datasets
    print("\n   Merging datasets...")
    merged = pd.concat([steam_df, ig_df, gog_df], ignore_index=True, sort=False)
    merged = create_unified_schema(merged)
    
    print(f"   Combined: {len(merged)} total rows")
    
    # 5. Smart deduplication
    merged = smart_deduplicate(merged)
    
    # 6. Generate quality report
    generate_quality_report(merged)
    
    # 7. Save output
    print(f"\n5. Saving to {output_path}...")
    merged.to_csv(output_path, index=False, encoding='utf-8-sig')
    print("   ✓ Successfully saved master dataset!")
    print("="*70)
    
    return merged

if __name__ == "__main__":
    # File paths
    STEAM_CSV = "scraped_data/steam_games_detailed.csv"
    IG_CSV = "scraped_data/instant_gaming_data.csv"
    GOG_CSV = "scraped_data/gog_games_complete.csv"
    OUTPUT_CSV = "scraped_data/Master_Dataset.csv"
    
    # Run merger
    merged_data = merge_game_data(STEAM_CSV, IG_CSV, GOG_CSV, OUTPUT_CSV)