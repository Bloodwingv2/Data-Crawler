import pandas as pd
import re
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
    """Clean text from artifacts"""
    if pd.isna(text) or text == "N/A":
        return None
    text = str(text)
    for artifact in [r'\u00a0', r'\u200b', r'\\n', r'\\r', r'\\t', r'\xa0', r'&nbsp;']:
        text = re.sub(artifact, ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'&nbsp;|&amp;|&lt;|&gt;|&quot;', ' ', text)
    return text if text and not text.isspace() else None

def normalize_rating(rating_value, source):
    """Normalize ratings to 0-100 scale"""
    if pd.isna(rating_value) or rating_value in ["N/A", "--"]:
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
    """Extract numerical price"""
    if pd.isna(price_value) or price_value == "N/A":
        return None
    price_str = str(price_value).strip().lower()
    if 'free' in price_str:
        return 0.0
    price_str = price_str.replace('€', '').replace('$', '').replace('£', '').strip()
    if ',' in price_str and '.' not in price_str:
        price_str = price_str.replace(',', '.')
    elif ',' in price_str and '.' in price_str:
        price_str = price_str.replace('.', '').replace(',', '.')
    match = re.search(r'[\d]+\.?\d*', price_str)
    return float(match.group()) if match else None

def normalize_date(date_value):
    """Standardize dates to YYYY-MM-DD"""
    if pd.isna(date_value) or date_value == "N/A":
        return None
    date_str = str(date_value).strip()
    for fmt in ['%Y-%m-%d', '%d-%b-%y', '%d %b, %Y', '%b %d, %Y', '%B %d, %Y', '%d-%m-%Y', '%Y']:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    if re.match(r'^\d{4}$', date_str):
        return f"{date_str}-01-01"
    return None

def extract_genres(genres_value):
    """Clean and standardize genres"""
    if pd.isna(genres_value) or genres_value == "N/A":
        return None
    genres = [clean_text(g) for g in re.split(r'[,;|]', str(genres_value))]
    cleaned = [g.title() for g in genres if g and len(g) > 1]
    return ', '.join(cleaned) if cleaned else None

def extract_platform_from_title(title):
    """Extract platform from Instant Gaming titles like 'Game Name - PC (Steam)'"""
    if pd.isna(title):
        return None
    
    title_str = str(title)
    platforms = []
    
    # Look for platform indicators in title
    if re.search(r'\b(PC|Windows)\b', title_str, re.IGNORECASE):
        platforms.append('Windows')
    if re.search(r'\bMac\b', title_str, re.IGNORECASE):
        platforms.append('Mac')
    if re.search(r'\bLinux\b', title_str, re.IGNORECASE):
        platforms.append('Linux')
    
    return ', '.join(platforms) if platforms else None

def standardize_platform(platform_value):
    """Standardize platform names"""
    if pd.isna(platform_value) or platform_value == "N/A" or platform_value == "":
        return None
    
    platform_str = str(platform_value).lower().strip()
    platforms = []
    
    # Check for Windows - be more flexible with matching
    if any(x in platform_str for x in ['windows', 'win', 'pc', 'steam', 'ea app', 'ubisoft', 'microsoft', 'xbox']):
        platforms.append('Windows')
    
    # Check for Mac
    if any(x in platform_str for x in ['mac', 'macos', 'osx', 'os x']):
        if 'Mac' not in platforms:
            platforms.append('Mac')
    
    # Check for Linux
    if any(x in platform_str for x in ['linux', 'steamos']):
        platforms.append('Linux')
    
    result = ', '.join(platforms) if platforms else None
    return result

def calculate_discounted_price(row):
    """Calculate discounted price from original and discount"""
    if pd.notna(row.get('price')):
        return row.get('price')
    original = row.get('original_price')
    discount = row.get('discount_percentage')
    if pd.notna(original) and pd.notna(discount):
        try:
            orig = float(original)
            disc = float(str(discount).replace('%', '').replace('-', '').strip())
            return round(orig * (1 - disc / 100), 2) if disc > 0 else orig
        except (ValueError, TypeError):
            pass
    return row.get('price')

def map_columns_to_standard(df, source):
    """Map source columns to standard names"""
    mappings = {
        'Steam': {'title': 'game_title', 'rating_score': 'rating_raw', 'url': 'game_url'},
        'instant_gaming': {'title': 'game_title', 'ig_rating': 'rating_raw', 'url': 'game_url', 'genre': 'genres'},
        'GOG': {'title': 'game_title', 'rating': 'rating_raw', 'url': 'game_url'}
    }
    return df.rename(columns=mappings.get(source, {}))

def validate_and_clean_data(df, source):
    """Clean and validate data"""
    print(f"   Cleaning {source} data...")
    
    # Clean text fields
    for col in ['game_title', 'description', 'genres', 'review_text', 'developer', 'publisher']:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    
    # Normalize ratings
    if 'rating_raw' in df.columns:
        df['rating'] = df['rating_raw'].apply(lambda x: normalize_rating(x, source))
        df = df.drop(columns=['rating_raw'])
    
    # Clean discount percentage
    if 'discount_percentage' in df.columns:
        df['discount_percentage'] = df['discount_percentage'].apply(
            lambda v: float(str(v).replace('%', '').replace('-', '').strip()) if pd.notna(v) and v != "N/A" else None
        )
    
    # Normalize prices
    for col in ['original_price', 'current_price']:
        if col in df.columns:
            df[col] = df[col].apply(normalize_price)
    
    # Consolidate price columns
    if 'current_price' in df.columns:
        df['price'] = df['current_price']
        df = df.drop(columns=['current_price'])
    elif 'price' in df.columns:
        df['price'] = df['price'].apply(normalize_price)
    
    # Calculate discounted price
    df['discounted_price'] = df.apply(calculate_discounted_price, axis=1)
    if 'price' in df.columns:
        df = df.drop(columns=['price'])
    
    # Normalize dates
    if 'release_date' in df.columns:
        df['release_date'] = df['release_date'].apply(normalize_date)
    
    # Standardize genres
    if 'genres' in df.columns:
        df['genres'] = df['genres'].apply(extract_genres)
    elif 'genre' in df.columns:
        df['genres'] = df['genre'].apply(extract_genres)
        df = df.drop(columns=['genre'])
    
    # Standardize platforms - handle 'platforms' column (all sources use this name)
    if 'platforms' in df.columns:
        print(f"   Processing platforms column...")
        df['platform'] = df['platforms'].apply(standardize_platform)
        df = df.drop(columns=['platforms'])
    elif 'platform' in df.columns:
        print(f"   Processing platform column...")
        df['platform'] = df['platform'].apply(standardize_platform)
    
    # For Instant Gaming, if platform is still None, try extracting from title
    if source == 'instant_gaming' and 'platform' in df.columns and 'game_title' in df.columns:
        missing_platform = df['platform'].isna()
        if missing_platform.any():
            print(f"   Extracting platforms from titles for {missing_platform.sum()} games...")
            df.loc[missing_platform, 'platform'] = df.loc[missing_platform, 'game_title'].apply(extract_platform_from_title)
    
    # Remove invalid titles
    if 'game_title' in df.columns:
        before = len(df)
        df = df.dropna(subset=['game_title'])
        df = df[df['game_title'].str.len() > 0]
        dropped = before - len(df)
        if dropped > 0:
            print(f"   ⚠️  Dropped {dropped} rows with invalid titles")
    
    # Remove duplicate URLs
    if 'game_url' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['game_url'], keep='first')
        if before - len(df) > 0:
            print(f"   ⚠️  Dropped {before - len(df)} duplicate URLs")
    
    return df

def remove_reference_columns(df):
    """Remove reference/internal columns"""
    cols_to_drop = ['downloaded_images', 'downloaded_videos', 'header_image', 'screenshots', 
                    'videos', 'screenshot', 'video_url', 'product_id', 'status_tag', 
                    'stock_status', 'categories', 'background_image', 'clip']
    existing = [col for col in cols_to_drop if col in df.columns]
    if existing:
        print(f"   Dropping: {', '.join(existing)}")
        df = df.drop(columns=existing)
    return df

def create_unified_schema(df):
    """Create unified schema"""
    standard = ['data_source', 'game_title', 'release_date', 'rating', 'rating_percentage', 
                'review_count', 'discounted_price', 'original_price', 'discount_percentage', 
                'genres', 'platform', 'developer', 'publisher', 'description', 'review_text', 'game_url']
    for col in standard:
        if col not in df.columns:
            df[col] = None
    keep_extra = ['user_tags', 'game_features', 'editions']
    existing = [c for c in standard if c in df.columns]
    extra = [c for c in df.columns if c not in existing and c in keep_extra]
    return df[existing + extra]

def generate_quality_report(df):
    """Generate quality report"""
    print("\n" + "="*60)
    print("DATA QUALITY REPORT")
    print("="*60)
    print(f"\nTotal games: {len(df)}")
    
    print("\nBy source:")
    for source in df['data_source'].unique():
        count = len(df[df['data_source'] == source])
        print(f"   {source}: {count} ({count/len(df)*100:.1f}%)")
    
    if 'rating' in df.columns and df['rating'].notna().any():
        print("\nRating statistics:")
        print(f"   Average: {df['rating'].mean():.1f}/100")
        print(f"   Range: {df['rating'].min():.1f} - {df['rating'].max():.1f}")
        for source in df['data_source'].unique():
            ratings = df[df['data_source'] == source]['rating']
            if ratings.notna().any():
                print(f"   {source}: {ratings.mean():.1f}/100 (n={ratings.notna().sum()})")
    
    if 'discounted_price' in df.columns:
        print("\nPrice statistics:")
        paid = df[df['discounted_price'] > 0]
        free = df[df['discounted_price'] == 0]
        print(f"   Free: {len(free)} | Paid: {len(paid)}")
        if len(paid) > 0:
            print(f"   Avg: ${paid['discounted_price'].mean():.2f} | Median: ${paid['discounted_price'].median():.2f}")
        discounted = df[(df['discount_percentage'].notna()) & (df['discount_percentage'] > 0)]
        if len(discounted) > 0:
            print(f"   On discount: {len(discounted)} ({len(discounted)/len(df)*100:.1f}%) - Avg: {discounted['discount_percentage'].mean():.1f}%")
    
    print("\nData completeness:")
    for field in ['game_title', 'rating', 'release_date', 'genres', 'platform', 'game_url']:
        if field in df.columns:
            filled = df[field].notna().sum()
            pct = filled/len(df)*100
            status = "✓" if pct >= 90 else "⚠️" if pct >= 70 else "✗"
            print(f"   {status} {field}: {filled}/{len(df)} ({pct:.1f}%)")
    
    if 'platform' in df.columns:
        print("\nPlatform availability:")
        for plat in ['Windows', 'Mac', 'Linux']:
            count = df[df['platform'].fillna('').str.contains(plat, case=False)].shape[0]
            print(f"   {plat}: {count} ({count/len(df)*100:.1f}%)")

def merge_game_data(steam_path, instant_gaming_path, gog_path, output_path='Master_Dataset.csv'):
    """Merge and normalize gaming datasets"""
    print("="*60)
    print("GAME DATA MERGER")
    print("="*60)
    
    # Load datasets
    print("\n1. Loading...")
    steam_df = load_csv_safely(steam_path)
    ig_df = load_csv_safely(instant_gaming_path)
    gog_df = load_csv_safely(gog_path)
    
    for name, df in {'Steam': steam_df, 'instant_gaming': ig_df, 'GOG': gog_df}.items():
        if df is not None:
            print(f"   ✓ {name}: {len(df)} rows")
        else:
            print(f"   ✗ {name}: Failed")
            return None
    
    # Process datasets
    print("\n2. Standardizing columns...")
    steam_df = map_columns_to_standard(steam_df, 'Steam')
    ig_df = map_columns_to_standard(ig_df, 'instant_gaming')
    gog_df = map_columns_to_standard(gog_df, 'GOG')
    
    print("\n3. Adding sources...")
    steam_df['data_source'] = 'Steam'
    ig_df['data_source'] = 'instant_gaming'
    gog_df['data_source'] = 'GOG'
    
    print("\n4. Cleaning...")
    steam_df = validate_and_clean_data(steam_df, 'Steam')
    ig_df = validate_and_clean_data(ig_df, 'instant_gaming')
    gog_df = validate_and_clean_data(gog_df, 'GOG')
    
    steam_df = remove_reference_columns(steam_df)
    ig_df = remove_reference_columns(ig_df)
    gog_df = remove_reference_columns(gog_df)
    
    # Merge
    print("\n5. Merging...")
    merged = pd.concat([steam_df, ig_df, gog_df], ignore_index=True, sort=False)
    print(f"   ✓ {len(merged)} rows")
    
    merged = create_unified_schema(merged)
    
    # Handle missing ratings
    print("\n6. Filtering by rating...")
    for source in merged['data_source'].unique():
        source_games = merged[merged['data_source'] == source]
        missing = source_games['rating'].isna().sum()
        if len(source_games) > 0:
            print(f"   {source}: {missing}/{len(source_games)} missing ({missing/len(source_games)*100:.1f}%)")
    
    before = len(merged)
    merged = merged.dropna(subset=['rating'])
    print(f"   ⚠️  Dropped {before - len(merged)} games without ratings")
    
    # Deduplicate
    print("\n7. Deduplicating...")
    before = len(merged)
    merged['temp'] = merged['game_title'].str.lower().str.strip()
    merged = merged.sort_values('rating', ascending=False)
    merged = merged.drop_duplicates(subset=['temp'], keep='first')
    merged = merged.drop(columns=['temp'])
    if before - len(merged) > 0:
        print(f"   ⚠️  Dropped {before - len(merged)} duplicates")
    
    generate_quality_report(merged)
    
    # Save
    print(f"\n8. Saving to {output_path}...")
    merged.to_csv(output_path, index=False, encoding='utf-8-sig')
    print("   ✓ Saved!")
    print("\n" + "="*60)
    print("COMPLETE!")
    print("="*60)
    
    return merged

if __name__ == "__main__":
    STEAM_CSV = "scraped_data/steam_games_detailed.csv" 
    IG_CSV = "scraped_data/instant_gaming_backup.csv"
    GOG_CSV = "scraped_data/gog_games_complete.csv"
    OUTPUT_CSV = "scraped_data/Master_Dataset.csv"
    
    merged = merge_game_data(STEAM_CSV, IG_CSV, GOG_CSV, OUTPUT_CSV)
    
    if merged is not None:
        print(f"\n📊 Sample:")
        print(merged[['data_source', 'game_title', 'rating', 'discounted_price', 'platform']].head(15))
        print(f"\n📋 Columns ({len(merged.columns)}):")
        for i, col in enumerate(merged.columns, 1):
            print(f"{i}. {col}")