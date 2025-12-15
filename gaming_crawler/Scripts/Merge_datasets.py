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

def clean_platform(platform_value):
    """Clean platform data without standardizing"""
    if pd.isna(platform_value) or platform_value == "N/A" or platform_value == "":
        return None
    
    platform_str = str(platform_value).strip()
    
    # Handle empty or meaningless values
    if platform_str.lower() in ['', 'n/a', 'none', 'null']:
        return None
    
    # Just clean whitespace and return as-is
    return platform_str if platform_str else None

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
    renamed = df.rename(columns=mappings.get(source, {}))
    
    # Debug: Print column names after mapping
    print(f"   Columns after mapping: {list(renamed.columns)}")
    if 'platforms' in renamed.columns:
        print(f"   ✓ 'platforms' column found")
        # Show sample values
        sample_platforms = renamed['platforms'].dropna().head(3).tolist()
        if sample_platforms:
            print(f"   Sample platform values: {sample_platforms}")
    elif 'platform' in renamed.columns:
        print(f"   ✓ 'platform' column found (singular)")
        sample_platforms = renamed['platform'].dropna().head(3).tolist()
        if sample_platforms:
            print(f"   Sample platform values: {sample_platforms}")
    else:
        print(f"   ⚠️  No 'platforms' or 'platform' column found!")
    
    return renamed

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
    
    # FIXED: Process platforms - handle both 'platforms' (plural) and 'platform' (singular)
    print(f"   Processing platform data for {source}...")
    
    if 'platforms' in df.columns:
        print(f"   Found 'platforms' column (plural) - cleaning...")
        non_null_count = df['platforms'].notna().sum()
        print(f"   Non-null platform entries: {non_null_count}/{len(df)}")
        
        # Clean the platforms column without standardizing
        df['platform'] = df['platforms'].apply(clean_platform)
        df = df.drop(columns=['platforms'])
        
        # Count successful cleaning
        success_count = df['platform'].notna().sum()
        print(f"   Successfully cleaned: {success_count}/{non_null_count} entries")
        
    elif 'platform' in df.columns:
        print(f"   Found 'platform' column (singular) - cleaning...")
        non_null_count = df['platform'].notna().sum()
        print(f"   Non-null platform entries: {non_null_count}/{len(df)}")
        
        df['platform'] = df['platform'].apply(clean_platform)
        
        success_count = df['platform'].notna().sum()
        print(f"   Successfully cleaned: {success_count}/{non_null_count} entries")
    else:
        print(f"   ⚠️  No platform column found in {source} data!")
    
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
        print(f"   Dropping reference columns: {', '.join(existing)}")
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
        print("\nPlatform availability (raw values):")
        # Show top platform values
        platform_counts = df['platform'].value_counts().head(10)
        for plat, count in platform_counts.items():
            print(f"   {plat}: {count} ({count/len(df)*100:.1f}%)")
        
        # Show platform distribution by source
        print("\nPlatform coverage by source:")
        for source in df['data_source'].unique():
            source_df = df[df['data_source'] == source]
            plat_count = source_df['platform'].notna().sum()
            print(f"   {source}: {plat_count}/{len(source_df)} ({plat_count/len(source_df)*100:.1f}%)")

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