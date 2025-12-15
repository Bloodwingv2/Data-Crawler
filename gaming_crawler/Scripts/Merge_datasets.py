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
    if pd.isna(rating_value) or rating_value in ["N/A", "--", ""]:
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
    
    # Handle European format (comma as decimal)
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
    
    for fmt in ['%d-%b-%y', '%Y-%m-%d', '%d %b, %Y', '%b %d, %Y', '%B %d, %Y', '%d-%m-%Y', '%Y']:
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
    """Clean platform data"""
    if pd.isna(platform_value) or platform_value == "N/A" or platform_value == "":
        return None
    platform_str = str(platform_value).strip()
    if platform_str.lower() in ['', 'n/a', 'none', 'null']:
        return None
    return platform_str

def calculate_discounted_price(row):
    """Calculate discounted price from original and discount"""
    for col in ['discounted_price', 'current_price', 'price']:
        if col in row and pd.notna(row.get(col)):
            return row.get(col)
    
    original = row.get('original_price')
    discount = row.get('discount_percentage')
    if pd.notna(original) and pd.notna(discount):
        try:
            orig = float(original)
            disc = float(str(discount).replace('%', '').replace('-', '').strip())
            return round(orig * (1 - disc / 100), 2) if disc > 0 else orig
        except (ValueError, TypeError):
            pass
    return None

def map_columns_to_standard(df, source):
    """Map source columns to standard names"""
    mappings = {
        'Steam': {
            'title': 'game_title',
            'rating_score': 'rating_raw',
            'url': 'game_url',
            'original_price': 'original_price_raw',
            'price': 'current_price_raw',
            'review_count': 'review_count'
        },
        'instant_gaming': {
            'title': 'game_title',
            'ig_rating': 'rating_raw',
            'url': 'game_url',
            'genre': 'genres',
            'current_price': 'current_price_raw',
            'platforms': 'platforms'
        },
        'GOG': {
            'title': 'game_title',
            'rating': 'rating_raw',
            'rating_count': 'review_count',
            'url': 'game_url',
            'price': 'current_price_raw',
            'status_tag': 'release_status'
        }
    }
    return df.rename(columns=mappings.get(source, {}))

def validate_and_clean_data(df, source):
    """Clean and validate data"""
    print(f"   Cleaning {source} data...")
    
    # Text cleaning
    for col in ['game_title', 'description', 'genres', 'developer', 'publisher']:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    
    # Rating normalization
    for col in ['rating_raw', 'rating_score', 'rating']:
        if col in df.columns:
            df['rating'] = df[col].apply(lambda x: normalize_rating(x, source))
            df = df.drop(columns=[col])
            break
            
    # Price normalization
    price_cols = {'original_price': 'original_price', 'original_price_raw': 'original_price', 
                  'current_price': 'current_price_temp', 'current_price_raw': 'current_price_temp', 
                  'price': 'current_price_temp'}
    
    for old, new in price_cols.items():
        if old in df.columns:
            df[new] = df[old].apply(normalize_price)
            if old != new:
                df = df.drop(columns=[old])
                
    # Discount calculation
    if 'discount_percentage' in df.columns:
        df['discount_percentage'] = df['discount_percentage'].apply(
            lambda v: float(str(v).replace('%', '').replace('-', '').strip()) 
            if pd.notna(v) and v != "N/A" else None
        )
        
    df['discounted_price'] = df.apply(calculate_discounted_price, axis=1)
    if 'current_price_temp' in df.columns:
        df = df.drop(columns=['current_price_temp'])
        
    # Date and Platform cleaning
    if 'release_date' in df.columns:
        df['release_date'] = df['release_date'].apply(normalize_date)
        
    if 'genres' in df.columns:
        df['genres'] = df['genres'].apply(extract_genres)
        
    # Standardize Platform to singular column
    if 'platforms' in df.columns:
        df['platform'] = df['platforms'].apply(clean_platform)
        df = df.drop(columns=['platforms'])
    elif 'platform' in df.columns:
        df['platform'] = df['platform'].apply(clean_platform)
    else:
        df['platform'] = None

    # Drop duplicates and invalid titles
    if 'game_title' in df.columns:
        df = df.dropna(subset=['game_title'])
        df = df[df['game_title'].str.len() > 0]
        
    if 'game_url' in df.columns:
        df = df.drop_duplicates(subset=['game_url'], keep='first')
        
    return df

def apply_business_rules(df):
    """Apply specific user-defined business rules for gaps and drops"""
    print("\n" + "="*60)
    print("APPLYING BUSINESS RULES")
    print("="*60)
    
    # Rule 1: Games with no prices -> mark as free (0.0)
    # We assume 'discounted_price' is the final price column.
    missing_price = df['discounted_price'].isna().sum()
    df['discounted_price'] = df['discounted_price'].fillna(0.0)
    df['original_price'] = df['original_price'].fillna(0.0)
    print(f"   ✓ Rule 1: Marked {missing_price} games with no price data as 'Free' (0.0)")

    # Rule 2: Games with no reviews -> mark as 'Not yet rated'
    # "No reviews" implies review_count is NaN or 0.
    # We must convert 'rating' to object type to hold strings.
    df['rating'] = df['rating'].astype('object')
    
    # Identify rows with no reviews
    no_reviews_mask = (df['review_count'].isna()) | (df['review_count'] == 0)
    
    # Apply label
    df.loc[no_reviews_mask, 'rating'] = "Not yet rated"
    print(f"   ✓ Rule 2: Marked {no_reviews_mask.sum()} games with no reviews as 'Not yet rated'")

    # Rule 3: Games with no devs AND publishers -> drop them
    before_drop = len(df)
    df = df.dropna(subset=['developer', 'publisher'], how='all')
    dropped_count = before_drop - len(df)
    print(f"   ✓ Rule 3: Dropped {dropped_count} games with missing Developer AND Publisher")
    
    return df

def create_unified_schema(df):
    """Create unified schema with all essential columns"""
    standard = [
        'data_source', 'game_title', 'release_date', 'rating', 'review_count',
        'discounted_price', 'original_price', 'discount_percentage', 
        'genres', 'platform', 'developer', 'publisher', 'description', 
        'release_status', 'game_url'
    ]
    
    for col in standard:
        if col not in df.columns:
            df[col] = None
            
    return df[[c for c in standard if c in df.columns]]

def generate_quality_report(df):
    """Generate comprehensive quality report handling mixed types"""
    print("\n" + "="*60)
    print("DATA QUALITY REPORT")
    print("="*60)
    print(f"\nTotal games: {len(df)}")
    
    # Convert rating to numeric for stats, coercing "Not yet rated" to NaN
    numeric_ratings = pd.to_numeric(df['rating'], errors='coerce')
    
    if numeric_ratings.notna().any():
        print("\nRating statistics (excluding 'Not yet rated'):")
        print(f"   Average: {numeric_ratings.mean():.1f}/100")
        print(f"   Range: {numeric_ratings.min():.1f} - {numeric_ratings.max():.1f}")
        
    print(f"\n   'Not yet rated' count: {len(df[df['rating'] == 'Not yet rated'])}")
    
    if 'discounted_price' in df.columns:
        print("\nPrice statistics:")
        paid = df[df['discounted_price'] > 0]
        free = df[df['discounted_price'] == 0]
        print(f"   Free: {len(free)} | Paid: {len(paid)}")
        if len(paid) > 0:
            print(f"   Avg Paid Price: ${paid['discounted_price'].mean():.2f}")

    print("\nData completeness:")
    essential_fields = ['game_title', 'rating', 'release_date', 'genres', 'platform', 
                        'developer', 'publisher', 'game_url']
    for field in essential_fields:
        if field in df.columns:
            filled = df[field].notna().sum()
            pct = filled/len(df)*100
            status = "✓" if pct >= 90 else "⚠️" if pct >= 70 else "✗"
            print(f"   {status} {field}: {filled}/{len(df)} ({pct:.1f}%)")

def merge_game_data(steam_path, instant_gaming_path, gog_path, output_path='Master_Dataset.csv'):
    """Merge and normalize gaming datasets"""
    print("="*60)
    print("GAME DATA MERGER WITH BUSINESS RULES")
    print("="*60)
    
    # 1. Load
    print("\n1. Loading datasets...")
    steam_df = load_csv_safely(steam_path)
    ig_df = load_csv_safely(instant_gaming_path)
    gog_df = load_csv_safely(gog_path)
    
    if any(d is None for d in [steam_df, ig_df, gog_df]):
        return None
    
    # 2. Map & Tag
    steam_df = map_columns_to_standard(steam_df, 'Steam')
    ig_df = map_columns_to_standard(ig_df, 'instant_gaming')
    gog_df = map_columns_to_standard(gog_df, 'GOG')
    
    steam_df['data_source'] = 'Steam'
    ig_df['data_source'] = 'instant_gaming'
    gog_df['data_source'] = 'GOG'
    
    # 3. Clean
    print("\n2. Cleaning individual datasets...")
    steam_df = validate_and_clean_data(steam_df, 'Steam')
    ig_df = validate_and_clean_data(ig_df, 'instant_gaming')
    gog_df = validate_and_clean_data(gog_df, 'GOG')
    
    # 4. Merge
    print("\n3. Merging datasets...")
    merged = pd.concat([steam_df, ig_df, gog_df], ignore_index=True, sort=False)
    merged = create_unified_schema(merged)
    
    # 5. Apply Business Rules (The specific user request)
    merged = apply_business_rules(merged)
    
    # 6. Deduplicate
    print("\n4. Deduplicating...")
    before = len(merged)
    # Convert rating to numeric specifically for sorting logic (best rated first)
    # We temporarily use a numeric column for sorting
    merged['rating_sort'] = pd.to_numeric(merged['rating'], errors='coerce').fillna(-1)
    
    merged['temp_title'] = merged['game_title'].str.lower().str.strip()
    merged = merged.sort_values('rating_sort', ascending=False)
    merged = merged.drop_duplicates(subset=['temp_title'], keep='first')
    
    merged = merged.drop(columns=['temp_title', 'rating_sort'])
    
    if before - len(merged) > 0:
        print(f"   ✓ Dropped {before - len(merged)} duplicates")
    
    # 7. Final Report & Save
    generate_quality_report(merged)
    
    print(f"\n5. Saving to {output_path}...")
    merged.to_csv(output_path, index=False, encoding='utf-8-sig')
    print("   ✓ Saved successfully!")
    
    return merged

if __name__ == "__main__":
    # Update paths as needed
    STEAM_CSV = "scraped_data/steam_games_detailed.csv" 
    IG_CSV = "scraped_data/instant_gaming_data.csv"
    GOG_CSV = "scraped_data/gog_games_complete.csv"
    OUTPUT_CSV = "scraped_data/Master_Dataset.csv"
    
    merged = merge_game_data(STEAM_CSV, IG_CSV, GOG_CSV, OUTPUT_CSV)