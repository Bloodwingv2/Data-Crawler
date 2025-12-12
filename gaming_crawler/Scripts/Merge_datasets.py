import pandas as pd

def load_csv_safely(filepath, encoding='utf-8'):
    """
    Load CSV with fallback encoding options
    """
    try:
        return pd.read_csv(filepath, encoding=encoding)
    except UnicodeDecodeError:
        print(f"UTF-8 failed for {filepath}, trying latin-1...")
        return pd.read_csv(filepath, encoding='latin-1')
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def standardize_columns(df, source_name):
    """
    Add source column and standardize common fields
    """
    df = df.copy()
    df['data_source'] = source_name
    
    # Standardize title column names
    if 'title' in df.columns:
        df['game_title'] = df['title']
    
    return df

def merge_game_data(steam_path, rawg_path, gog_path, output_path='merged_games.csv'):
    """
    Merge three gaming datasets from Steam, RAWG, and GOG
    
    Parameters:
    - steam_path: Path to Steam CSV file
    - rawg_path: Path to RAWG CSV file
    - gog_path: Path to GOG CSV file
    - output_path: Path for merged output CSV
    """
    
    print("="*60)
    print("GAME DATA MERGER")
    print("="*60)
    
    # Load datasets
    print("\n1. Loading datasets...")
    steam_df = load_csv_safely(steam_path)
    rawg_df = load_csv_safely(rawg_path)
    gog_df = load_csv_safely(gog_path)
    
    # Check if all loaded successfully
    datasets = {
        'Steam': steam_df,
        'RAWG': rawg_df,
        'GOG': gog_df
    }
    
    for name, df in datasets.items():
        if df is not None:
            print(f"   ✓ {name}: {len(df)} rows, {len(df.columns)} columns")
        else:
            print(f"   ✗ {name}: Failed to load")
            return None
    
    # Add source identifiers
    print("\n2. Adding source identifiers...")
    steam_df = standardize_columns(steam_df, 'Steam')
    rawg_df = standardize_columns(rawg_df, 'RAWG')
    gog_df = standardize_columns(gog_df, 'GOG')
    
    # Create unique identifier for potential matching
    print("\n3. Creating merge keys...")
    for df in [steam_df, rawg_df, gog_df]:
        if 'game_title' in df.columns:
            df['title_normalized'] = df['game_title'].str.lower().str.strip()
    
    # Merge all datasets
    print("\n4. Merging datasets...")
    
    # Outer merge to keep ALL data from all sources
    merged_df = pd.concat([steam_df, rawg_df, gog_df], 
                          ignore_index=True, 
                          sort=False)
    
    print(f"   ✓ Merged dataset: {len(merged_df)} rows, {len(merged_df.columns)} columns")
    
    # Reorder columns for better readability
    print("\n5. Organizing columns...")
    
    # Priority columns first
    priority_cols = [
        'data_source', 'game_title', 'title', 'release_date', 
        'price', 'original_price', 'discount_percentage',
        'review_summary', 'rating', 'metacritic_score'
    ]
    
    # Get columns that exist
    existing_priority = [col for col in priority_cols if col in merged_df.columns]
    other_cols = [col for col in merged_df.columns if col not in existing_priority]
    
    # Reorder
    merged_df = merged_df[existing_priority + other_cols]
    
    # Generate summary statistics
    print("\n6. Summary Statistics:")
    print(f"   Total games: {len(merged_df)}")
    print(f"   From Steam: {len(merged_df[merged_df['data_source'] == 'Steam'])}")
    print(f"   From RAWG: {len(merged_df[merged_df['data_source'] == 'RAWG'])}")
    print(f"   From GOG: {len(merged_df[merged_df['data_source'] == 'GOG'])}")
    print(f"   Total columns: {len(merged_df.columns)}")
    
    # Check for missing data
    print("\n7. Data Completeness:")
    key_columns = ['game_title', 'title', 'release_date', 'url']
    for col in key_columns:
        if col in merged_df.columns:
            missing = merged_df[col].isna().sum()
            pct = (missing / len(merged_df)) * 100
            print(f"   {col}: {missing} missing ({pct:.1f}%)")
    
    # Save merged dataset
    print(f"\n8. Saving to {output_path}...")
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"   ✓ Saved successfully!")
    
    # Create a summary report
    #summary_path = output_path.replace('.csv', '_summary.txt')
    #with open(summary_path, 'w', encoding='utf-8') as f:
        #f.write("GAME DATA MERGE SUMMARY\n")
        #f.write("="*60 + "\n\n")
        #f.write(f"Total games merged: {len(merged_df)}\n")
        #f.write(f"From Steam: {len(merged_df[merged_df['data_source'] == 'Steam'])}\n")
        #f.write(f"From RAWG: {len(merged_df[merged_df['data_source'] == 'RAWG'])}\n")
        #f.write(f"From GOG: {len(merged_df[merged_df['data_source'] == 'GOG'])}\n")
        #f.write(f"\nTotal columns: {len(merged_df.columns)}\n")
        #f.write(f"\nColumn list:\n")
        #for i, col in enumerate(merged_df.columns, 1):
            #f.write(f"{i}. {col}\n")
    
    #print(f"   ✓ Summary saved to {summary_path}")
    
    print("\n" + "="*60)
    print("MERGE COMPLETE!")
    print("="*60)
    
    return merged_df

# Example usage
if __name__ == "__main__":
    # Update these paths to your actual file locations
    STEAM_CSV = "scraped_data/steam_games_detailed.csv" 
    RAWG_CSV = "scraped_data/rawg_games_20251212_150342.csv"
    GOG_CSV = "scraped_data/gog_games_detailed.csv"
    OUTPUT_CSV = "scraped_data/Master_Dataset.csv"
    
    # Run the merge
    merged_data = merge_game_data(
        steam_path=STEAM_CSV,
        rawg_path=RAWG_CSV,
        gog_path=GOG_CSV,
        output_path=OUTPUT_CSV
    )
    
    if merged_data is not None:
        print(f"\nFirst few rows of merged data:")
        print(merged_data.head())
        
        # Optional: Show column names
        print(f"\nAll columns in merged dataset:")
        for i, col in enumerate(merged_data.columns, 1):
            print(f"{i}. {col}")