import pandas as pd
import re
import unicodedata
from datetime import datetime

# ==========================================
# UTF-8 RECOVERY + CLEANING FUNCTION
# ==========================================
def fix_encoding(text):
    if not isinstance(text, str):
        return text
    
    if pd.isna(text) or text.strip() == '':
        return text

    # Step 1: Unicode normalization FIRST (don't try latin1 conversion)
    text = unicodedata.normalize("NFKC", text)

    # Step 2: Regex cleanup of common mojibake patterns
    replacements = {
        r"â€™": "'",
        r"â€˜": "'",
        r"â€œ": '"',
        r"â€\x9d": '"',
        r"â€": "–",
        r"â€": "—",
        r"â„¢": "™",
        r"Â®": "®",
        r"Â©": "©",
        r"Â ": " ",
        r"Â": "",
    }

    for pattern, repl in replacements.items():
        text = re.sub(pattern, repl, text)

    # Step 3: Remove control chars (but keep newlines if needed)
    text = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]", "", text)

    # Step 4: Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text).strip()

    return text


# ==========================================
# MAIN PIPELINE
# ==========================================
def main():
    INPUT_FILE = "scraped_data/Master_Dataset.csv"
    OUTPUT_FILE = "scraped_data/Master_Dataset_cleaned.csv"

    # Read with explicit UTF-8 encoding
    df = pd.read_csv(INPUT_FILE, encoding='utf-8', encoding_errors='replace')

    # --------------------------------------
    # Fix encoding for text columns (BEFORE filling missing data)
    # --------------------------------------
    object_cols = df.select_dtypes(include="object").columns
    for col in object_cols:
        df[col] = df[col].apply(fix_encoding)

    # --------------------------------------
    # Clean & convert review_count
    # --------------------------------------
    if "review_count" in df.columns:
        df["review_count"] = (
            df["review_count"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.extract(r"(\d+)", expand=False)
            .astype(float)
        )

    # --------------------------------------
    # Report missing data BEFORE dropping
    # --------------------------------------
    print("\nMissing data summary BEFORE cleaning:")
    missing_before = df.isna().sum()
    if missing_before.sum() > 0:
        print(missing_before[missing_before > 0])
        print(f"\nTotal rows before: {len(df)}")
    
    # --------------------------------------
    # Drop rows with missing CRITICAL fields
    # (developers, publishers, ratings)
    # --------------------------------------
    rows_before = len(df)
    critical_columns = []
    
    # Check which critical columns exist
    if 'developers' in df.columns:
        critical_columns.append('developers')
    if 'publishers' in df.columns:
        critical_columns.append('publishers')
    if 'rating' in df.columns:
        critical_columns.append('rating')
    elif 'ratings' in df.columns:
        critical_columns.append('ratings')
    if 'release_date' in df.columns:
        critical_columns.append('release_date')
    
    if critical_columns:
        df = df.dropna(subset=critical_columns)
        rows_after = len(df)
        rows_dropped = rows_before - rows_after
        
        print(f"\n✓ Dropped {rows_dropped} rows missing critical fields: {', '.join(critical_columns)}")
        print(f"✓ Remaining rows: {rows_after} ({(rows_after/rows_before*100):.1f}% retained)")
    else:
        print("\n⚠ Warning: No critical columns found (developers, publishers, rating/ratings)")
    
    # --------------------------------------
    # Fill remaining missing numeric data with 0
    # (for non-critical fields like review_count, prices, etc.)
    # --------------------------------------
    numeric_cols = df.select_dtypes(include="number").columns
    for col in numeric_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna(0)
    
    # --------------------------------------
    # Fill remaining missing text data with 'Unknown'
    # (for non-critical fields)
    # --------------------------------------
    object_cols = df.select_dtypes(include="object").columns
    for col in object_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna('Unknown')
    
    # --------------------------------------
    # Drop rows with MORE THAN 3 'Unknown' values
    # --------------------------------------
    rows_before_unknown = len(df)
    
    # Count 'Unknown' values in each row (only in object columns)
    unknown_counts = (df[object_cols] == 'Unknown').sum(axis=1)
    
    # Keep only rows with 3 or fewer 'Unknown' values
    df = df[unknown_counts <= 3]
    
    rows_after_unknown = len(df)
    unknown_dropped = rows_before_unknown - rows_after_unknown
    
    print(f"\n✓ Dropped {unknown_dropped} rows with more than 3 'Unknown' fields")
    print(f"✓ Remaining rows: {rows_after_unknown} ({(rows_after_unknown/rows_before*100):.1f}% of original data retained)")
    
    # --------------------------------------
    # Drop rows where BOTH developer AND publisher are 'Unknown'
    # --------------------------------------
    rows_before_dev_pub = len(df)
    
    if 'developer' in df.columns and 'publisher' in df.columns:
        # Drop if both are 'Unknown'
        both_unknown_mask = (df['developer'] == 'Unknown') & (df['publisher'] == 'Unknown')
        df = df[~both_unknown_mask]
        
        rows_after_dev_pub = len(df)
        dev_pub_dropped = rows_before_dev_pub - rows_after_dev_pub
        
        print(f"\n✓ Dropped {dev_pub_dropped} rows where both developer AND publisher are 'Unknown'")
        print(f"✓ Remaining rows: {rows_after_dev_pub} ({(rows_after_dev_pub/rows_before*100):.1f}% of original data retained)")
    else:
        print("\n⚠ Warning: Could not check developer/publisher - columns not found")

    # --------------------------------------
    # Recalculate discount_percentage
    # --------------------------------------
    if {"original_price", "discounted_price", "discount_percentage"}.issubset(df.columns):
        mask = (df["original_price"] > 0) & (df["discounted_price"] > 0)
        df.loc[mask, "discount_percentage"] = (
            (df.loc[mask, "original_price"] - df.loc[mask, "discounted_price"])
            / df.loc[mask, "original_price"] * 100
        ).round(2)
        
        # Ensure discount percentage is non-negative
        df.loc[df["discount_percentage"] < 0, "discount_percentage"] = 0

    # --------------------------------------
    # Infer release_status
    # --------------------------------------
    if "release_date" in df.columns:
        today = pd.Timestamp.today()
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        df["release_status"] = df["release_date"].apply(
            lambda x: "Released" if pd.notna(x) and x <= today else 
                     ("Upcoming" if pd.notna(x) else "Unknown")
        )

    # --------------------------------------
    # Save final dataset with explicit UTF-8
    # --------------------------------------
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"\n✓ Final UTF-8 cleaned dataset saved as: {OUTPUT_FILE}")
    print(f"✓ Total rows: {len(df)}")
    print(f"✓ Total columns: {len(df.columns)}")
    
    # Show data quality summary
    print(f"\n📊 Data Quality Summary:")
    unknown_counts_final = (df[object_cols] == 'Unknown').sum()
    if unknown_counts_final.sum() > 0:
        print(f"Remaining 'Unknown' values by column:")
        print(unknown_counts_final[unknown_counts_final > 0])
    else:
        print("✓ No 'Unknown' values in dataset")
    
    zero_counts = (df[numeric_cols] == 0).sum()
    if zero_counts.sum() > 0:
        print(f"\nZero values by column:")
        print(zero_counts[zero_counts > 0])


if __name__ == "__main__":
    main()