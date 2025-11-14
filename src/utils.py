import pandas as pd
import re

DATE_PATTERNS = [
    # Standard numeric formats
    r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b',     # MM/DD/YYYY, MM-DD-YYYY
    r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b',       # YYYY/MM/DD, YYYY-MM-DD
    
    # Full month names
    r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s*\d{1,2},?\s*\d{4}\b',  # Month DD, YYYY
    r'\b\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{2,4}\b',  # DD Month YYYY
    
    # "For the year ended..." formats
    r'For\s+the\s+.*?ended\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*\d{1,2},?\s*\d{4}',
    
    # Abbreviated months
    r'\b\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{2,4}\b',  # DD Month YYYY
    r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2},?\s+\d{2,4}\b',  # Month DD, YYYY
]

def create_df_from_html_file(file_path):
    try:
        df = pd.read_html(file_path)
    except ValueError as e:
        if "No tables found" in str(e):
            print("No HTML tables found in the file")
            df = None
        else:
            print(f"ValueError: {e}")
            df = None
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        df = None
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = None
    return df

def create_df_from_csv(file_path):
    try:
        df = pd.read_csv(file_path, quotechar='"', low_memory=False)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        df = None
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = None
    return df

def make_new_dir(dir_path):
    import os
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def column_has_numbers(series):
    numeric = pd.to_numeric(series, errors='coerce')
    return numeric.notna().any()

def is_date_in_row(row):
    for col in row:
        if pd.isna(col):
            continue
            
        col_str = str(col).replace('  ', ' ')
        
        for pattern in DATE_PATTERNS:
            if re.search(pattern, col_str, re.IGNORECASE):
                return True
    
    return False

def remove_empty_columns_from_df(df):
    columns_to_keep = []
    
    for col in df.columns:
        # Convert column to string and clean it
        col_values = df[col].astype(str).str.strip()

        # Remove invisible characters like zero-width spaces
        col_values = col_values.str.replace('​', '').str.replace('\u200b', '')
        
        # Keep column if it has any non-empty values
        if not col_values.eq('').all() and not col_values.eq('nan').all():
            columns_to_keep.append(col)
    
    return df[columns_to_keep]

def regex_format_date(text):
    if not isinstance(text, str):
        return None

    # Extraction patterns with capture groups
    extraction_patterns = [
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",
        r"For\s+the\s+.*?ended\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",
        r".*?(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",
        r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{2,4})\b",  # DD Month YYYY
    ]

    
    for pattern in extraction_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) >= 3:
                # Handle different group orders
                if groups[0].isdigit():  # DD Month YYYY format
                    day, month, year = groups[:3]
                    return f"{month} {day}, {year}"
                else:  # Month DD, YYYY format
                    month, day, year = groups[:3]
                    return f"{month} {day}, {year}"
    
    return None

def regex_format_number(text):
    if not isinstance(text, str):
        return None
    
    # $(21.94) - dollar sign with parentheses
    # $46,624 - dollar sign with commas
    # (631) - just parentheses
    # More specific patterns for each format
    pattern = r'(?:\$\(([0-9,]+(?:\.[0-9]+)?)\)|\$([0-9,]+(?:\.[0-9]+)?)|^\(([0-9,]+(?:\.[0-9]+)?)\)$|^([0-9,]+(?:\.[0-9]+)?)$)'
    
    match = re.search(pattern, text)
    
    if match:
        # Get the matched number (one of the groups will be non-None)
        number_str = next((g for g in match.groups() if g), None)
        
        if number_str:
            number_str = number_str.replace(',', '')
            try:
                number = float(number_str)
                
                # Check if it's negative (has parentheses)
                if '(' in text and ')' in text:
                    number = -number
                    
                return number
            except ValueError:
                pass
    
    return None

def to_snake_case(text):
    """Convert text to snake_case format"""
    # Remove parentheses and their contents
    text = re.sub(r'\([^)]*\)', '', text)
    # Remove apostrophes (both regular and Unicode variants)
    text = re.sub(r"['']", '', text)  # Handles both ' and '
    # Replace spaces, hyphens, and other separators with underscores
    text = re.sub(r'[-\s]+', '_', text)
    # Insert underscore before uppercase letters that follow lowercase letters
    text = re.sub(r'([a-z])([A-Z])', r'\1_\2', text)
    # Convert to lowercase
    text = text.lower()
    # Remove multiple underscores
    text = re.sub(r'_+', '_', text)
    # Remove leading/trailing underscores
    text = text.strip('_')
    return text