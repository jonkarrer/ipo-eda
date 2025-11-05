import pandas as pd
import re


def has_numbers(series):
    numeric = pd.to_numeric(series, errors='coerce')
    return numeric.notna().any()

def is_a_number(text):
    try:
        float(text)
        return True
    except ValueError:
        return False

def regex_date(text):
    if not isinstance(text, str):
        return None
    
    # More flexible pattern that handles various spacing and formats
    patterns = [
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",  # Month Day, Year or Month Day Year
        r"For\s+the\s+.*?ended\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",  # "For the year ended..."
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2})(?:st|nd|rd|th)?,?\s*(\d{4})"  # Month 1st, Year
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)  # Case insensitive
        if match:
            if len(match.groups()) == 3:
                month, day, year = match.groups()
                return f"{month} {day}, {year}"
    
    return None

def regex_number(text):
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

def locate_value_and_date(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        return None 
    
    new_rows = []
    recent_date = None
    
    for row_idx, row in df.iterrows():
        row_name = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ""
        
        # Check if this entire row contains a date (common in financial statements)
        row_as_string = ' '.join([str(cell) for cell in row if not pd.isna(cell)])
        date_in_row = regex_date(row_as_string)
        
        if date_in_row:
            recent_date = row_as_string
            print(f"Found date section: {recent_date}")
            continue  # Skip to next row, this row is just a date header
        
        # Process each cell in the row for numbers
        for col_idx, cell_value in enumerate(row):
            if pd.isna(cell_value):
                continue
            
            # Skip the first column (it's the row name/symbol)
            if col_idx == 0:
                continue
                
            # Check if it's a number
            number_value = None
            if isinstance(cell_value, str):
                number_value = regex_number(cell_value)
            elif isinstance(cell_value, (int, float)):
                number_value = cell_value
            
            # Add row if we have date and number
            if recent_date is not None and number_value is not None:
                new_rows.append({
                    'symbol': row_name,
                    'context_date': recent_date,
                    'date': regex_date(recent_date),
                    'value': number_value
                })
                # print(f"Added: {row_name}, {recent_date}, {number_value}")
    
    return pd.DataFrame(new_rows)





def clean_out_columns_and_rows(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        return None 
    for i,col in enumerate(df.columns):
        if i == 0:
            continue
        df[col].replace('$', pd.NA, inplace=True)
        df[col].replace('$—', pd.NA, inplace=True)
        df[col].replace('—', pd.NA, inplace=True)
        if not has_numbers(df[col]):
            df.drop(col, axis=1, inplace=True)

    df.dropna(how='all', inplace=True)

    return df

def main():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol']]

    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        df = clean_out_columns_and_rows(f'./data/sec-ipo-finance/{dir_name}/financial/combined.csv')
        if df is None:
            continue
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_02.csv', index=False)

# main()

df = locate_value_and_date('./data/sec-ipo-finance/ADT/financial/combined_clean_01.csv')
df.to_csv(f'./data/try_this.csv', index=False)
# df.to_csv(f'./data/sec-ipo-finance/ADT/financial/combined_clean_02.csv', index=False)