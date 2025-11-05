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
    
    recent_date = None
    new_rows = []
    
    # Iterate through rows
    for row_idx, row in df.iterrows():
        try:
            # Safer way to get the first column value
            row_name = row.iloc[0] if len(row) > 0 and not pd.isna(row.iloc[0]) else ""
            row_name = str(row_name)  # Convert to string safely
        except (IndexError, KeyError):
            continue  # Skip this row if we can't get the first column
        
        # Iterate through all columns in the row
        for col_idx in range(len(row)):
            try:
                row_value = row.iloc[col_idx]
            except (IndexError, KeyError):
                continue
            
            # If nan, skip
            if pd.isna(row_value):
                continue
            
            # Convert to string for processing
            row_value_str = str(row_value)
            
            # Check if the value is a date
            is_date = regex_date(row_value_str)
            
            if is_date:
                # Update the recent date
                recent_date = row_value_str
            else:
                # Check if it's a number
                number_value = None
                if isinstance(row_value, str):
                    number_value = regex_number(row_value)
                elif isinstance(row_value, (int, float)):
                    number_value = row_value
                else:
                    # Try to extract number from string representation
                    number_value = regex_number(row_value_str)
                
                # Only add a row if we have both a recent date and a valid number
                if recent_date is not None and number_value is not None:
                    new_row = {
                        'symbol': row_name,
                        'context_date': recent_date,
                        'date': regex_date(recent_date),
                        'value': number_value
                    }
                    new_rows.append(new_row)
    
    # Create DataFrame from collected rows
    new_df = pd.DataFrame(new_rows).sort_values(by=['symbol'], ascending=[True])
    return new_df





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