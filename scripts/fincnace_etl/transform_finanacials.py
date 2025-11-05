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
    
    # More flexible patterns that can handle accumulated text
    patterns = [
        # Standard formats
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",
        
        # "For the year ended..." formats
        r"For\s+the\s+.*?ended\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",
        
        # Handle accumulated parts like "For The Years Ended December 31, 2016"
        r".*?(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})",
        
        # Handle "December 31, 2016" pattern
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),\s*(\d{4})",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if len(match.groups()) >= 3:
                month, day, year = match.groups()[:3]
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
    column_dates = [None] * len(df.columns)
    column_contexts = [None] * len(df.columns)  # Store the original date context
    date_header_rows = []
    
    for row_idx, row in df.iterrows():
        row_name = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ""
        
        # If first column is empty, this is probably a date header row
        if not row_name or row_name.strip() == "":
            date_header_rows.append(row)
            continue
        
        # We hit a row with a symbol - process accumulated date headers
        if date_header_rows:
            
            # Combine date parts column-wise
            for col_idx in range(len(df.columns)):
                column_date_parts = []
                
                for header_row in date_header_rows:
                    if col_idx < len(header_row) and not pd.isna(header_row.iloc[col_idx]):
                        part = str(header_row.iloc[col_idx]).strip()
                        if part:
                            column_date_parts.append(part)
                
                # Try to parse the combined parts as a date
                if column_date_parts:
                    combined_date_str = ' '.join(column_date_parts)
                    date_match = regex_date(combined_date_str)
                    
                    if date_match:
                        column_dates[col_idx] = date_match
                        column_contexts[col_idx] = combined_date_str  # Store the context
            
            # Clear accumulated headers
            date_header_rows = []
        
        # Process this data row for numbers
        for col_idx, cell_value in enumerate(row):
            if pd.isna(cell_value) or col_idx == 0:  # Skip NaN and first column
                continue
            
            # Check if it's a number
            number_value = None
            if isinstance(cell_value, str):
                number_value = regex_number(cell_value)
            elif isinstance(cell_value, (int, float)):
                number_value = cell_value
            
            # Add row if we have both date and number for this column
            if number_value is not None and column_dates[col_idx] is not None:
                new_rows.append({
                    'symbol': row_name,
                    'date': column_dates[col_idx],
                    'context_date': column_contexts[col_idx],  # Add the context
                    'value': number_value
                })

    if new_rows is None:
        return None
    return pd.DataFrame(new_rows)

def format_df(df):
    df = df.drop_duplicates(subset=['symbol', 'value', 'context_date', 'date'], keep='first')
    df.loc[:, 'date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['symbol', 'date'])

    return df


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
        file_path= f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_02.csv';
        df = locate_value_and_date(file_path)
        if df is None:
            continue
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_02.csv', index=False)

# main()

file_path= f'./data/sec-ipo-finance/ADT/financial/combined_clean_02.csv';
df = pd.read_csv(file_path)
df = format_df(df)
df.to_csv(f'./data/try_this.csv', index=False)
# df.to_csv(f'./data/sec-ipo-finance/ADT/financial/combined_clean_02.csv', index=False)