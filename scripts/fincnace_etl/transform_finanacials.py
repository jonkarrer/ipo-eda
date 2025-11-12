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

def format_df(df):
    df = df.drop_duplicates(subset=['symbol', 'value', 'context_date', 'date'], keep='first')
    df.loc[:, 'date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce')
    df.loc[:,'symbol'] = df['symbol'].astype(str).str.lower()
    df.loc[:,'context_date'] = df['context_date'].astype(str)
    df = df.sort_values(by=['symbol', 'date'])

    return df

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

def filter_important_rows(df):
    finance_keywords = [
        'Revenue', 'Accounts Receivable', 'Earnings', 'Accounts Payable', 
        'Liabilities', 'Assets', 'Expense', 'Interest',  # Added missing comma here
        'Cash', 'Debt', 'Inventory', 'Earnings',
        'Depreciation', 'Cost', 'Income',
        'Inventories', 'Inventory', 'Land', 'Machinery', 'Equipment', 'Profit',  # Added missing comma
        'Machinery', 'Equipment', 'Operating',
        'Other Liabilities', 'Property', 'Sales', 'Loss',
        # Add these keywords to match your CSV data:
        'Deposits', 'Capital', 'Reserves', 'Interests', 'Deficit', 'Equity', 'Capitalization'
    ]

    # Lowercase the symbol column for consistent comparison
    df['symbol'] = df['symbol'].apply(to_snake_case)

    # Now use lowercase keywords for matching
    finance_keywords_lower = [keyword.lower() for keyword in finance_keywords]
    df = df[df['symbol'].str.contains('|'.join(finance_keywords_lower), case=False, na=False)]
    df = df[df['symbol'].str.len() < 30]

    # Remove dupe dates
    df = df.sort_values('value').drop_duplicates(subset=['symbol', 'date'], keep='last')

    # Part 1: Exact match for "Month DD, YYYY" format
    date_exact = df['context_date'].str.match(r'^[A-Za-z]+\s+\d{1,2},\s+\d{4}$', case=False, na=False)

    # Part 2: Contains match for "Year End" variations
    year_end_contains = df['context_date'].str.contains(r'Years?\s+Ended?', case=False, na=False, regex=True)
    
    year_end_contains_singular = df['context_date'].str.contains(r'Year?\s+Ended?', case=False, na=False, regex=True)
    
    year_end_contains_double_space = df['context_date'].str.contains(r'Year?\s\s+Ended?', case=False, na=False, regex=True)

    # Part 3: Contains match for 'As of' variations
    as_of_contains = df['context_date'].str.contains(r'As\s+of', case=False, na=False, regex=True)

    # Combine conditions with OR
    df = df[date_exact | year_end_contains | as_of_contains | year_end_contains_singular | year_end_contains_double_space]
    df = df.sort_values(['symbol', 'date'])

    return df

# First: Take the combined.csv file and clean out columns and rows -> combined_clean_01.csv
# Note: The combined.csv file was generated during the extraction script
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

def is_date_in_row(row):
    for col in row:
        if pd.isna(col):
            continue
            
        col_str = str(col).replace('  ', ' ')
        
        for pattern in DATE_PATTERNS:
            if re.search(pattern, col_str, re.IGNORECASE):
                return True
    
    return False

# Second: Locate the value and date from combined_clean_01 -> combined_clean_02
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

        if is_date_in_row(row):
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
                # print(column_date_parts) 
                column_date_parts = [re.sub(r'\s+', ' ', date) for date in column_date_parts]
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

# Third: Take the combined_clean_02.csv file and format it -> combined_clean_03.csv
def format_and_filter_rows(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        return None 
    except Exception as e:
        return None
    
    if df.columns.size == 0:
        return None

    df = format_df(df)
    df = filter_important_rows(df)
    return df

# Fourth: Calculate trend and recent
def calculate_trend_and_recent(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        return None 
    except Exception as e:
        return None

    new_df = pd.DataFrame() 
    for name, group in df.groupby('symbol'):
        first_value = group['value'].iloc[0]
        last_value = group['value'].iloc[-1]
        diff = last_value - first_value

        new_trend_row = pd.DataFrame({
            'symbol': f'{name}_trend',
            'value': [diff]
        })
        new_recent_row = pd.DataFrame({
            'symbol': f'{name}_recent',
            'value': [last_value]
        })

        new_df = pd.concat([new_df, new_trend_row, new_recent_row], ignore_index=True)
    
    return new_df

# Fifth: Pivot tables from other steps
def pivot_df(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        return None 
    except Exception as e:
        return None

    current_cols = df.set_index('symbol')['value']
    result = pd.concat([current_cols]).to_frame().T
    result = result.dropna(axis=1, how='any')

    return result

def combine_pivot_tables(file_path, accumulating_df):
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        return None 
    except Exception as e:
        return None 

    accumulating_df = pd.concat([accumulating_df, df], axis=1)

    return accumulating_df

def step_one():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol']]

    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/financial/combined.csv';
        df = clean_out_columns_and_rows(file_path)
        if df is None:
            continue
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_01.csv', index=False)

def step_two():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol']]

    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_01.csv';
        df = locate_value_and_date(file_path)
        if df is None:
            print(f'df not made {dir_name}')
            continue
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_02.csv', index=False)

def step_three():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol']]

    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_02.csv';
        df = format_and_filter_rows(file_path)
        if df is None:
            continue
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_03.csv', index=False)

def step_four():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol']]

    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_03.csv';
        df = calculate_trend_and_recent(file_path)
        if df is None:
            continue
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_04.csv', index=False)

def step_five():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')

    for tuple_row in list(ipo_df.itertuples(index=False)):
        # Convert tuple to dictionary
        row_dict = dict(zip(ipo_df.columns, tuple_row))
        
        # Extract file coordinates
        dir_name = row_dict['symbol']  # or whatever column contains the directory name
        file_path = f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_04.csv'
        df = pivot_df(file_path)
        
        if df is None:
            continue
            
        # Add all columns from the current row
        for col_name, value in row_dict.items():
            df[col_name] = value

        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_05.csv', index=False)

def step_six():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol']]

    all_dfs = []
    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_05.csv';
        try:
            df = pd.read_csv(file_path, low_memory=False)
        except FileNotFoundError:
            continue
        except Exception as e:
            continue
        if df is None:
            continue

        all_dfs.append(df)

    all_dfs = pd.concat(all_dfs, axis=0)
    all_dfs.to_csv(f'./data/all_financial.csv', index=False)

def main():
    # step_one()
    # step_two()
    # step_three()
    # step_four()
    # step_five()
    step_six()

main()


# file_path= f'./data/sec-ipo-finance/AHI/financial/combined_clean_01.csv';
# df = locate_value_and_date(file_path)
# df.to_csv(f'./data/sec-ipo-finance/AHI/financial/combined_clean_02.csv', index=False)
# file_path= f'./data/sec-ipo-finance/ADT/financial/combined_clean_02.csv';
# df = pd.read_csv(file_path)
# df = format_df(df)
# df = filter_important_rows(df)
# df = calculate_trend_and_recent(df)
# df.to_csv(f'./data/try_this.csv', index=False)
# df.to_csv(f'./data/sec-ipo-finance/ADT/financial/combined_clean_02.csv', index=False)