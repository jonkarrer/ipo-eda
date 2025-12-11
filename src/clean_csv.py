from utils import column_has_numbers, is_date_in_row, regex_format_date, regex_format_number, to_snake_case
from process_html import clean_html_file_and_stringify
import pandas as pd
import re

# First: Take the combined.csv file and clean out columns and rows -> combined_clean_01.csv
def clean_out_columns_and_rows(df):
    for i,col in enumerate(df.columns):
        if i == 0:
            continue
        df[col].replace('$', pd.NA, inplace=True)
        df[col].replace('$—', pd.NA, inplace=True)
        df[col].replace('—', pd.NA, inplace=True)
        if not column_has_numbers(df[col]):
            df.drop(col, axis=1, inplace=True)

    df.dropna(how='all', inplace=True)

    return df

# Second: Locate the value and date from combined_clean_01 -> combined_clean_02
def locate_value_and_date(df):
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
                column_date_parts = [re.sub(r'\s+', ' ', date) for date in column_date_parts]
                # Try to parse the combined parts as a date
                if column_date_parts:
                    combined_date_str = ' '.join(column_date_parts)
                    date_match = regex_format_date(combined_date_str)
                    
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
                number_value = regex_format_number(cell_value)
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
def format_and_filter_rows(df):
    if df.columns.size == 0:
        return None

    df = df.drop_duplicates(subset=['symbol', 'value', 'context_date', 'date'], keep='first')
    df.loc[:, 'date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce')
    df.loc[:,'symbol'] = (df['symbol']
                     .astype(str)
                     .str.lower()
                     .replace(r'[",\'\u2019:;().·]', '', regex=True)  # Remove punctuation
                     .str.replace(r'\s+', ' ', regex=True)            # Clean up extra spaces
                     .str.strip())                                    # Remove leading/trailing spaces
    df.loc[:,'context_date'] = df['context_date'].astype(str)
    df = df.sort_values(by=['symbol', 'date'])

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

# Fourth: Calculate trend and recent
def calculate_trend_and_recent(df):
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

# Fifth: Pivot
def pivot_df(df):
    current_cols = df.set_index('symbol')['value']
    result = pd.concat([current_cols]).to_frame().T
    result = result.dropna(axis=1, how='any')

    return result

# Sixth: Calculate IPO Prospectus Document Length
def calculate_document_length(df, symbol):
    file_name = df['url']
    file_name = file_name.split('/')[-1]
    file_path = f'./data/sec-ipo-files/{symbol}/{file_name}'
    html_content = clean_html_file_and_stringify(file_path)
    document_length = len(html_content)

    df['document_length'] = document_length

    return df 
