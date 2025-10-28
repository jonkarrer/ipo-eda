import os
import re
import pandas as pd
from secxbrl import parse_inline_xbrl
from bs4 import BeautifulSoup

def process_xbrl_file(file_path):
    with open(file_path) as f:
        content = f.read() 

    return parse_inline_xbrl(content)

def filter_xbrl_df(df):
    # Define key patterns to match
    key_patterns = [
        'Revenue', 'CostOfGoods', 'CostOfRevenue', 'GrossProfit', 
        'OperatingIncome', 'NetIncome', 'EarningsPerShare',
        'Assets', 'Cash', 'AccountsReceivable', 'Inventory',
        'Liabilities', 'StockholdersEquity', 'RetainedEarnings',
        'NetCashProvidedByUsedInOperating', 'NetCashProvidedByUsedInInvesting', 
        'NetCashProvidedByUsedInFinancing',
        'WeightedAverageNumberOfShares', 'SharesOutstanding',
        'DepreciationAndAmortization', 'ShareBasedCompensation',
        'InterestExpense', 'IncomeTaxExpense'
    ]

    df = df[df['Is_Null'] == 'false'] # Drop rows with null = true
    df = df[df['Taxonomy_Prefix'] == 'us-gaap'] # Only want us-gaap data
    df['Value'] = pd.to_numeric(df['Value'].str.replace(',', ''), errors='coerce') # Convert to number, otherwise drop string info
    df = df.dropna(subset=['Value']) # Drop nulls
    df = df.drop_duplicates()
    df['Element_Name'] = df['Element_Name'].str.replace("us-gaap:", '') # Remove us-gaap prefix

    # Create a pattern-based filter
    pattern = '|'.join(key_patterns)
    df_filtered = df[df['Element_Local_Name'].str.contains(pattern, case=False, na=False)].copy()
    df_deduped = df_filtered.drop_duplicates(
        subset=['Element_Local_Name', 'Period_Start', 'Period_End', 'Instant_Date', 'Entity_ID'],
        keep='first'
    )
    return df_deduped
    

def analyze_prospectus_keywords(file_path):
    # Load HTML
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    
    text = soup.get_text().lower()
    
    target_keywords = [
        # Technology
        'technology', 'software', 'ai', 'machine learning',
        'cloud', 'saas', 'platform', 'digital', 'data', 'analytics', 'algorithm',
        'automation', 'blockchain', 'cryptocurrency', 'cybersecurity',
        'subscription', 'recurring','e-commerce', 'mobile', 'app', 'virtual',
        
        # Industry specific
        'healthcare', 'biotech', 'pharmaceutical', 'medical', 'clinical',
        'energy', 'renewable', 'solar', 'electric', 'battery',
        'real estate', 'logistics', 'transportation', 'automotive',
    ]
    
    # Count keyword frequencies
    keyword_counts = {}
    
    for kw in target_keywords:
        # Use word boundaries for exact matches, case insensitive
        pattern = r'\b' + re.escape(kw.lower()) + r'\b'
        matches = re.findall(pattern, text)
        keyword_counts[kw] = len(matches)
    
    return list(keyword_counts.items())

def generate_xbrl_dataframe(parsed_xbrl_items):
    # Extract desired attributes from xbrl
    rows = []
    for item in parsed_xbrl_items:
        element_name = item['_attributes'].get('name', '')
        period_end = item['_context'].get('context_period_enddate', '')
        instant_date = item['_context'].get('context_period_instant', '')
        
        # Use period_end if available, otherwise instant_date, otherwise no date
        date_part = period_end or instant_date or ''
        
        # Create combined element name with date
        if date_part:
            combined_element_name = f"{element_name}_{date_part}"
        else:
            combined_element_name = element_name

        row = {
            'Fact_ID': item['_attributes'].get('id', ''),
            'Element_Name': item['_attributes'].get('name', ''),
            'Value': item.get('_val', ''),
            'Unit': item['_attributes'].get('unitref', ''),
            'Is_Null': item['_attributes'].get('xs:nil', 'false'),
            'Context_Ref': item['_context'].get('_contextref', ''),
            'Entity_ID': item['_context'].get('context_entity_identifier', ''),
            'Period_Start': item['_context'].get('context_period_startdate', ''),
            'Period_End': item['_context'].get('context_period_enddate', ''),
            'Instant_Date': item['_context'].get('context_period_instant', ''),
            'Taxonomy_Prefix': item['_attributes'].get('name', '').split(':')[0] if ':' in item['_attributes'].get('name', '') else '',
            'Element_Local_Name': item['_attributes'].get('name', '').split(':')[-1] if ':' in item['_attributes'].get('name', '') else item['_attributes'].get('name', ''),
            'Combined_Element_Name': combined_element_name,
            'Date_Part': date_part,
            'Data_Type': 'xbrl'
        }
        rows.append(row)
        
    df = pd.DataFrame(rows)
    return filter_xbrl_df(df)


def generate_keyword_dataframe(keywords_frequencies):
    # Add word list extraction to rows
    rows = []
    for word_and_frequency in keywords_frequencies:
        row = {
            'Fact_ID': '0000',
            'Element_Name': word_and_frequency[0],
            'Value': word_and_frequency[1],
            'Unit': 'count',
            'Is_Null': 'false',
            'Context_Ref': 'frequency',
            'Entity_ID': '0000',
            'Period_Start': '',
            'Period_End': '',
            'Instant_Date': '',
            'Taxonomy_Prefix': 'custom',
            'Element_Local_Name': 'keyword_frequency',
            'Combined_Element_Name': '',
            'Date_Part': '',
            'Data_Type': 'keyword'
        }
        rows.append(row)
       
    return pd.DataFrame(rows)

def convert_dates_to_datetimes(combined_df):
    combined_df['Period_Start'] = pd.to_datetime(combined_df['Period_Start'])
    combined_df['Period_End'] = pd.to_datetime(combined_df['Period_End'])
    combined_df['Instant_Date'] = pd.to_datetime(combined_df['Instant_Date'])
    return combined_df

def create_trend_column(df, element_name):
    column = 'Value'
    
    # Create trend column
    df_sorted = df.sort_values('Instant_Date', ascending=True)
    df_sorted['Trend'] = df_sorted.groupby('Element_Name')[column].transform(lambda x: x.iloc[-1] - x.iloc[0])
    
    return df_sorted

def filter_for_latest_date(df):
    df_filtered = (df.sort_values('Instant_Date')
                    .groupby('Element_Name', as_index=False)
                    .last())
    return df_filtered

def remove_non_instant_dates(df):
    df_filtered = df[df['Instant_Date'].notna()]
    return df_filtered

def filter_out_columns(df):
    df_filtered = df[['Element_Name', 'Value', 'Unit', 'Trend', 'Instant_Date', 'Data_Type']]
    return df_filtered

def pivot_df(df):
    current_cols = df.set_index('Element_Name')['Value'].add_suffix('_Current')
    trend_cols = df.set_index('Element_Name')['Trend'].add_suffix('_Trend')
    result = pd.concat([current_cols, trend_cols]).to_frame().T

    result['Instant_Date'] = df['Instant_Date'].iloc[0]
    result['Data_Type'] = df['Data_Type'].iloc[0]
    result = result.dropna(axis=1, how='any')

    return result

def try_to_get_file(dir_name, file_name):
        file_path=f'./data/sec-ipo-files/{dir_name}/{file_name}'
        does_exist = os.path.exists(file_path)

        if does_exist:
            return file_path
        else:
            return False

def main():
    all_dfs = []
    no_files_found = []
    unparsed_symbols = []

    df = pd.read_csv('./data/ipo_day_summary.csv')
    df = df[['symbol', 'url']]
    df['url'] = df['url'].apply(lambda x: x.split('/')[-1])
    
    for tuple in list(df.itertuples(index=False)):
        # Get file coordinates
        dir_name=tuple[0]
        file_name=tuple[1]

        # Fetch the file for parsing
        file_path = try_to_get_file(dir_name, file_name) 

        # File may not exist
        if file_path == False:
            no_files_found.append(dir_name)
            continue

        # Parsing may fail
        parsed_xbrl_file = process_xbrl_file(file_path)
        if len(parsed_xbrl_file) == 0:
            unparsed_symbols.append(dir_name)
            continue
      
        keyword_frequencies = analyze_prospectus_keywords(file_path)
        xbrl_df = generate_xbrl_dataframe(parsed_xbrl_file)
        keyword_df = generate_keyword_dataframe(keyword_frequencies)

        df = convert_dates_to_datetimes(xbrl_df)
        trend_df = create_trend_column(df, 'Cash')
        latest_df = filter_for_latest_date(trend_df)
        only_instant_df = remove_non_instant_dates(latest_df)
        concat_df = pd.concat([only_instant_df, keyword_df])
        final_df = filter_out_columns(concat_df)
        pivoted_df = pivot_df(final_df)
        pivoted_df.to_csv(f'./data/sec-ipo-files/{dir_name}/pivot_table.csv')
        all_dfs.append(pivoted_df)


    final_df = pd.concat(all_dfs)
    final_df.to_csv(f'./data/full_eda.csv', index=False)

    no_files_df = pd.DataFrame(no_files_found, columns=['Symbol'])
    no_files_df.to_csv('./data/no_files_found.csv', index=False)

    unparsed_df = pd.DataFrame(unparsed_symbols, columns=['Symbol'])
    unparsed_df.to_csv('./data/unparsed.csv', index=False)

main() 
