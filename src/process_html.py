import re
from utils import make_new_dir, remove_empty_columns_from_df
import pandas as pd
from io import StringIO

finance_keywords= ['Revenue', 'Accounts Receivable', 'Liabilities', 'Assets', 'Cash', 'Common Stock', 'Differed Tax', 'Inventory', 'Earnings', 'Operating Loss', 'Months Ended', 'Year Ended', 'Depreciation']
column_keywords = ['Six Months End', 'Twelve Months End', 'Year Ended', 'Six Months Ended', 'Twelve Months Ended', 'Years Ended', 'Years End', 'Period From']

def clean_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content= file.read()
    
    # Remove HTML comments
    html_content = re.sub(r'<!--.*?-->', '', html_content, flags=re.DOTALL)

    # Lowercase html tags
    html_content = re.sub(r'<[^>]+>', lambda match: match.group(0).lower(), html_content)

    # Uppercase Doctype tag
    html_content = re.sub(r'<!doctype[^>]*>', lambda match: match.group(0).upper(), html_content)

    # Make sure doctype is the first thing
    html_content = re.sub(r'<!doctype[^>]*>(.*)', r'<!DOCTYPE \1>', html_content)

    return html_content

def create_df_from_html_string(html_content):
    try:
        df = pd.read_html(StringIO(html_content))
    except ValueError as e:
        if "No tables found" in str(e):
            print("No HTML tables found in the file")
            df = None
        else:
            print(f"ValueError: {e}")
            df = None
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = None
    return df

def find_finance_tables(df, finance_keywords):
    all_dfs = []
    for item in df:
        for w in finance_keywords:
            exists = item.astype(str).apply(lambda x: x.str.contains(w, case=False, na=False)).any().any()
            if exists:
                is_small = item.shape[0] < 4
                if is_small:
                    continue
                all_dfs.append(item) 

    return all_dfs

def extract_finance_tables_from_html(file_path, finance_keywords):
    html_content = clean_html_file(file_path)
    raw_dfs_list = create_df_from_html_string(html_content)
    finance_df = find_finance_tables(raw_dfs_list, finance_keywords)
    if len(finance_df) == 0:
        return None

    return finance_df

def save_each_finance_table_df(dir_name, finance_dfs):
    # Create folder if it doesn't exist to store data
    make_new_dir(f'./data/sec-ipo-finance/{dir_name}')

    # Save tables to folder
    for i,f_df in enumerate(finance_dfs):
        if isinstance(f_df.columns, pd.MultiIndex):
            f_df.columns = f_df.columns.to_flat_index()
            finance_dfs[i] = f_df
        f_df.to_csv(f'./data/sec-ipo-finance/{dir_name}/{i}.csv', index=False)

def generate_combined_financial_csv(dir_name, finance_dfs):
        combined_df = pd.concat(finance_dfs)
        final_df = remove_empty_columns_from_df(combined_df)
        final_df.to_csv(f'./data/sec-ipo-finance/{dir_name}/combined.csv', index=False)


def html_tables_to_csv(dir_and_file_names_df):
    for tuple in list(dir_and_file_names_df.itertuples(index=False)):
        # Create File Path
        dir_name=tuple[0]
        file_name=tuple[1]
        file_path = f'./data/sec-ipo-files/{dir_name}/{file_name}'
        
        # Extract table data from html
        finance_dfs = extract_finance_tables_from_html(file_path, finance_keywords)
        if finance_dfs is None:
            print(f'Could not create df from html file {dir_name}/{file_name}')
            continue
        
        # Save files and combine frames for one unified dataframe
        save_each_finance_table_df(dir_name, finance_dfs)
        generate_combined_financial_csv(dir_name, finance_dfs)
