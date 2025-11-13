import os
import pandas as pd


finance_keywords= ['Revenue', 'Accounts Receivable', 'Liabilities', 'Assets', 'Cash', 'Common Stock', 'Differed Tax', 'Inventory', 'Earnings', 'Operating Loss', 'Months Ended', 'Year Ended', 'Depreciation']
column_keywords = ['Six Months End', 'Twelve Months End', 'Year Ended', 'Six Months Ended', 'Twelve Months Ended', 'Years Ended', 'Years End', 'Period From']

def extract_table_data(file_path):
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

def find_finance_tables(df):
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

def remove_empty_columns(df):
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


def main():
    ipo_df = pd.read_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol', 'url']]
    ipo_df['url'] = ipo_df['url'].apply(lambda x: x.split('/')[-1])

    for tuple in list(ipo_df.itertuples(index=False)):
        # Extract file coordinates
        dir_name=tuple[0]
        file_name=tuple[1]
        
        # Extract table data from html
        df = extract_table_data(f'./data/sec-ipo-files/{dir_name}/{file_name}')
        if df is None:
            print(f'df not made {dir_name}')
            continue

        finance_dfs = find_finance_tables(df)

        if len(finance_dfs) == 0:
            print(f'No finance data found for {dir_name},')
            continue

        # Create folder if it doesn't exist to store data
        os.makedirs(f'./data/sec-ipo-finance/{dir_name}/financial', exist_ok=True)

        # Save tables to folder
        for i,f_df in enumerate(finance_dfs):
            # Create combined dataframe 
            if isinstance(f_df.columns, pd.MultiIndex):
                f_df.columns = f_df.columns.to_flat_index()
                finance_dfs[i] = f_df
            f_df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/{i}.csv', index=False)

        combined_df = pd.concat(finance_dfs)
        final_df = remove_empty_columns(combined_df)
        final_df.to_csv(f'./data/sec-ipo-finance/{dir_name}/financial/combined.csv', index=False)

main() 
