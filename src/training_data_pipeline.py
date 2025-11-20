from process_html import html_tables_to_csv 
from clean_csv import clean_out_columns_and_rows, locate_value_and_date, format_and_filter_rows, calculate_trend_and_recent, pivot_df
from utils import create_df_from_csv
import pandas as pd

def create_training_dataset(ipo_list):
    all_dfs = []
    for tuple in list(ipo_list.itertuples(index=False)):
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/combined.csv';

        df = create_df_from_csv(file_path)
        if df is None or df.columns.size == 0:
            print(f'A: Combined file not converted into a df for {dir_name},')
            continue

        df = clean_out_columns_and_rows(df)
        if df is None or df.columns.size == 0:
            print(f'B: Could not clean out columns and rows for {dir_name},')
            continue

        df = locate_value_and_date(df) 
        if df is None or df.columns.size == 0:
            print(f'C: Could not locate value and date for {dir_name},')
            continue
        
        df = format_and_filter_rows(df)
        if df is None or df.columns.size == 0:
            print(f'D: Could not format and filter rows for {dir_name},')
            continue

        df = calculate_trend_and_recent(df)
        if df is None or df.columns.size == 0:
            print(f'E: Could not calculate trend and recent for {dir_name},')
            continue

        # Convert tuple to dictionary
        row_dict = dict(zip(ipo_list.columns, tuple))
        df = pivot_df(df)
        if df is None or df.columns.size == 0:
            print(f'F: Could not pivot df for {dir_name},')
            continue

        # Add all columns from the current row
        for col_name, value in row_dict.items():
            df[col_name] = value
        
        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/clean_financial.csv', index=False)
        
        all_dfs.append(df)

    print('Combining dataframes...') 
    all_dfs = pd.concat(all_dfs, axis=0)
    all_dfs.to_csv(f'./data/all_financial.csv', index=False)
    print('All financial data cleaned and saved to ./data/all_financial.csv')

def combine_like_terms(df):
    df['cash_trend'].fillna(df['cash_and_cash_equivalents_trend'], inplace=True)
    df['cash_recent'].fillna(df['cash_and_cash_equivalents_recent'], inplace=True)
    df['total_shareholders_equity_trend'].fillna(df['total_stockholders_equity_trend'], inplace=True)
    df['total_shareholders_equity_recent'].fillna(df['total_stockholders_equity_recent'], inplace=True)
    return df

def remove_mostly_nan_columns(df):
    threshold = len(df) * 0.60
    df = df.dropna(thresh=threshold, axis=1)
    df = df.drop(['url'], axis=1)
    return df

def join_keywords_with_financials(fin_df, keyword_df):
    # join on symbol
    joined_df = pd.merge(fin_df, keyword_df, how='inner', on='symbol')
    return joined_df

def clean_training_dataset():
    df = create_df_from_csv("./data/all_financial.csv")
    df = combine_like_terms(df)
    df = remove_mostly_nan_columns(df)
    df.to_csv("./data/all_financial_reduced.csv", index=False)

def main():
    # Gather file locations to process into a dataframe
    ipo_list = create_df_from_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_list = ipo_list[['symbol', 'url']]
    ipo_list['url'] = ipo_list['url'].apply(lambda x: x.split('/')[-1])

    # Extract html tables related to financial from raw/dirty SEC ipo prospectus files into csv files
    html_tables_to_csv(ipo_list)

    # Create training dataset from those csv files
    create_training_dataset(ipo_list)

    # Clean training dataset
    clean_training_dataset()

main()