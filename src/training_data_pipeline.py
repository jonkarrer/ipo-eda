from process_html import html_tables_to_csv 
from sanitize_csv import clean_out_columns_and_rows, locate_value_and_date, format_and_filter_rows, calculate_trend_and_recent, pivot_df
from utils import create_df_from_csv

def main():
    # Gather file locations to process into a dataframe
    ipo_list = create_df_from_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_list = ipo_list[['symbol', 'url']]
    ipo_list['url'] = ipo_list['url'].apply(lambda x: x.split('/')[-1])

    # First step is to extract html tables related to financial from raw/dirty SEC ipo prospectus files into csv files
    # html_tables_to_csv(ipo_list)

    # Second step is to format and clean the csv files
    # clean_csvs(ipo_list)

    # Third step is to take the combined_clean_02.csv file and format it -> combined_clean_03.csv



def clean_csvs(ipo_list):
    for tuple in list(ipo_list.itertuples(index=False)):
        dir_name=tuple[0]
        file_path= f'./data/sec-ipo-finance/{dir_name}/combined.csv';

        df = create_df_from_csv(file_path)
        if df is None:
            print(f'A: Combined file not converted into a df for {dir_name},')
            continue

        df = clean_out_columns_and_rows(df)
        if df is None:
            print(f'B: Could not clean out columns and rows for {dir_name},')
            continue

        df = locate_value_and_date(df) 
        if df is None:
            print(f'C: Could not locate value and date for {dir_name},')
            continue
        
        df = format_and_filter_rows(df)
        if df is None:
            print(f'D: Could not format and filter rows for {dir_name},')
            continue

        df = calculate_trend_and_recent(df)
        if df is None:
            print(f'E: Could not calculate trend and recent for {dir_name},')
            continue

        df.to_csv(f'./data/sec-ipo-finance/{dir_name}/clean_financial.csv', index=False)


def pivot_and_combine_tables(ipo_list):
    for tuple_row in list(ipo_list.itertuples(index=False)):
        # Convert tuple to dictionary
        row_dict = dict(zip(ipo_list.columns, tuple_row))
        
        # Extract file coordinates
        dir_name = row_dict['symbol']  # or whatever column contains the directory name
        file_path = f'./data/sec-ipo-finance/{dir_name}/financial/combined_clean_04.csv'
        df = create_df_from_csv(file_path)
        df = pivot_df(df)
        
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

# main()
