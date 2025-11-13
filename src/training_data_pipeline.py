from process_html import html_tables_to_csv 
from sanitize_csv import clean_out_columns_and_rows
from utils import create_df_from_csv

def main():
    # Gather files to process into a dataframe
    ipo_df = create_df_from_csv('./datasets/keyword_analysis_with_url.csv')
    ipo_df = ipo_df[['symbol', 'url']]
    ipo_df['url'] = ipo_df['url'].apply(lambda x: x.split('/')[-1])

    # First step is to extract html tables related to financial from raw/dirty SEC ipo prospectus files into csv files
    html_tables_to_csv(ipo_df)

main()
