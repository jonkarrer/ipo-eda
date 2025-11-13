from utils import create_df_from_csv, column_has_numbers
import pandas as pd

# First: Take the combined.csv file and clean out columns and rows -> combined_clean_01.csv
def clean_out_columns_and_rows(file_path):
    df = create_df_from_csv(file_path)
    if df is None:
        return None
    
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