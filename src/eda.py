from utils import create_df_from_csv
import pandas as pd

def remove_mostly_nan_columns(df):
    threshold = len(df) * 0.80
    df = df.dropna(thresh=threshold, axis=1)
    return df

def main():
    df = create_df_from_csv("./data/all_financial.csv")
    # cols_df = pd.DataFrame({"column": df.columns})
    # cols_df.to_csv("./data/columns.csv", index=False)
    df = remove_mostly_nan_columns(df)
    print(df.shape)
    df.to_csv("./data/all_financial_reduced.csv", index=False)


main()