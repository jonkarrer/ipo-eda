from utils import create_df_from_csv
import pandas as pd


def join_keywords_with_financials(fin_df, keyword_df):
    fin_df = create_df_from_csv("./data/all_financial_reduced.csv")
    keyword_df = create_df_from_csv('./datasets/keyword_analysis_with_url.csv')
    joined_df = join_keywords_with_financials(fin_df, keyword_df)
    # joined_df.to_csv('./data/all_financial_with_keywords.csv', index=False)
    return joined_df


def main():
    df = join_keywords_with_financials()

main()