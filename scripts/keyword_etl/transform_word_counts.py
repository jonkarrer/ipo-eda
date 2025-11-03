import pandas as pd
import os

def get_files_in_directory(directory):
    files = []
    for filename in os.listdir(directory):
        files.append(os.path.join(directory, filename))
    return files

def concat_all_word_counts():
    word_count_files = get_files_in_directory('./data/word_counts')

    all_dfs = []
    for file in word_count_files:
        df = pd.read_csv(file)
        all_dfs.append(df)

    df = pd.concat(all_dfs)
    df.to_csv('./data/word_counts/all.csv', index=False)

def group_and_sum_word_counts():
    df = pd.read_csv('./data/word_counts/all.csv')
    df = df.groupby(['column_name'])['count'].sum().reset_index().sort_values('count', ascending=True)
    df.to_csv('./data/word_counts/word_count_summary.csv', index=False)


def main():
    concat_all_word_counts()
    group_and_sum_word_counts()


main()