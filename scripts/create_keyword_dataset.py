import os
import pandas as pd

def get_files_in_directory(directory):
    files = []
    for filename in os.listdir(directory):
        files.append(os.path.join(directory, filename))
    return files

def main():
    files = get_files_in_directory('./data/keyword_datasets/keywords_removed')

    all_dfs = []
    for file in files:
        df = pd.read_csv(file)
        all_dfs.append(df)

    print('Concatenating dataframes...')
    df = pd.concat(all_dfs)
    df.to_csv('./data/keyword_datasets/keyword_dataset_summary.csv', index=False)


main()