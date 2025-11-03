import pandas as pd
import os

def get_files_in_directory(directory):
    files = []
    for filename in os.listdir(directory):
        files.append(os.path.join(directory, filename))
    return files

def get_low_count_keywords_list():
    file = './data/word_counts/word_count_summary.csv'
    return pd.read_csv(file)['column_name'].tolist()

def remove_low_count_keywords(df):
    low_count_keywords = get_low_count_keywords_list()
    df = df.drop(low_count_keywords, axis=1, errors='ignore')
    return df

def main():
    files = get_files_in_directory('./data/keyword_datasets')
    print(files)

    for file in files:
        file_name = file.split('/')[-1]
        df = pd.read_csv(file)
        df = remove_low_count_keywords(df)
        df.to_csv('./data/keyword_datasets/keywords_removed_' + file_name, index=False)

main()
