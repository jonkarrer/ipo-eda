import pandas as pd
import shutil
import os

def try_to_copy_file(dir_name, file_name):
    possible_parent_dirs=['sec-ipo-prospectus', 'sec-ipo-prospectus-2', 'sec-ipo-prospectus-3', 'sec-ipo-prospectus-4', 'sec-ipo-prospectus-5']

    for p_dir in possible_parent_dirs:
        # Check which parent has the file
        file_path=f'/Volumes/Shared/sec-ipo-data/{p_dir}/{dir_name}/{file_name}'
        does_exist = os.path.exists(file_path)

        if does_exist:
            destination_dir=f"./data/sec-ipo-files/{dir_name}/"
            destination_file=f'{destination_dir}/{file_name}'
            os.makedirs(os.path.dirname(destination_dir), exist_ok=True)
            shutil.copy(file_path, destination_file)
            return



def main():
    # Get all symbols and last part of url to find files
    df = pd.read_csv('./data/ipo_day_summary.csv')

    df = df[['symbol', 'url']]
    df['url'] = df['url'].apply(lambda x: x.split('/')[-1])
    
    for tuple in list(df.itertuples(index=False)):
        dir_name=tuple[0]
        file_name=tuple[1]
        try_to_copy_file(dir_name, file_name)

main()