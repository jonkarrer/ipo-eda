import pandas as pd

def create_df_from_html_file(file_path):
    try:
        df = pd.read_html(file_path)
    except ValueError as e:
        if "No tables found" in str(e):
            print("No HTML tables found in the file")
            df = None
        else:
            print(f"ValueError: {e}")
            df = None
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        df = None
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = None
    return df

def create_df_from_csv(file_path):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        df = None
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = None
    return df

def make_new_dir(dir_path):
    import os
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def column_has_numbers(series):
    numeric = pd.to_numeric(series, errors='coerce')
    return numeric.notna().any()

def remove_empty_columns_from_df(df):
    columns_to_keep = []
    
    for col in df.columns:
        # Convert column to string and clean it
        col_values = df[col].astype(str).str.strip()

        # Remove invisible characters like zero-width spaces
        col_values = col_values.str.replace('​', '').str.replace('\u200b', '')
        
        # Keep column if it has any non-empty values
        if not col_values.eq('').all() and not col_values.eq('nan').all():
            columns_to_keep.append(col)
    
    return df[columns_to_keep]