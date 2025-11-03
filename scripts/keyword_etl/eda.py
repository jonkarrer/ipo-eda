import plotly.express as px
import pandas as pd
import os

def try_to_get_file(dir_name, file_name):
        file_path=f'./data/sec-ipo-files/{dir_name}/{file_name}'
        does_exist = os.path.exists(file_path)

        if does_exist:
            return file_path
        else:
            return False

def plot_largest_diffs(df):
    df.drop(['Day', 'Close', 'Volume', 'Public_Price_Per_Share', 'Price_Public_Total'], axis=1, inplace=True)
    df.sort_values(by='Diff', ascending=False, inplace=True)
    fig = px.bar(df, y="Diff", title="Largest Diffs")
    fig.show()

def plot_winners_and_losers(df):
    df.drop(['Day', 'Symbol', 'Close', 'Volume', 'Public_Price_Per_Share', 'Price_Public_Total'], axis=1, inplace=True)
    df['Result'] = df['Diff'] > 0

    fig = px.bar(df, y="Result", title="Result of Diff Analysis")
    fig.show() 

def plot_keyword_histogram_analysis(df):
    df.drop(['Day', 'Symbol', 'Close', 'Volume', 'Public_Price_Per_Share', 'Price_Public_Total'], axis=1, inplace=True)

    column_name = 'machine learning'  # Replace with actual column name
    fig = px.histogram(df, x=column_name, nbins=200, title=f"Distribution of {column_name}")
    fig.show()

    
def plot_keyword_correlation_analysis(df):
    df.drop(['Day', 'Symbol', 'Close', 'Volume', 'Public_Price_Per_Share', 'Price_Public_Total'], axis=1, inplace=True)
    diff_values = df['Diff']
    
    # Split data into positive and negative Diff periods
    positive_diff_mask = diff_values > 0
    negative_diff_mask = diff_values < 0

    results = []
    for col in df.columns:
        if col != 'Diff':
            col_values = df[col]
            col_values = col_values.replace(0, pd.NA)
            count = col_values.notna().sum()
            
            # Calculate mean presence during positive vs negative Diff periods
            positive_mean = col_values[positive_diff_mask].mean()
            negative_mean = col_values[negative_diff_mask].mean()
                
            # Calculate the difference (positive - negative)
            diff_in_means = positive_mean - negative_mean
                
            if not pd.isna(diff_in_means):
                results.append({
                    'column': col,
                    'positive_mean': positive_mean,
                    'negative_mean': negative_mean,
                    'difference': diff_in_means,
                    'count': count
                })
    results_df = pd.DataFrame(results).sort_values('difference', key=abs, ascending=False)
    results_df.to_csv('./data/eda/keyword_correlations.csv', index=False)
    results_df = results_df.sort_values('difference', ascending=True)
    
    # Simplest possible bar chart
    fig = px.bar(
        results_df, 
        x='difference', 
        y='column',
        orientation='h'
    )
    fig.show()

df = pd.read_csv('./data/keyword_datasets/keyword_dataset_summary.csv')
df.drop(['Hong Kong', 'San Diego', 'Latin America'], axis=1, inplace=True)
df.to_csv('./data/eda/keyword_dataset_summary.csv', index=False)
# print(df['Symbol' == '484'])
plot_largest_diffs(df)