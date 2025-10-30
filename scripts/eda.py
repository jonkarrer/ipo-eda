import plotly.express as px
import pandas as pd
import os

columns_to_remove=[
    'Incorporated', 'LLP', 'Voting', 'Crime', 'Shareholder', 'Rights', 'Flows', 'CEO', 'Vehicle', 'CFO', 'COO', 'Worldwide', 'Party', 'Related', 'U.S.', 'Exclusive', 'Form', 'Forum', 'Nasdaq', 'NYSE', 'Listing', 'Only', 'Purchasers', 'Exempts', "Investors", 'Investor' 'Accounting',
    'Bulletin', 'Applied', 'Limited', 'Molecular', 'Gigabit', 'Adjusted', 'EBITDA', 'Export', 'Control', 'Electronics', 'Engineers', 'Deferred', 'Nonqualified', 'Probability', 'Scenario', 'Google', 'Fiber', 'Gb', 'Ethernet', 'Beta', 'Gamma', 'Sigma',
    'Automotive', 'Auto', 'Optical', 'Active', 'Texas', 'College', 'Participation', 'Plan', 'Incentive', 'Lease', 'Yard', 'Restaurant', 'Reason', 'Pacific', "Good", 'Great', 'Relationships', 'Certain', 'Electronic', 'Backup', 'Withholding', 'Investments', 'Joint', 'Trend',
    'Motor', 'Boys', 'Mountain', 'Lookout', 'Mr.', 'Mrs.', 'Owner', 'Trademark', 'Parties', 'Consent', 'Written', 'Sponsorship', 'Sponsorships', 'Retail', 'Properties', 'Average', 'Attorney', 'Attorneys', 'Horizons', 'Surgery', 'Plastic', "Steak", 'Downtown', 'Trolley', 'Shopping',
    'Salary', 'Base', "Award", 'Awards', 'Liquidations', 'Distributions', 'Penalty', 'Bids', "Balance", 'Sheets', 'Note', 'Landmark', 'Hotel', 'Hotels', 'Admin', 'Administration', 'Drug', 'Option', 'Share', "Sphere", 'Adaptive', 'Biotechnologies', 'Biologics', 'Biology', 'Harbour', 'No',
    'Patent', 'Release', 'Pay', 'Payment', 'Payments', 'Royalty', 'Biotech', 'Genome', 'Brain', 'Search', 'Deep', 'Antibodies', 'Responses', 'Find', 'Natural', 'View', 'Detailed', 'Cell', 'Cells', 'Human', 'Immune', 'License', 'Emergency', 'Therapeutics', 'Pharma', 'Pharmaceuticals', 'Cancer',
    'Series', 'Financial', 'Instruments', 'Improvements', 'Features', 'Part', 'Liquidation', 'Preference', 'Preferences', 'Officer', 'Scientific', 'Science', 'Industry', 'Opportunities', 'Drugs', 'Quant', 'Quanta', 'Compute', 'Computing', 'Computer', 'Parkway', 'Stockholder', 'Proposals', 'Proposal',
    'Embassy', 'Accounts', 'Receivable', 'Cash', 'Tourism', 'Commission', 'Republic', 'Banana', 'Barn', 'Pottery', 'Store', 'Home', 'Express', 'Juice', 'Dress', 'Square', 'Feet', 'Crab', 'Tea', 'Salsa', 'Fresh', 'Refining', 'Refinery', 'Taco', 'Outlet', 'Grocery', 'Old', 'PCS', 'Sprint',
    'Experience', 'Experiences', 'Showed', 'Street', 'Beach', 'Airbnb', 'Nationals', 'Specially', 'Designated', 'Tourism', 'Council', 'Hurricane', 'Open', 'Booked', 'Night', 'Stays', 'Cash', 'Free', 'Flow', 'Retained', 'Inventory', 'Resale', 'Restriction', 'Restrictions', 'Santa', 'Biosciences',
    'Geographic', 'Discover', 'Retain', 'Guests', 'Web', 'Amazon', 'Project', 'Regulatory', 'Fine', 'Arts', 'Ms', 'Ms.', 'Ask', 'Founder', 'Compensation', 'Institutional', 'Tax', 'Taxes', 'Lodge', 'Lodging', 'Readily', 'Healthcare', 'Program', 'Review', 'Programs', 'Disease', 'Neuroimaging', 'Initiative', 'Initiatives',
    'Neuro', 'Biotherapeutics', 'Critical', 'Accounting', 'Economic', 'Video', 'Offering', 'Investor', 'Relations', 'Warrant', 'Agent', 'Clinical', 'Trial', 'INC.', 'Professor', 'Gross', 'Profit', 'Fully', 'Paid', 'Therapy', 'Breakthrough', 'Medicinal', 'Products', 'Education', 'Affordability', 'Book',
    'Place', 'IRA', 'Online', 'Privacy', 'Accountants', 'Prospectus', 'Demand', 'Registration', 'Laws', 'Canaccord', 'Limitation', 'Exercise', 'Stamp', 'Debt', 'Insurance', 'Independence', 'Foods', 'Labs', 'Lab', 'Laboratories', 'Budget', 'Brokers', 'Tiger', 'Genetics', 'Law', 'Future', 'Issuance',
    'Academy', 'Naval', 'Navy', 'Qualified', 'IPO', 'Error', 'Correction', 'Correct', 'Moelis', 'Drive', 'Military', 'Boca', 'Eastern', 'Marine', 'Corps', 'Institute', 'Rule', 'Conduct', 'Provisions', 'Final', 'KGaA', 'Entertainment', 'Works', 'Body', 'Deficit Reduction', 'Menlo Park', 'Palo Alto',
    'Las Vegas', 'San Francisco', 'Puerto Rico', 'Product Approval', 'Lexington', 'Avenue', 'Climate Change', 'Saudi Arabia', 'Blank Check', 'Monte Carlo', 'Reverse Split' 
]

# low_count_col_df = pd.read_csv('./data/low_count_columns.csv')
# new_low_df = pd.read_csv('./data/low_count_columns_2000-2500.csv')
# combined_df = pd.concat([low_count_col_df, new_low_df], ignore_index=True)
# combined_df = combined_df.groupby(['column_name'])['count'].sum().reset_index()
# combined_df.to_csv('./data/low_count_columns_new.csv', index=False)
# columns_to_remove = combined_df['column_name'].tolist() + columns_to_remove

def try_to_get_file(dir_name, file_name):
        file_path=f'./data/sec-ipo-files/{dir_name}/{file_name}'
        does_exist = os.path.exists(file_path)

        if does_exist:
            return file_path
        else:
            return False

def build_dataset():
    all_dfs = []
    
    df = pd.read_csv('./data/ipo_day_summary.csv')
    df = df[['symbol', 'url', 'public_price_per_share']]
    df['url'] = df['url'].apply(lambda x: x.split('/')[-1])
    df = df.drop_duplicates(subset=['symbol'])

    i=0
    for tuple in list(df.itertuples(index=False)):
        i+=1
        if i < 2000:
            continue
        dir_name=tuple[0]
        file_name=tuple[1]
        public_price_per_share=tuple[2]
        # volume=tuple[2]
        # day=tuple[3]
        # ipo_date=tuple[4]
        # open_price=tuple[5]
        # close=tuple[6]
        # diff=tuple[7]
        # public_price_per_share=tuple[8]
        # price_public_total=tuple[9]

        # Get file coordinates
        file_name="word_analysis_2.csv"
        file_path = try_to_get_file(dir_name, file_name) 
        
        # File may not exist
        if file_path == False:
            print(f"File {file_name} does not exist for {dir_name}")
            continue
        
        ipo_df = pd.read_csv(file_path)
        ipo_df = ipo_df[[col for col in ipo_df.columns 
                   if not any(word in col for word in columns_to_remove)]]
        ipo_df['Symbol'] = dir_name
        ipo_df['Public_Price_Per_Share'] = public_price_per_share
        
        ipo_df.to_csv(f'./data/sec-ipo-files/{dir_name}/word_analysis_3.csv', index=False)
        all_dfs.append(ipo_df)
        
        if i > 2500:
            break

    print("Concatting") 
    c = pd.concat(all_dfs)
    export_low_count_columns(c)
    c.loc['Total'] = c.count()
    c.to_csv('./data/eda_dataset_temp.csv', index=False)

def export_low_count_columns(df, max_count=10):
    # Get the total counts from the last row
    df.loc['Total'] = df.count()
    total_counts = df.iloc[-1]
    
    # Find columns with count <= max_count
    low_count_columns = total_counts[total_counts <= max_count]
    
    # Create a simple dataframe
    low_count_df = pd.DataFrame({
        'column_name': low_count_columns.index,
        'count': low_count_columns.values
    }).sort_values('count')
    
    # Export to CSV
    low_count_df.to_csv('./data/low_count_columns_2000-2500.csv', index=False)

    
    return low_count_df

def plot_counts(df):
    # Create histogram
    counts = df.count()
    exclude_cols = [
        # Technology
        'technology', 'software', 'ai', 'machine learning',
        'cloud', 'saas', 'platform', 'digital', 'data', 'analytics', 'algorithm',
        'automation', 'blockchain', 'cryptocurrency', 'cybersecurity',
        'subscription', 'recurring','e-commerce', 'mobile', 'app', 'virtual',
        
        # Industry specific
        'healthcare', 'biotech', 'pharmaceutical', 'medical', 'clinical',
        'energy', 'renewable', 'solar', 'electric', 'battery',
        'real estate', 'logistics', 'transportation', 'automotive',

        # Other
        'Day', 'IPO Date', 'Open', 'Close', 'Public Price Per Share', 'Price Public Total', 'IPO_Date', 'UBS', 'Ubs', 'Price_Public_Total', 'Symbol'
    ]
    filter_counts = counts.drop(exclude_cols, errors='ignore')

    # Get top 50
    top_50 = filter_counts.nlargest(100)

    # Create bar chart
    fig = px.bar(
        x=top_50.index,
        y=top_50.values,
        title="Top 50 Columns by Non-Null Count",
        labels={'x': 'Column Name', 'y': 'Non-Null Count'}
    )

    # Rotate x-axis labels for readability
    fig.update_layout(xaxis_tickangle=-45)
    fig.show()

def plot_diff_correlation(df):
    df.drop(['Day', 'Symbol', 'Close', 'Volume'], axis=1, inplace=True)

    # Remove total row
    df_no_total = df.iloc[:-1]
    
    # Get diff values
    diff_values = df_no_total['Diff']
    
    # Calculate correlation for each column with Diff
    correlations = []
    
    for col in df_no_total.columns:
        if col != 'Diff':  # Skip the Diff column itself
            # For binary/categorical columns, use point-biserial correlation
            col_values = df_no_total[col]
            
            # Skip if column is all null or has no variation
            if col_values.notna().sum() < 2:
                continue
                
            corr = col_values.corr(diff_values)
            if not pd.isna(corr):
                correlations.append({
                    'column': col,
                    'correlation': corr,
                    'abs_correlation': abs(corr),
                    'count': col_values.notna().sum()
                })
    
    # Convert to DataFrame and sort by absolute correlation
    corr_df = pd.DataFrame(correlations).sort_values('abs_correlation', ascending=False)
    
    # Show top positive and negative correlations
    print("Top 20 POSITIVE correlations (associated with higher Diff):")
    top_positive = corr_df[corr_df['correlation'] > 0].head(20)
    for _, row in top_positive.iterrows():
        print(f"  {row['column']}: {row['correlation']:.4f} (n={row['count']})")
    
    print("\nTop 20 NEGATIVE correlations (associated with lower Diff):")
    top_negative = corr_df[corr_df['correlation'] < 0].head(20)
    for _, row in top_negative.iterrows():
        print(f"  {row['column']}: {row['correlation']:.4f} (n={row['count']})")
    
    # Plot top correlations
    top_20_overall = corr_df.head(20)
    
    fig = px.bar(
        top_20_overall,
        x='correlation',
        y='column',
        orientation='h',
        title='Top 20 People/Banks/Cities by Correlation with First-Day Price Change',
        labels={'correlation': 'Correlation with Diff', 'column': 'Person/Bank/City'},
        color='correlation',
        color_continuous_scale='RdBu_r'
    )
    fig.update_layout(height=600)
    fig.show()
    
def tiered_correlation_analysis(df):
    df.drop(['Day', 'Symbol', 'Close', 'Volume'], axis=1, inplace=True)
    df_no_total = df.iloc[:-1]
    diff_values = df_no_total['Diff']
    
    # Analyze at different confidence levels
    for min_count in [20, 15, 10, 7]:
        correlations = []
        
        for col in df_no_total.columns:
            if col != 'Diff':
                col_values = df_no_total[col]
                count = col_values.notna().sum()
                
                if count >= min_count:
                    corr = col_values.corr(diff_values)
                    if not pd.isna(corr):
                        correlations.append({
                            'column': col,
                            'correlation': corr,
                            'count': count
                        })
        
        corr_df = pd.DataFrame(correlations).sort_values('correlation', key=abs, ascending=False)
        
        print(f"\n{'='*50}")
        print(f"TIER: Minimum {min_count} appearances ({len(corr_df)} entities)")
        print(f"{'='*50}")
        
        if len(corr_df) > 0:
            print("Top 20 Positive:")
            for _, row in corr_df[corr_df['correlation'] > 0].head(20).iterrows():
                print(f"  {row['column']}: +{row['correlation']:.4f} (n={row['count']})")
            
            print("Top 20 Negative:")
            for _, row in corr_df[corr_df['correlation'] < 0].head(20).iterrows():
                print(f"  {row['column']}: {row['correlation']:.4f} (n={row['count']})")
        else:
            print("No entities meet this threshold.")
        

build_dataset()
# df = pd.read_csv('./data/eda_dataset.csv')
# plot_counts(df)
# plot_diff_correlation(df)
# tiered_correlation_analysis(df)