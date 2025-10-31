import os
import pandas as pd

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

def try_to_get_file(dir_name, file_name):
        file_path=f'./data/sec-ipo-files/{dir_name}/{file_name}'
        does_exist = os.path.exists(file_path)

        if does_exist:
            return file_path
        else:
            return False

def get_files_in_directory(directory):
    files = []
    for filename in os.listdir(directory):
        files.append(os.path.join(directory, filename))
    return files

def nightly_build():
    files = pd.read_csv('./data/ipo_day_summary.csv')
    files = files[['symbol', 'url', 'public_price_per_share']]
    files = files.drop_duplicates(subset=['symbol'])
    files['url'] = files['url'].apply(lambda x: x.split('/')[-1])

    batches = [(0, 500), (500, 1000), (1000, 1500), (1500, 2000), (2000, 2500), (2500, 3000)]
    batch_index=0
    for batch in batches:
        batch_dfs = []
        
        for i,tuple in enumerate(list(files.itertuples(index=False))):
            dir_name=tuple[0]
            file_name=tuple[1]
            public_price_per_share=tuple[2]


            # Set batch bounds
            batch_start=batch[0]
            batch_end=batch[1]
            batch_index += 1

            # Want to not continue if the batch is below the start
            if i < batch_start:
                continue

            # Get file coordinates
            file_name="word_analysis_2.csv"
            file_path = try_to_get_file(dir_name, file_name) 

            # File may not exist
            if file_path == False:
                continue
            
            ipo_df = pd.read_csv(file_path)
            ipo_df = ipo_df[[col for col in ipo_df.columns 
                       if not any(word in col for word in columns_to_remove)]]
            ipo_df['Symbol'] = dir_name
            ipo_df['Public_Price_Per_Share'] = public_price_per_share

            ipo_df.to_csv(f'./data/sec-ipo-files/{dir_name}/nightly_build.csv', index=False)
            batch_dfs.append(ipo_df)

            # Want to break if the batch is above the end, thus need to move to the next batch
            if i >= batch_end:
                break
        
        print(f"Concatenating Batch {batch}...") 
        combined_batches = pd.concat(batch_dfs)
        export_low_count_columns(combined_batches, batch_index)
        combined_batches.loc['Total'] = combined_batches.count()
        combined_batches.to_csv(f'./data/keyword_datasets/batch_{batch_index}.csv', index=False)


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

def export_low_count_columns(df, index, max_count=6):
    # Get the total counts from the last row
    total_counts = df.count()
    
    # Find columns with count <= max_count
    low_count_columns = total_counts[total_counts <= max_count]
    
    # Create a simple dataframe
    low_count_df = pd.DataFrame({
        'column_name': low_count_columns.index,
        'count': low_count_columns.values
    }).sort_values('count')
    
    # Export to CSV
    low_count_df.to_csv(f'./data/low_count_columns_{index}.csv', index=False)

def add_urls_to_keyword_analysis():
    keyword_df = pd.read_csv('./datasets/keyword_analysis_copy.csv')
    ipo_df = pd.read_csv('./data/ipo_day_summary.csv')
    keyword_df.columns = keyword_df.columns.str.lower()
    ipo_df.columns = ipo_df.columns.str.lower()

    new_df = pd.merge(keyword_df, ipo_df[['symbol', 'public_price_per_share', 'price_public_total', 'url', 'ipo_date']], how='left', on=['symbol', 'public_price_per_share', 'price_public_total' ])
    new_df.drop_duplicates(subset=['symbol'], inplace=True)
    new_df.to_csv('./datasets/keyword_analysis_merge.csv', index=False)


def main():
    files = get_files_in_directory('./data/keyword_datasets/keywords_removed')

    all_dfs = []
    for file in files:
        df = pd.read_csv(file)
        all_dfs.append(df)

    print('Concatenating dataframes...')
    df = pd.concat(all_dfs)
    df.to_csv('./data/keyword_datasets/keyword_dataset_summary.csv', index=False)


# main()
add_urls_to_keyword_analysis()