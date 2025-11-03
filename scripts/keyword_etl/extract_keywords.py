import pandas as pd
import re
from bs4 import BeautifulSoup
import os
from nltk import ne_chunk, pos_tag, word_tokenize

# nltk.download('punkt_tab')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('maxent_ne_chunker')
# nltk.download('words')
# nltk.download('averaged_perceptron_tagger_eng')
# nltk.download('maxent_ne_chunker_tab')

def try_to_get_file(dir_name, file_name):
        file_path=f'./data/sec-ipo-files/{dir_name}/{file_name}'
        does_exist = os.path.exists(file_path)

        if does_exist:
            return file_path
        else:
            return False

def extract_person_names(soup):
    text = soup.get_text()
    # Tokenize and tag
    tokens = word_tokenize(text)
    pos_tags = pos_tag(tokens)
    
    # Named entity recognition
    tree = ne_chunk(pos_tags)
    
    keyword_counts = {}
    for subtree in tree:
        if hasattr(subtree, 'label') and subtree.label() == 'PERSON':
            name = ' '.join([token for token, pos in subtree.leaves()])
            if name in keyword_counts:
                continue
            
            if not is_this_a_person(name):
                continue

            pattern = r'\b' + re.escape(name) + r'\b'
            matches = re.findall(pattern, text)
            keyword_counts[name] = len(matches)  
   
    return list(keyword_counts.items())  

def is_this_a_person(name):
    # Must be 2-3 words only
    name_parts = name.split()
    if len(name_parts) < 2 or len(name_parts) > 3:
        return False
    
    # Immediate rejections for obvious non-person terms
    definite_non_person_words = {
        'Corporation', 'Company', 'Inc', 'LLC', 'Ltd', 'Group', 'Holdings', 
        'Capital', 'Management', 'Partners', 'Associates', 'Fund', 'Funds',
        'Acquisition', 'Investment', 'Ventures', 'Trust', 'Bank', 'Securities',
        'Services', 'Systems', 'Solutions', 'Technologies', 'Global', 'International',
        'Committee', 'Board', 'Directors', 'Report', 'Act', 'Rules', 'Legal',
        'Market', 'Exchange', 'Class', 'Unit', 'Program', 'Factor', 'Factors',
        'Risk', 'Risks', 'Income', 'Tax', 'Revenue', 'Business', 'Initial',
        'Target', 'Portfolio', 'Vehicle', 'Presence', 'Knowledge', 'Support',
        'Value', 'Shares', 'Impact', 'System', 'Current', 'Annual', 'Special',
        'Energy', 'Power', 'Infrastructure', 'Opportunities', 'Strategic',
        'Initiatives', 'Credit', 'Equity', 'Lending', 'Real', 'Estate',
        'Commercial', 'Private', 'Public', 'Corporate', 'Direct', 'Mezzanine',
        'Dynamic', 'Robust', 'Sourcing', 'Scaled', 'Investing', 'Hardware',
        'Software', 'Supply', 'Stores', 'Center', 'Holdings', 'Brands',
        'Waste', 'Advisory', 'Royal', 'Purpose', 'Unless', 'Except', 'Due',
        'Further', 'Highly', 'Known', 'Trends', 'Ability', 'Islands', 'Cayman', 'High', 'Distinction',
        'Life', 'Science', 'Health', 'Services', 'Systems', 'Solutions', 'Technologies', 'Global', 'International',
        'Committee', 'Board', 'Directors', 'Report', 'Act', 'Rules', 'Legal',
        'Market', 'Exchange', 'Class', 'Unit', 'Program', 'Factor', 'Factors',
        'Risk', 'Risks', 'Income', 'Tax', 'Revenue', 'Business', 'Initial',
        'Target', 'Portfolio', 'Vehicle', 'Presence', 'Knowledge', 'Support',
        'Value', 'Shares', 'Impact', 'System', 'Current', 'Annual', 'Special',
        'Energy', 'Power', 'Infrastructure', 'Opportunities', 'Strategic',
        'Initiatives', 'Credit', 'Equity', 'Lending', 'Real', 'Estate',
        'Commercial', 'Private', 'Public', 'Corporate', 'Direct', 'Mezzanine',
        'Dynamic', 'Robust', 'Sourcing', 'Scaled', 'Investing', 'Hardware',
        'Software', 'Supply', 'Stores', 'Center', 'Holdings', 'Brands',
        'Waste', 'Advisory', 'Royal', 'Purpose', 'Unless', 'Except', 'Due',
        'Further', 'Highly', 'Known', 'Trends', 'Ability', 'Islands', 'Cayman', 'High', 'Distinction',
        'Life', 'Science', 'Health', 'Services', 'Systems', 'Solutions', 'Technologies', 'Global', 'International',
        'Committee', 'Board', 'Directors', 'Report', 'Act', 'Rules', 'Legal',
        'Market', 'Exchange', 'Class', 'Unit', 'Program', 'Factor', 'Factors',
        'Employee', 'Employees', 'Director', 'Directors', 'Management', 'Managers',
        'School', 'Periodic', 'Periodically', 'Reporting', 'Period', 'Periods',
        'Netherlands', 'Australia', 'New', 'York', 'United', 'States', 'State', 'U.S.', 'U.S.A.', 'U.S.A', 'U.S', 'U.S.', 'U.S.A.', 'U.S.A', 'U.S',
        'University', 'Liabilities', 'Liability', 'Civil', 'Civilian', 'Civilians', "Net", 'Loss', 'Loan', 'Explanatory', 'Paragraph'

        # Business/Legal entities
        'Enterprise', 'Enterprises', 'Industries', 'Organization', 'Organizations',
        'Institution', 'Institutions', 'Foundation', 'Foundations', 'Agency', 'Agencies',
        'Authority', 'Authorities', 'Department', 'Departments', 'Ministry', 'Bureau',
        'Office', 'Division', 'Subsidiary', 'Subsidiaries', 'Affiliate', 'Affiliates',
        'Partnership', 'Partnerships', 'Consortium', 'Alliance', 'Network', 'Networks',
        'Platform', 'Platforms', 'Framework', 'Frameworks', 'Structure', 'Structures',
    
        # Financial/Investment terms
        'Assets', 'Asset', 'Capital', 'Capitalization', 'Financing', 'Finance',
        'Securities', 'Security', 'Bond', 'Bonds', 'Stock', 'Stocks', 'Options',
        'Derivatives', 'Commodities', 'Currency', 'Currencies', 'Treasury',
        'Reserve', 'Reserves', 'Liquidity', 'Volatility', 'Yield', 'Returns',
        'Performance', 'Benchmark', 'Index', 'Indices', 'Rating', 'Ratings',
        'Valuation', 'Pricing', 'Premium', 'Discount', 'Margin', 'Margins',
    
        # Business operations
        'Operations', 'Operation', 'Process', 'Processes', 'Procedure', 'Procedures',
        'Strategy', 'Strategies', 'Planning', 'Development', 'Implementation',
        'Execution', 'Delivery', 'Production', 'Manufacturing', 'Distribution',
        'Supply', 'Chain', 'Logistics', 'Procurement', 'Sourcing', 'Vendor',
        'Vendors', 'Client', 'Clients', 'Customer', 'Customers', 'Consumer',
        'Consumers', 'Market', 'Markets', 'Segment', 'Segments', 'Channel',
        'Channels', 'Sales', 'Marketing', 'Advertising', 'Promotion', 'Brand',
    
        # Technology/Systems
        'Technology', 'Technologies', 'Innovation', 'Innovations', 'Research',
        'Development', 'Engineering', 'Design', 'Architecture', 'Infrastructure',
        'Database', 'Databases', 'Application', 'Applications', 'Interface',
        'Interfaces', 'Protocol', 'Protocols', 'Standard', 'Standards',
        'Specification', 'Specifications', 'Configuration', 'Integration',
        'Automation', 'Analytics', 'Data', 'Information', 'Intelligence',
    
        # Geographic/Location terms
        'Region', 'Regions', 'Territory', 'Territories', 'Area', 'Areas',
        'Zone', 'Zones', 'District', 'Districts', 'County', 'Counties',
        'State', 'States', 'Province', 'Provinces', 'Country', 'Countries',
        'Nation', 'Nations', 'City', 'Cities', 'Town', 'Towns', 'Village',
        'Villages', 'Location', 'Locations', 'Site', 'Sites', 'Facility',
        'Facilities', 'Campus', 'Building', 'Buildings', 'Complex',
    
        # Time/Temporal terms
        'Period', 'Periods', 'Quarter', 'Quarters', 'Year', 'Years', 'Month',
        'Months', 'Week', 'Weeks', 'Day', 'Days', 'Date', 'Dates', 'Time',
        'Timeline', 'Schedule', 'Phase', 'Phases', 'Stage', 'Stages',
        'Cycle', 'Cycles', 'Term', 'Terms', 'Duration', 'Interval', 'Intervals',
    
        # Abstract concepts
        'Concept', 'Concepts', 'Principle', 'Principles', 'Theory', 'Theories',
        'Model', 'Models', 'Method', 'Methods', 'Approach', 'Approaches',
        'Technique', 'Techniques', 'Practice', 'Practices', 'Standard', 'Standards',
        'Quality', 'Efficiency', 'Effectiveness', 'Productivity', 'Capacity',
        'Capability', 'Capabilities', 'Competency', 'Competencies', 'Skill',
        'Skills', 'Expertise', 'Experience', 'Knowledge', 'Understanding',
    
        # Measurement/Quantitative terms
        'Metric', 'Metrics', 'Measure', 'Measures', 'Indicator', 'Indicators',
        'Parameter', 'Parameters', 'Variable', 'Variables', 'Factor', 'Factors',
        'Rate', 'Rates', 'Ratio', 'Ratios', 'Percentage', 'Percent', 'Amount',
        'Amounts', 'Volume', 'Volumes', 'Size', 'Scale', 'Level', 'Levels',
        'Degree', 'Degrees', 'Range', 'Ranges', 'Limit', 'Limits', 'Threshold',
    
        # Document/Communication terms
        'Document', 'Documents', 'Report', 'Reports', 'Statement', 'Statements',
        'Filing', 'Filings', 'Disclosure', 'Disclosures', 'Notice', 'Notices',
        'Announcement', 'Announcements', 'Communication', 'Communications',
        'Message', 'Messages', 'Letter', 'Letters', 'Memo', 'Memorandum',
        'Agreement', 'Agreements', 'Contract', 'Contracts', 'Policy', 'Policies',
    
        # Status/Condition terms
        'Status', 'Condition', 'Conditions', 'State', 'Situation', 'Situations',
        'Position', 'Positions', 'Standing', 'Ranking', 'Classification',
        'Category', 'Categories', 'Type', 'Types', 'Kind', 'Kinds', 'Form',
        'Forms', 'Nature', 'Character', 'Characteristic', 'Characteristics',
    
        # Action/Process terms
        'Action', 'Actions', 'Activity', 'Activities', 'Event', 'Events',
        'Transaction', 'Transactions', 'Transfer', 'Transfers', 'Exchange',
        'Exchanges', 'Trade', 'Trading', 'Purchase', 'Purchases', 'Sale',
        'Sales', 'Acquisition', 'Acquisitions', 'Merger', 'Mergers',
        'Consolidation', 'Restructuring', 'Reorganization', 'Transformation',
    
        # Common adjectives that appear as standalone terms
        'General', 'Specific', 'Primary', 'Secondary', 'Main', 'Principal',
        'Major', 'Minor', 'Senior', 'Junior', 'Executive', 'Administrative',
        'Technical', 'Professional', 'Commercial', 'Industrial', 'Residential',
        'Domestic', 'Foreign', 'Local', 'Regional', 'National', 'International',
        'Global', 'Universal', 'Common', 'Standard', 'Basic', 'Advanced',
        'Premium', 'Superior', 'Excellent', 'Outstanding', 'Exceptional',
    
        # Miscellaneous common non-person terms
        'Item', 'Items', 'Object', 'Objects', 'Thing', 'Things', 'Matter',
        'Issue', 'Issues', 'Problem', 'Problems', 'Solution', 'Solutions',
        'Result', 'Results', 'Outcome', 'Outcomes', 'Effect', 'Effects',
        'Impact', 'Impacts', 'Influence', 'Benefit', 'Benefits', 'Advantage',
        'Advantages', 'Disadvantage', 'Disadvantages', 'Cost', 'Costs',
        'Expense', 'Expenses', 'Fee', 'Fees', 'Charge', 'Charges', 'Price',
        'Prices', 'Worth', 'Value', 'Values'
    }
    
    # Reject if any part is a non-person word
    if any(part in definite_non_person_words for part in name_parts):
        return False
    
    # Must start with capital letters (proper nouns)
    if not all(part[0].isupper() for part in name_parts):
        return False
    
    # No numbers allowed
    if any(char.isdigit() for char in name):
        return False
    
    # No special characters except periods and hyphens
    allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ .-')
    if not all(char in allowed_chars for char in name):
        return False
    
    # Check for typical person name patterns
    # First name should be reasonable length
    first_name = name_parts[0]
    if len(first_name) < 2 or len(first_name) > 12:
        return False
    
    # Last name should be reasonable length
    last_name = name_parts[-1]
    if len(last_name) < 2 or len(last_name) > 15:
        return False
    
    return True

key_patterns = [
    'Revenue', 'CostOfGoods', 'CostOfRevenue', 'GrossProfit', 
    'OperatingIncome', 'NetIncome', 'EarningsPerShare',
    'Assets', 'Cash', 'AccountsReceivable', 'Inventory',
    'Liabilities', 'StockholdersEquity', 'RetainedEarnings',
    'NetCashProvidedByUsedInOperating', 'NetCashProvidedByUsedInInvesting', 
    'NetCashProvidedByUsedInFinancing',
    'WeightedAverageNumberOfShares', 'SharesOutstanding',
    'DepreciationAndAmortization', 'ShareBasedCompensation',
    'InterestExpense', 'IncomeTaxExpense'
]

def extract_underwriters(soup, html_content):
    underwriters = []
    
    # Common underwriter patterns
    underwriter_keywords = [
        'morgan stanley', 'goldman sachs', 'jp morgan', 'jpmorgan', 'citigroup', 'citi',
        'bank of america', 'merrill lynch', 'wells fargo', 'barclays', 'credit suisse',
        'deutsche bank', 'ubs', 'jefferies', 'cowen', 'piper sandler', 'raymond james',
        'william blair', 'stifel', 'canaccord', 'rbc capital', 'bmO capital',
        'evercore', 'lazard', 'moelis', 'centerview'
    ]
    
    # Look for underwriter sections
    text_lower = html_content.lower()
    
    # Find underwriter tables or sections
    tables = soup.find_all('table')
    for table in tables:
        table_text = table.get_text().lower()
        if any(term in table_text for term in ['underwriter', 'book-running manager', 'lead manager']):
            for keyword in underwriter_keywords:
                if keyword in table_text:
                    underwriters.append(keyword.title())
    
    # Also search in general text
    for keyword in underwriter_keywords:
        if keyword in text_lower:
            # Check if it's in an underwriting context
            context_window = 200  # characters around the match
            for match in re.finditer(re.escape(keyword), text_lower):
                start = max(0, match.start() - context_window)
                end = min(len(text_lower), match.end() + context_window)
                context = text_lower[start:end]
                
                if any(term in context for term in ['underwriter', 'book-running', 'lead manager', 'offering']):
                    underwriters.append(keyword.title())
                    break
    
    return list(set(underwriters))

def analyze_prospectus_keywords(soup):
    text = soup.get_text().lower()
    target_keywords = [
        # Technology
        'technology', 'software', 'ai', 'machine learning',
        'cloud', 'saas', 'platform', 'digital', 'data', 'analytics', 'algorithm',
        'automation', 'blockchain', 'cryptocurrency', 'cybersecurity',
        'subscription', 'recurring','e-commerce', 'mobile', 'app', 'virtual',
        
        # Industry specific
        'healthcare', 'biotech', 'pharmaceutical', 'medical', 'clinical',
        'energy', 'renewable', 'solar', 'electric', 'battery',
        'real estate', 'logistics', 'transportation', 'automotive',
    ]
    
    # Count keyword frequencies
    keyword_counts = {}
    
    for kw in target_keywords:
        # Use word boundaries for exact matches, case insensitive
        pattern = r'\b' + re.escape(kw.lower()) + r'\b'
        matches = re.findall(pattern, text)
        keyword_counts[kw] = len(matches)
    
    return list(keyword_counts.items())


def generate_keyword_dataframe(keywords_frequencies):
    # Add word list extraction to rows
    rows = []
    for word_and_frequency in keywords_frequencies:
        row = {
            'Element_Name': word_and_frequency[0],
            'Value': word_and_frequency[1],
            'Data_Type': 'keyword'
        }
        rows.append(row)
       
    return pd.DataFrame(rows)

def generate_names_dataframe(names):
    rows=[]
    for n in names:
        if n[1] == 0:
            continue
        row = {
            'Element_Name': n[0],
            'Value': n[1],
            'Data_Type': 'Person'
        }
        rows.append(row)
    return pd.DataFrame(rows)

def generate_underwriter_dataframe(underwriters):
    rows=[]
    for u in underwriters:
        row = {
            'Element_Name': u,
            'Value': 1,
            'Data_Type': 'underwriter'
        }
        rows.append(row)
    return pd.DataFrame(rows)

def pivot_df(df):
    current_cols = df.set_index('Element_Name')['Value']
    result = pd.concat([current_cols]).to_frame().T
    result = result.dropna(axis=1, how='any')

    return result



def main():
    all_dfs = []
    
    df = pd.read_csv('./data/ipo_day_summary.csv')
    df = df[['symbol', 'url', 'volume', 'day', 'ipo_date', 'open', 'close', 'diff', 'public_price_per_share', 'price_public_total']]
    df['url'] = df['url'].apply(lambda x: x.split('/')[-1])
    
    for tuple in list(df.itertuples(index=False)):
        # Get file coordinates
        dir_name=tuple[0]
        file_name=tuple[1]
        volume=tuple[2]
        day=tuple[3]
        ipo_date=tuple[4]
        open_price=tuple[5]
        close=tuple[6]
        diff=tuple[7]
        public_price_per_share=tuple[8]
        price_public_total=tuple[9]

        # Fetch the file for parsing
        file_path = try_to_get_file(dir_name, file_name) 

        # File may not exist
        if file_path == False:
            continue

        with open(file_path, 'r', encoding='utf-8') as file:
            html_content= file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        keyword_list = analyze_prospectus_keywords(soup)
        names_list = extract_person_names(soup)
        underwriter_list = extract_underwriters(soup, html_content)

        keyword_df = generate_keyword_dataframe(keyword_list)
        names_df = generate_names_dataframe(names_list)
        underwriter_df = generate_underwriter_dataframe(underwriter_list)

        df = pd.concat([keyword_df, names_df, underwriter_df], ignore_index=True)
        pivoted_df = pivot_df(df)
        pivoted_df['Volume'] = volume
        pivoted_df['Day'] = day
        pivoted_df['IPO_Date'] = ipo_date
        pivoted_df['Open'] = open_price
        pivoted_df['Close'] = close
        pivoted_df['Diff'] = diff
        pivoted_df['Public_Price_Per_Share'] = public_price_per_share
        pivoted_df['Price_Public_Total'] = price_public_total
        pivoted_df['symbol'] = dir_name
        pivoted_df.to_csv(f'./data/sec-ipo-files/{dir_name}/word_analysis_2.csv', index=False)
        all_dfs.append(pivoted_df)


    final_df = pd.concat(all_dfs)
    final_df.to_csv(f'./data/full_eda.csv', index=False)

main()