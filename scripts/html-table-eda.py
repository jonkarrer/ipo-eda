import pandas as pd
import re
from bs4 import BeautifulSoup
import nltk
from nltk import ne_chunk, pos_tag, word_tokenize

# nltk.download('punkt_tab')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('maxent_ne_chunker')
# nltk.download('words')
# nltk.download('averaged_perceptron_tagger_eng')
# nltk.download('maxent_ne_chunker_tab')

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
            
            check = cross_check(name)
            if not check:
                continue

            pattern = r'\b' + re.escape(name) + r'\b'
            matches = re.findall(pattern, text)
            keyword_counts[name] = len(matches)  
   
    return list(keyword_counts.items())  

def cross_check(name):
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
        'Further', 'Highly', 'Known', 'Trends', 'Ability', 'Islands', 'Cayman'
    }
    
    # Reject if any part is a non-person word
    if any(part in definite_non_person_words for part in name_parts):
        return False
    
    # Reject single words that are clearly not names
    single_word_rejects = {
        'See', 'Unit', 'Risk', 'Ares', 'Power', 'Energy', 'Global', 'Smart',
        'Floor', 'Board', 'Class', 'Rule', 'Legal', 'Due', 'Impact', 'System',
        'Initial', 'Current', 'Annual', 'Market', 'Exchange', 'Director',
        'Income', 'Shares', 'Business', 'Target', 'Limited', 'Special',
        'Infrastructure', 'Strategic', 'Credit', 'Private', 'Corporate',
        'Direct', 'Dynamic', 'Robust', 'Scaled', 'Known', 'Ability',
        'Unless', 'Except', 'Further', 'Highly', 'Support', 'Principal',
        'Adjustments', 'Rules', 'Includes', 'Risks', 'Directors', 'Nominees',
        'Considerations', 'Trends', 'Oxley'
    }
    
    if name in single_word_rejects:
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
    """Extract underwriter information"""
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

def analyze_prospectus_keywords(text):
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
            'Element_Name': 'Underwriter',
            'Value': u,
            'Data_Type': 'underwriter'
        }
        rows.append(row)
    return pd.DataFrame(rows)

def main():
    file_path = './data/sec-ipo-files/AAC/d61603d424b4.htm'
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content= file.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
   
    text_content = soup.get_text().lower()

    keyword_list = analyze_prospectus_keywords(text_content)
    names_list = extract_person_names(soup)
    underwriter_list = extract_underwriters(soup, html_content)

    keyword_df = generate_keyword_dataframe(keyword_list)
    names_df = generate_names_dataframe(names_list)
    underwriter_df = generate_underwriter_dataframe(underwriter_list)

    df = pd.concat([keyword_df, names_df, underwriter_df], ignore_index=True)
    df.to_csv('./data/table-parser.csv', index=False)


main()