from bs4 import BeautifulSoup
from collections import Counter
import re
import pandas as pd

def analyze_prospectus_keywords(keyword, target_keywords=None):
    # Load HTML
    path = f'./sec-docs/{keyword}.html'
    with open(path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    
    text = soup.get_text().lower()
    
    # Default technology/business keywords if none provided
    if target_keywords is None:
        target_keywords = [
            # Technology
            'technology', 'software', 'ai', 'machine learning',
            'cloud', 'saas', 'platform', 'digital', 'data', 'analytics', 'algorithm',
            'automation', 'blockchain', 'cryptocurrency', 'fintech', 'cybersecurity',
            'subscription', 'recurring', 'marketplace',
            'e-commerce', 'mobile', 'app', 'virtual',
            
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
    
    # Create DataFrame
    df = pd.DataFrame(list(keyword_counts.items()), 
                     columns=['keyword', 'frequency'])
    df = df.sort_values('frequency', ascending=False)
    return df

def analyze_prospectus_words_two(keyword, top_n=50):
    # Load HTML
    path = f'./sec-docs/{keyword}.html'
    with open(path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    
    text = soup.get_text()
    
    # Basic stopwords (you can expand this)
    stopwords = {
        'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before',
        'after', 'above', 'below', 'between', 'among', 'is', 'are', 'was',
        'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does',
        'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
        'can', 'shall', 'a', 'an', 'this', 'that', 'these', 'those', 'we',
        'us', 'our', 'ours', 'you', 'your', 'yours', 'he', 'him', 'his',
        'she', 'her', 'hers', 'it', 'its', 'they', 'them', 'their', 'theirs',
        'i', 'me', 'my', 'mine', 'who', 'whom', 'whose', 'which', 'what',
        'where', 'when', 'why', 'how', 'if', 'then', 'than', 'so', 'as',
        'such', 'both', 'either', 'neither', 'not', 'no', 'nor', 'only',
        'own', 'same', 'few', 'more', 'most', 'other', 'some', 'any', 'each',
        'every', 'all', 'both', 'half', 'many', 'much', 'several', 'since'
    }
    
    # Financial document specific stopwords
    financial_stopwords = {
        'company', 'business', 'financial', 'year', 'years', 'million', 
        'thousand', 'billion', 'including', 'related', 'pursuant', 'section',
        'table', 'see', 'page', 'footnote', 'december', 'january', 'february',
        'march', 'april', 'june', 'july', 'august', 'september', 'october',
        'november', 'ended', 'period', 'periods', 'fiscal', 'quarter',
        'quarters', 'month', 'months', 'date', 'approximately', 'significant',
        'various', 'certain', 'generally', 'primarily', 'substantially', 'stock', 'common',
        'shares', 'srt', 'securities', 'agreement', 'scenariopreviouslyreportedmember', 'gaap', 'net',
        'purchase', 'under', 'inc', 'llc', 'holdings', 'statements', 'operations', 'assets', 'cash', 'value',
        'loss', 'sales', 'current', 'information', 'rime', 'semicab', 'warrants', 'amount', 'price',
        'prospectus', 'expenses', 'results', 'consolidated', 'paid', 'interest', 'outstanding', 
        'offering', 'based', 'equity', 'operating', 'liabilities', 'note', 'streeterville', 'pre',
        'smcb', 'directors', 'issued', 'total', 'series', 'machine', 'subject', 'use', 'balance', 'capital',
        'tax', 'liability', 'time', 'singing', 'notes', 'filed', 'additional', 'services', 'due', 'board',
        'revenue', 'management', 'result', 'sec', 'future', 'number', 'share', 'per', 'three',
        'risks', 'transaction', 'issuance', 'customers', 'public', 'market', 'statement', 'entered', 'form', 'income',
        'impact', 'exercise', 'upon', 'six', 'expense', 'material', 'fair', 'also', 'available', 'lease',
        'proceeds', 'connection', 'executive', 'compensation', 'stingray', 'registration', 'accounting', 'costs',
        'party', 'payable', 'loan', 'stockholders', 'increase', 'act', 'officer', 'sale'
    }
    
    stopwords.update(financial_stopwords)
    
    # Extract words (only alphabetic, length > 2)
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    
    # Filter out stopwords
    filtered_words = [word for word in words if word not in stopwords]
    
    # Count words
    word_counts = Counter(filtered_words)
    
    # Create DataFrame for better analysis
    df = pd.DataFrame(word_counts.most_common(top_n), 
                     columns=['word', 'frequency'])
    
    return df, word_counts

# Usage
keyword = 'rime'
word_df = analyze_prospectus_keywords(keyword, None)

print("Most common words in the prospectus:")
print(word_df)

# Save to CSV
word_df.to_csv(f'./analysis/{keyword}_word_frequency.csv', index=False)