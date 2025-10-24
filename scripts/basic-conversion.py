from secxbrl import parse_inline_xbrl
import pandas as pd

from bs4 import BeautifulSoup
import re

# Add this after you load the content but before parsing XBRL
def extract_offering_price_table(content):
    """Extract the public offering price table from HTML"""
    
    soup = BeautifulSoup(content, 'html.parser')
    
    # Find table containing "Public offering price"
    offering_data = {}
    
    # Look for the text "Public offering price" in the document
    for element in soup.find_all(text=re.compile(r'Public offering price', re.IGNORECASE)):
        # Find the parent table
        table = element.find_parent('table')
        if table:
            rows = table.find_all('tr')
            
            # Find header row with "Per Unit" and "Total"
            header_row = None
            for row in rows:
                cells = row.find_all(['th', 'td'])
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                if any('per unit' in text.lower() for text in cell_texts) and any('total' in text.lower() for text in cell_texts):
                    header_row = row
                    break
            
            if header_row:
                # Get column indices
                header_cells = header_row.find_all(['th', 'td'])
                per_unit_idx = None
                total_idx = None
                
                for i, cell in enumerate(header_cells):
                    text = cell.get_text(strip=True).lower()
                    if 'per unit' in text:
                        per_unit_idx = i
                    elif 'total' in text:
                        total_idx = i
                
                # Extract data rows
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    if len(cells) > max(per_unit_idx or 0, total_idx or 0):
                        row_label = cells[0].get_text(strip=True)
                        
                        if 'public offering price' in row_label.lower():
                            if per_unit_idx is not None:
                                offering_data['public_offering_price_per_unit'] = cells[per_unit_idx].get_text(strip=True)
                            if total_idx is not None:
                                offering_data['public_offering_price_total'] = cells[total_idx].get_text(strip=True)
                        
                        # You can add more rows here if needed
                        elif 'underwriting discount' in row_label.lower():
                            if per_unit_idx is not None:
                                offering_data['underwriting_discount_per_unit'] = cells[per_unit_idx].get_text(strip=True)
                            if total_idx is not None:
                                offering_data['underwriting_discount_total'] = cells[total_idx].get_text(strip=True)
                        
                        elif 'proceeds' in row_label.lower():
                            if per_unit_idx is not None:
                                offering_data['proceeds_per_unit'] = cells[per_unit_idx].get_text(strip=True)
                            if total_idx is not None:
                                offering_data['proceeds_total'] = cells[total_idx].get_text(strip=True)
            
            break  # Stop after finding the first matching table
    
    return offering_data


keyword = 'rime'

# load data
path = f'./sec-docs/{keyword}.html'
with open(path,'rb') as f:
    content = f.read()

# Define the important financial elements to filter on
important_elements = [
    # Core Financial Performance
    'RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues',
    'CostOfGoodsAndServicesSold', 'CostOfRevenue',
    'GrossProfit', 'OperatingIncomeLoss', 'NetIncomeLoss', 'ProfitLoss',
    'EarningsPerShareBasic', 'EarningsPerShareDiluted',
    
    # Balance Sheet Fundamentals
    'Assets', 'AssetsCurrent', 'Cash',
    'AccountsReceivableNet', 'InventoryNet',
    'Liabilities', 'LiabilitiesCurrent',
    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    'RetainedEarningsAccumulatedDeficit',
    
    # Cash Flow Essentials
    'NetCashProvidedByUsedInOperatingActivities',
    'NetCashProvidedByUsedInInvestingActivities',
    'NetCashProvidedByUsedInFinancingActivities',
    
    # Key Metrics
    'WeightedAverageNumberOfSharesOutstandingBasic',
    'SharesOutstanding',
    
    # Important Operational Items
    'DepreciationAndAmortization', 'ShareBasedCompensation',
    'InterestExpense', 'IncomeTaxExpenseBenefit',
    
    # Red Flags
    'GoodwillImpairmentLoss', 'AssetImpairmentCharges',
    'AllowanceForDoubtfulAccountsReceivableCurrent'
]

# Define key patterns to match
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

# Add this line after loading content and before parsing XBRL
# offering_price_data = extract_offering_price_table(content)
# print("Offering Price Data:", offering_price_data)

# You can also save this data
# import json
# with open(f'./offering_data/{keyword}_offering_price.json', 'w') as f:
#     json.dump(offering_price_data, f, indent=2)

# parse data
ix = parse_inline_xbrl(content)
with open(f'./xbrl/{keyword}.txt','w', encoding='utf-8') as f:
    f.writelines([str(item)+'\n\n' for item in ix])

# Flatten the data into a spreadsheet format
rows = []
for item in ix:
    row = {
        'Fact_ID': item['_attributes'].get('id', ''),
        'Element_Name': item['_attributes'].get('name', ''),
        'Value': item.get('_val', ''),
        'Unit': item['_attributes'].get('unitref', ''),
        'Is_Null': item['_attributes'].get('xs:nil', 'false'),
        'Context_Ref': item['_context'].get('_contextref', ''),
        'Entity_ID': item['_context'].get('context_entity_identifier', ''),
        'Period_Start': item['_context'].get('context_period_startdate', ''),
        'Period_End': item['_context'].get('context_period_enddate', ''),
        'Instant_Date': item['_context'].get('context_period_instant', ''),
        'Taxonomy_Prefix': item['_attributes'].get('name', '').split(':')[0] if ':' in item['_attributes'].get('name', '') else '',
        'Element_Local_Name': item['_attributes'].get('name', '').split(':')[-1] if ':' in item['_attributes'].get('name', '') else item['_attributes'].get('name', '')
    }
    rows.append(row)

# Create DataFrame
df = pd.DataFrame(rows)

# Cleanup
df = df[df['Is_Null'] == 'false'] # Drop rows with null = true
df = df[df['Taxonomy_Prefix'] == 'us-gaap'] # Only want us-gaap data
df['Value'] = pd.to_numeric(df['Value'].str.replace(',', ''), errors='coerce') # Convert to number, otherwise drop string info
df = df.dropna(subset=['Value']) # Drop nulls
df = df.drop_duplicates()
df['Element_Name'] = df['Element_Name'].str.replace("us-gaap:", '') # Remove us-gaap prefix

# Create a pattern-based filter
pattern = '|'.join(key_patterns)
df_filtered = df[df['Element_Local_Name'].str.contains(pattern, case=False, na=False)].copy()
df_deduped = df_filtered.drop_duplicates(
    subset=['Element_Local_Name', 'Period_Start', 'Period_End', 'Instant_Date', 'Entity_ID'],
    keep='first'
)
# Or save to CSV
df_deduped.to_csv(f'./csv/{keyword}.csv', index=False)
