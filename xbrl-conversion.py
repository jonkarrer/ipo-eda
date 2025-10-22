from secxbrl import parse_inline_xbrl
import pandas as pd

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
