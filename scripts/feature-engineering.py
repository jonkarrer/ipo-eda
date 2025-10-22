# Useful metrics from dataframe
# AccountsReceivableNet
# AccruedLiabilitiesCurrent
# AccruedLiabilitiesCurrentAndNoncurrent
# Assets
# AssetsCurrent
# Cash
# CommonStockSharesOutstanding
# CostOfGoodsAndServicesSold
# CostOfRevenue
# DeferredTaxAssetsGross
# DeferredTaxAssetsNet
# DeferredTaxLiabilities
# DepreciationAndAmortization
# EarningsPerShareBasic
# EarningsPerShareDiluted
# EmployeeBenefitsAndShareBasedCompensation
# GrossProfit
# InterestExpense
# InterestExpenseCapitalSecurities
# InterestExpenseLongTermDebt
# InventoryNet
# Liabilities
# LiabilitiesCurrent
# NetIncomeLoss
# NetIncomeLossAttributableToNoncontrollingInterest
# NetIncomeLossAvailableToCommonStockholdersBasic
# NonoperatingIncomeExpense
# OperatingIncomeLoss
# OtherAssetsCurrent
# OtherAssetsNoncurrent
# OtherCostOfOperatingRevenue
# OtherLiabilitiesCurrent
# RetainedEarningsAccumulatedDeficit
# Revenues
# SharesOutstanding

def create_financial_ratios(df):
    """
    Transform raw financial statement items into meaningful ratios
    """
    # Profitability Ratios
    df['gross_margin'] = df['GrossProfit'] / df['Revenues']
    df['operating_margin'] = df['OperatingIncomeLoss'] / df['Revenues']
    df['net_margin'] = df['NetIncomeLoss'] / df['Revenues']
    df['roa'] = df['NetIncomeLoss'] / df['Assets']
    df['roe'] = df['NetIncomeLoss'] / (df['Assets'] - df['Liabilities'])
    
    # Efficiency Ratios
    df['asset_turnover'] = df['Revenues'] / df['Assets']
    df['receivables_turnover'] = df['Revenues'] / df['AccountsReceivableNet']
    df['inventory_turnover'] = df['CostOfGoodsAndServicesSold'] / df['InventoryNet']
    
    # Liquidity Ratios
    df['current_ratio'] = df['AssetsCurrent'] / df['LiabilitiesCurrent']
    df['quick_ratio'] = (df['AssetsCurrent'] - df['InventoryNet']) / df['LiabilitiesCurrent']
    df['cash_ratio'] = df['Cash'] / df['LiabilitiesCurrent']
    
    # Leverage Ratios
    df['debt_to_assets'] = df['Liabilities'] / df['Assets']
    df['debt_to_equity'] = df['Liabilities'] / (df['Assets'] - df['Liabilities'])
    
    # Per-Share Metrics
    df['book_value_per_share'] = (df['Assets'] - df['Liabilities']) / df['SharesOutstanding']
    df['revenue_per_share'] = df['Revenues'] / df['SharesOutstanding']
    df['cash_per_share'] = df['Cash'] / df['SharesOutstanding']
    
    # Working Capital Metrics
    df['working_capital'] = df['AssetsCurrent'] - df['LiabilitiesCurrent']
    df['working_capital_ratio'] = df['working_capital'] / df['Revenues']
    
    # Cost Structure
    df['cogs_ratio'] = df['CostOfGoodsAndServicesSold'] / df['Revenues']
    df['employee_cost_ratio'] = df['EmployeeBenefitsAndShareBasedCompensation'] / df['Revenues']
    
    return df

def create_valuation_features(df, ipo_price, peer_multiples):
    """
    Create valuation-based features for IPO prediction
    """
    # Calculate market cap at IPO price
    df['market_cap_at_ipo'] = ipo_price * df['SharesOutstanding']
    
    # Valuation Multiples
    df['price_to_sales'] = df['market_cap_at_ipo'] / df['Revenues']
    df['price_to_book'] = ipo_price / df['book_value_per_share']
    df['price_to_earnings'] = ipo_price / df['EarningsPerShareBasic']  # Handle negatives
    
    # Enterprise Value
    df['enterprise_value'] = df['market_cap_at_ipo'] + df['Liabilities'] - df['Cash']
    df['ev_to_revenue'] = df['enterprise_value'] / df['Revenues']
    df['ev_to_ebitda'] = df['enterprise_value'] / (df['OperatingIncomeLoss'] + df['DepreciationAndAmortization'])
    
    # Relative to Peer Multiples (you'll need to collect peer data)
    df['ps_vs_peers'] = df['price_to_sales'] / peer_multiples['median_ps']
    df['pb_vs_peers'] = df['price_to_book'] / peer_multiples['median_pb']
    df['ev_rev_vs_peers'] = df['ev_to_revenue'] / peer_multiples['median_ev_rev']
    
    return df

def create_quality_scores(df):
    """
    Create composite quality scores
    """
    # Profitability Quality Score (0-100)
    profit_components = ['gross_margin', 'operating_margin', 'net_margin', 'roa', 'roe']
    df['profitability_score'] = 0
    
    for component in profit_components:
        if component in df.columns:
            # Convert to percentile rank (0-1) then scale to 0-20
            df['profitability_score'] += df[component].rank(pct=True) * 20
    
    # Financial Health Score (0-100)
    health_components = ['current_ratio', 'quick_ratio', 'cash_ratio']
    df['financial_health_score'] = 0
    
    for component in health_components:
        if component in df.columns:
            df['financial_health_score'] += np.minimum(df[component].rank(pct=True) * 33.33, 33.33)
    
    # Efficiency Score (0-100)
    efficiency_components = ['asset_turnover', 'receivables_turnover', 'inventory_turnover']
    df['efficiency_score'] = 0
    
    for component in efficiency_components:
        if component in df.columns and df[component].notna().any():
            df['efficiency_score'] += df[component].rank(pct=True) * 33.33
    
    return df

def create_ipo_specific_features(df):
    """
    Features specific to IPO analysis
    """
    # Cash Position Strength
    df['cash_runway_months'] = df['Cash'] / (df['OperatingIncomeLoss'].abs() / 12)  # Assuming monthly burn
    df['cash_as_pct_of_market_cap'] = df['Cash'] / df['market_cap_at_ipo']
    
    # Growth Efficiency
    df['revenue_per_employee'] = df['Revenues'] / (df['EmployeeBenefitsAndShareBasedCompensation'] / 50000)  # Rough estimate
    
    # Capital Intensity
    df['capex_intensity'] = df['DepreciationAndAmortization'] / df['Revenues']  # Proxy for capital intensity
    
    # Dilution Metrics
    df['dilution_ratio'] = df['EarningsPerShareDiluted'] / df['EarningsPerShareBasic']
    
    # Interest Coverage (ability to service debt)
    df['interest_coverage'] = df['OperatingIncomeLoss'] / np.maximum(df['InterestExpense'], 0.001)
    
    # Working Capital Efficiency
    df['days_sales_outstanding'] = (df['AccountsReceivableNet'] / df['Revenues']) * 365
    df['days_inventory_outstanding'] = (df['InventoryNet'] / df['CostOfGoodsAndServicesSold']) * 365
    
    return df