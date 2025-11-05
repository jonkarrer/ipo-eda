import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple, Any, Optional
import logging
from datetime import datetime
import os
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialCSVProcessor:
    def __init__(self):
        self.document_patterns = {
            'spac': ['SPAC', 'Special Purpose', 'Trust Account', 'Business Combination', 'Acquisition Corp'],
            'insurance': ['Premium', 'Claims', 'Underwriting', 'Policy', 'Loss Ratio', 'Combined Ratio'],
            'banking': ['Deposits', 'Loans', 'Credit', 'Interest Income', 'Net Interest Margin'],
            'investment': ['Assets Under Management', 'Investment', 'Portfolio', 'Fund'],
            'prospectus': ['Prospectus', 'Offering', 'Securities', 'Registration'],
            'balance_sheet': ['Balance Sheet', 'Assets', 'Liabilities', 'Equity'],
            'income_statement': ['Income Statement', 'Revenue', 'Net Income', 'Operating Income'],
            'cash_flow': ['Cash Flow', 'Operating Activities', 'Investing Activities', 'Financing Activities']
        }
        
        self.financial_terms = {
            'assets': ['total assets', 'current assets', 'non-current assets', 'fixed assets'],
            'liabilities': ['total liabilities', 'current liabilities', 'long-term liabilities'],
            'equity': ['stockholders equity', 'shareholders equity', 'total equity', 'retained earnings'],
            'revenue': ['total revenue', 'net revenue', 'gross revenue', 'sales', 'income'],
            'expenses': ['total expenses', 'operating expenses', 'cost of goods sold', 'cogs'],
            'ratios': ['debt to equity', 'current ratio', 'quick ratio', 'roe', 'roa']
        }
        
    def identify_document_type(self, df: pd.DataFrame) -> str:
        """Classify document type based on content patterns"""
        try:
            # Convert all cells to string and join
            text_content = ' '.join(df.astype(str).values.flatten()).lower()
            
            scores = {}
            for doc_type, keywords in self.document_patterns.items():
                scores[doc_type] = sum(1 for keyword in keywords 
                                     if keyword.lower() in text_content)
            
            if max(scores.values()) == 0:
                return 'unknown'
                
            return max(scores, key=scores.get)
        except Exception as e:
            logger.error(f"Error identifying document type: {e}")
            return 'unknown'
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare dataframe for processing"""
        try:
            # Remove completely empty rows and columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            # Reset index
            df = df.reset_index(drop=True)
            
            # Replace common null representations
            df = df.replace(['—', '-', '', 'N/A', 'n/a', 'NULL'], np.nan)
            
            return df
        except Exception as e:
            logger.error(f"Error cleaning dataframe: {e}")
            return df
    
    def extract_company_name(self, df: pd.DataFrame) -> str:
        """Extract company name from document"""
        try:
            # Look in first few rows for company name patterns
            for idx in range(min(10, len(df))):
                for col in df.columns:
                    cell_value = str(df.iloc[idx, col])
                    if any(term in cell_value.lower() for term in ['inc.', 'corp.', 'ltd.', 'llc', 'company']):
                        # Clean up the company name
                        name = re.sub(r'[^\w\s\.\,\&\-]', '', cell_value).strip()
                        if len(name) > 3:
                            return name
            return "Unknown Company"
        except Exception as e:
            logger.error(f"Error extracting company name: {e}")
            return "Unknown Company"
    
    def extract_dates(self, df: pd.DataFrame) -> List[str]:
        """Extract dates from the document"""
        dates = []
        date_patterns = [
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',
            r'\b\d{4}-\d{1,2}-\d{1,2}\b',
            r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
            r'\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b'
        ]
        
        try:
            text_content = ' '.join(df.astype(str).values.flatten())
            
            for pattern in date_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                dates.extend(matches)
            
            return list(set(dates))  # Remove duplicates
        except Exception as e:
            logger.error(f"Error extracting dates: {e}")
            return []
    
    def detect_financial_structures(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect key financial data structures"""
        try:
            structures = {
                'balance_sheet': self._find_balance_sheet_data(df),
                'income_statement': self._find_income_statement_data(df),
                'cash_flow': self._find_cash_flow_data(df),
                'key_metrics': self._find_key_metrics(df),
                'dates': self.extract_dates(df),
                'monetary_values': self._extract_monetary_data(df),
                'company_name': self.extract_company_name(df)
            }
            return structures
        except Exception as e:
            logger.error(f"Error detecting financial structures: {e}")
            return {}
    
    def _find_balance_sheet_data(self, df: pd.DataFrame) -> List[Dict]:
        """Extract balance sheet items"""
        balance_sheet_terms = [
            'total assets', 'current assets', 'total liabilities', 'current liabilities',
            'stockholders equity', 'shareholders equity', 'working capital', 
            'cash and equivalents', 'accounts receivable', 'inventory',
            'accounts payable', 'long-term debt', 'retained earnings'
        ]
        
        matches = []
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                for term in balance_sheet_terms:
                    if term in row_text:
                        monetary_values = self._extract_money_from_row(row)
                        if monetary_values:
                            matches.append({
                                'item': term,
                                'row_index': idx,
                                'values': monetary_values,
                                'raw_text': row_text[:100]  # Truncate for readability
                            })
        except Exception as e:
            logger.error(f"Error finding balance sheet data: {e}")
        
        return matches
    
    def _find_income_statement_data(self, df: pd.DataFrame) -> List[Dict]:
        """Extract income statement items"""
        income_terms = [
            'total revenue', 'net revenue', 'gross revenue', 'sales',
            'cost of goods sold', 'gross profit', 'operating expenses',
            'operating income', 'net income', 'earnings per share',
            'diluted eps', 'basic eps'
        ]
        
        matches = []
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                for term in income_terms:
                    if term in row_text:
                        monetary_values = self._extract_money_from_row(row)
                        if monetary_values:
                            matches.append({
                                'item': term,
                                'row_index': idx,
                                'values': monetary_values,
                                'raw_text': row_text[:100]
                            })
        except Exception as e:
            logger.error(f"Error finding income statement data: {e}")
        
        return matches
    
    def _find_cash_flow_data(self, df: pd.DataFrame) -> List[Dict]:
        """Extract cash flow items"""
        cash_flow_terms = [
            'operating activities', 'investing activities', 'financing activities',
            'net cash provided', 'net cash used', 'free cash flow',
            'capital expenditures', 'depreciation'
        ]
        
        matches = []
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                for term in cash_flow_terms:
                    if term in row_text:
                        monetary_values = self._extract_money_from_row(row)
                        if monetary_values:
                            matches.append({
                                'item': term,
                                'row_index': idx,
                                'values': monetary_values,
                                'raw_text': row_text[:100]
                            })
        except Exception as e:
            logger.error(f"Error finding cash flow data: {e}")
        
        return matches
    
    def _find_key_metrics(self, df: pd.DataFrame) -> List[Dict]:
        """Extract key financial metrics and ratios"""
        metric_terms = [
            'debt to equity', 'current ratio', 'quick ratio', 'return on equity',
            'return on assets', 'profit margin', 'gross margin', 'operating margin',
            'price to earnings', 'book value per share'
        ]
        
        matches = []
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                for term in metric_terms:
                    if term in row_text:
                        # Look for ratios (numbers with x, %, or decimal patterns)
                        ratio_values = self._extract_ratios_from_row(row)
                        monetary_values = self._extract_money_from_row(row)
                        
                        if ratio_values or monetary_values:
                            matches.append({
                                'item': term,
                                'row_index': idx,
                                'ratio_values': ratio_values,
                                'monetary_values': monetary_values,
                                'raw_text': row_text[:100]
                            })
        except Exception as e:
            logger.error(f"Error finding key metrics: {e}")
        
        return matches
    
    def _extract_monetary_data(self, df: pd.DataFrame) -> List[Dict]:
        """Extract and standardize monetary values"""
        monetary_pattern = r'\$?[\(]?[\d,]+\.?\d*[\)]?[MmBbKk]?'
        
        monetary_data = []
        try:
            for idx, row in df.iterrows():
                for col_idx, cell in enumerate(row):
                    if pd.isna(cell):
                        continue
                        
                    cell_str = str(cell)
                    matches = re.findall(monetary_pattern, cell_str)
                    
                    for match in matches:
                        cleaned_value = self._clean_monetary_value(match)
                        if cleaned_value is not None:
                            monetary_data.append({
                                'row': idx,
                                'col': col_idx,
                                'raw_value': match,
                                'cleaned_value': cleaned_value,
                                'is_negative': '(' in match or '-' in match
                            })
        except Exception as e:
            logger.error(f"Error extracting monetary data: {e}")
        
        return monetary_data
    
    def _extract_money_from_row(self, row: pd.Series) -> List[float]:
        """Extract monetary values from a pandas Series row"""
        values = []
        try:
            for cell in row:
                if pd.isna(cell):
                    continue
                
                cell_str = str(cell)
                # Look for monetary patterns
                matches = re.findall(r'\$?[\(]?[\d,]+\.?\d*[\)]?[MmBbKk]?', cell_str)
                
                for match in matches:
                    cleaned = self._clean_monetary_value(match)
                    if cleaned is not None:
                        values.append(cleaned)
        except Exception as e:
            logger.error(f"Error extracting money from row: {e}")
        
        return values
    
    def _extract_ratios_from_row(self, row: pd.Series) -> List[float]:
        """Extract ratio values from a pandas Series row"""
        ratios = []
        try:
            for cell in row:
                if pd.isna(cell):
                    continue
                
                cell_str = str(cell)
                # Look for ratio patterns (x.xx, xx%, x.xxx)
                ratio_patterns = [
                    r'\b\d+\.\d+x\b',  # 2.5x
                    r'\b\d+\.\d+%\b',  # 15.5%
                    r'\b\d+\.\d{2,3}\b'  # 1.234
                ]
                
                for pattern in ratio_patterns:
                    matches = re.findall(pattern, cell_str, re.IGNORECASE)
                    for match in matches:
                        try:
                            # Clean and convert
                            cleaned = re.sub(r'[x%]', '', match)
                            ratio_value = float(cleaned)
                            ratios.append(ratio_value)
                        except:
                            continue
        except Exception as e:
            logger.error(f"Error extracting ratios from row: {e}")
        
        return ratios
    
    def _clean_monetary_value(self, value: str) -> Optional[float]:
        """Convert string monetary value to float"""
        try:
            # Handle multipliers
            multiplier = 1
            if value.lower().endswith('k'):
                multiplier = 1_000
                value = value[:-1]
            elif value.lower().endswith('m'):
                multiplier = 1_000_000
                value = value[:-1]
            elif value.lower().endswith('b'):
                multiplier = 1_000_000_000
                value = value[:-1]
            
            # Remove $, commas, parentheses
            cleaned = re.sub(r'[\$,\(\)]', '', value)
            
            # Handle negative values in parentheses
            is_negative = '(' in value
            
            result = float(cleaned) * multiplier
            return -result if is_negative else result
        except:
            return None


class KeyMetricsExtractor:
    def __init__(self):
        self.metric_patterns = {
            'revenue': ['revenue', 'total revenue', 'net revenue', 'sales', 'income'],
            'net_income': ['net income', 'net earnings', 'profit', 'net profit'],
            'total_assets': ['total assets', 'assets'],
            'total_liabilities': ['total liabilities', 'liabilities'],
            'equity': ['stockholders equity', 'shareholders equity', 'total equity'],
            'eps': ['earnings per share', 'diluted eps', 'basic eps'],
            'shares_outstanding': ['shares outstanding', 'weighted average shares']
        }
    
    def extract_key_metrics(self, df: pd.DataFrame, doc_type: str) -> Dict[str, Any]:
        """Extract key financial metrics based on document type"""
        try:
            if doc_type == 'spac':
                return self._extract_spac_metrics(df)
            elif doc_type == 'insurance':
                return self._extract_insurance_metrics(df)
            elif doc_type == 'banking':
                return self._extract_banking_metrics(df)
            else:
                return self._extract_general_metrics(df)
        except Exception as e:
            logger.error(f"Error extracting key metrics: {e}")
            return {}
    
    def _extract_spac_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract SPAC-specific metrics"""
        spac_metrics = {
            'trust_account_value': None,
            'shares_subject_to_redemption': None,
            'working_capital': None,
            'total_assets': None,
            'stockholders_equity': None
        }
        
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                
                if 'trust account' in row_text or 'trust' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        spac_metrics['trust_account_value'] = max(values)
                
                if 'working capital' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        spac_metrics['working_capital'] = values[0]
                
                if 'total assets' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        spac_metrics['total_assets'] = max(values)
                
                if 'stockholders equity' in row_text or 'shareholders equity' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        spac_metrics['stockholders_equity'] = values[0]
        except Exception as e:
            logger.error(f"Error extracting SPAC metrics: {e}")
        
        return spac_metrics
    
    def _extract_insurance_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract insurance-specific metrics"""
        insurance_metrics = {
            'premiums_written': None,
            'premiums_earned': None,
            'losses_incurred': None,
            'combined_ratio': None,
            'loss_ratio': None
        }
        
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                
                if 'premiums written' in row_text or 'net premiums written' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        insurance_metrics['premiums_written'] = max(values)
                
                if 'combined ratio' in row_text:
                    ratios = self._extract_ratios_from_row(row)
                    if ratios:
                        insurance_metrics['combined_ratio'] = ratios[0]
        except Exception as e:
            logger.error(f"Error extracting insurance metrics: {e}")
        
        return insurance_metrics
    
    def _extract_banking_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract banking-specific metrics"""
        banking_metrics = {
            'total_deposits': None,
            'total_loans': None,
            'net_interest_income': None,
            'provision_for_losses': None,
            'tier_1_capital_ratio': None
        }
        
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                
                if 'deposits' in row_text and 'total' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        banking_metrics['total_deposits'] = max(values)
                
                if 'loans' in row_text and 'total' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        banking_metrics['total_loans'] = max(values)
        except Exception as e:
            logger.error(f"Error extracting banking metrics: {e}")
        
        return banking_metrics
    
    def _extract_general_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract general financial metrics"""
        general_metrics = {
            'total_revenue': None,
            'net_income': None,
            'total_assets': None,
            'total_liabilities': None,
            'stockholders_equity': None
        }
        
        try:
            for idx, row in df.iterrows():
                row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                
                # Revenue
                if any(term in row_text for term in ['total revenue', 'net revenue', 'revenue']):
                    values = self._extract_money_from_row(row)
                    if values:
                        general_metrics['total_revenue'] = max(values)
                
                # Net Income
                if 'net income' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        general_metrics['net_income'] = values[0]
                
                # Assets
                if 'total assets' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        general_metrics['total_assets'] = max(values)
                
                # Liabilities
                if 'total liabilities' in row_text:
                    values = self._extract_money_from_row(row)
                    if values:
                        general_metrics['total_liabilities'] = max(values)
                
                # Equity
                if any(term in row_text for term in ['stockholders equity', 'shareholders equity']):
                    values = self._extract_money_from_row(row)
                    if values:
                        general_metrics['stockholders_equity'] = values[0]
        except Exception as e:
            logger.error(f"Error extracting general metrics: {e}")
        
        return general_metrics
    
    def _extract_money_from_row(self, row: pd.Series) -> List[float]:
        """Extract monetary values from a pandas Series row"""
        values = []
        try:
            for cell in row:
                if pd.isna(cell):
                    continue
                
                cell_str = str(cell)
                matches = re.findall(r'\$?[\(]?[\d,]+\.?\d*[\)]?[MmBbKk]?', cell_str)
                
                for match in matches:
                    cleaned = self._clean_monetary_value(match)
                    if cleaned is not None:
                        values.append(cleaned)
        except Exception as e:
            logger.error(f"Error extracting money from row: {e}")
        
        return values
    
    def _extract_ratios_from_row(self, row: pd.Series) -> List[float]:
        """Extract ratio values from a pandas Series row"""
        ratios = []
        try:
            for cell in row:
                if pd.isna(cell):
                    continue
                
                cell_str = str(cell)
                ratio_patterns = [
                    r'\b\d+\.\d+x\b',
                    r'\b\d+\.\d+%\b',
                    r'\b\d+\.\d{2,3}\b'
                ]
                
                for pattern in ratio_patterns:
                    matches = re.findall(pattern, cell_str, re.IGNORECASE)
                    for match in matches:
                        try:
                            cleaned = re.sub(r'[x%]', '', match)
                            ratio_value = float(cleaned)
                            ratios.append(ratio_value)
                        except:
                            continue
        except Exception as e:
            logger.error(f"Error extracting ratios from row: {e}")
        
        return ratios
    
    def _clean_monetary_value(self, value: str) -> Optional[float]:
        """Convert string monetary value to float"""
        try:
            multiplier = 1
            if value.lower().endswith('k'):
                multiplier = 1_000
                value = value[:-1]
            elif value.lower().endswith('m'):
                multiplier = 1_000_000
                value = value[:-1]
            elif value.lower().endswith('b'):
                multiplier = 1_000_000_000
                value = value[:-1]
            
            cleaned = re.sub(r'[\$,\(\)]', '', value)
            is_negative = '(' in value
            result = float(cleaned) * multiplier
            return -result if is_negative else result
        except:
            return None


class FinancialReportGenerator:
    def generate_summary_report(self, processed_data: Dict[str, Any]) -> str:
        """Generate executive summary from processed data"""
        try:
            doc_type = processed_data.get('document_type', 'Unknown')
            company_name = processed_data.get('company_name', 'Unknown Company')
            
            report = f"# Financial Document Analysis: {company_name}\n\n"
            report += f"**Document Type:** {doc_type.title()}\n"
            report += f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            # Key Financial Metrics
            metrics = processed_data.get('key_metrics', {})
            if metrics and any(v is not None for v in metrics.values()):
                report += "## Key Financial Metrics\n\n"
                for metric, value in metrics.items():
                    if value is not None:
                        formatted_value = self._format_financial_value(value)
                        report += f"* **{metric.replace('_', ' ').title()}:** {formatted_value}\n"
                report += "\n"
            
            # Balance Sheet Summary
            balance_sheet = processed_data.get('balance_sheet', [])
            if balance_sheet:
                report += "## Balance Sheet Highlights\n\n"
                for item in balance_sheet[:5]:  # Top 5 items
                    if item.get('values'):
                        formatted_values = [self._format_financial_value(v) for v in item['values']]
                        report += f"* **{item['item'].title()}:** {', '.join(formatted_values)}\n"
                report += "\n"
            
            # Income Statement Summary
            income_statement = processed_data.get('income_statement', [])
            if income_statement:
                report += "## Income Statement Highlights\n\n"
                for item in income_statement[:5]:
                    if item.get('values'):
                        formatted_values = [self._format_financial_value(v) for v in item['values']]
                        report += f"* **{item['item'].title()}:** {', '.join(formatted_values)}\n"
                report += "\n"
            
            # Key Dates
            dates = processed_data.get('dates', [])
            if dates:
                report += "## Key Dates\n\n"
                for date in dates[:3]:  # Show first 3 dates
                    report += f"* {date}\n"
                report += "\n"
            
            # Summary Statistics
            monetary_values = processed_data.get('monetary_values', [])
            if monetary_values:
                values = [item['cleaned_value'] for item in monetary_values if item['cleaned_value'] is not None]
                if values:
                    report += "## Document Statistics\n\n"
                    report += f"* **Total Monetary Values Found:** {len(values)}\n"
                    report += f"* **Largest Value:** {self._format_financial_value(max(values))}\n"
                    report += f"* **Smallest Value:** {self._format_financial_value(min(values))}\n"
            
            return report
        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return f"Error generating report: {str(e)}"
    
    def _format_financial_value(self, value: float) -> str:
        """Format financial values for readability"""
        try:
            if pd.isna(value):
                return "N/A"
            
            abs_value = abs(value)
            sign = "-" if value < 0 else ""
            
            if abs_value >= 1_000_000_000:
                return f"{sign}${abs_value/1_000_000_000:.1f}B"
            elif abs_value >= 1_000_000:
                return f"{sign}${abs_value/1_000_000:.1f}M"
            elif abs_value >= 1_000:
                return f"{sign}${abs_value/1_000:.1f}K"
            else:
                return f"{sign}${abs_value:,.2f}"
        except:
            return str(value)


class BatchFinancialProcessor:
    def __init__(self):
        self.processor = FinancialCSVProcessor()
        self.extractor = KeyMetricsExtractor()
        self.reporter = FinancialReportGenerator()
    
    def process_single_csv(self, csv_file: str) -> Dict[str, Any]:
        """Process a single CSV file"""
        try:
            logger.info(f"Processing file: {csv_file}")
            
            # Load CSV with error handling
            try:
                df = pd.read_csv(csv_file, header=None, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(csv_file, header=None, encoding='latin-1')
            except Exception as e:
                return {'error': f"Failed to read CSV: {str(e)}", 'status': 'failed'}
            
            # Clean dataframe
            df = self.processor.clean_dataframe(df)
            
            if df.empty:
                return {'error': 'CSV file is empty after cleaning', 'status': 'failed'}
            
            # Identify document type
            doc_type = self.processor.identify_document_type(df)
            
            # Extract structures and metrics
            structures = self.processor.detect_financial_structures(df)
            key_metrics = self.extractor.extract_key_metrics(df, doc_type)
            
            # Combine all data
            processed_data = {
                'document_type': doc_type,
                'file_name': os.path.basename(csv_file),
                'file_path': csv_file,
                'key_metrics': key_metrics,
                **structures
            }
            
            # Generate summary
            summary = self.reporter.generate_summary_report(processed_data)
            
            return {
                'processed_data': processed_data,
                'summary': summary,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")
            return {
                'error': str(e),
                'status': 'failed',
                'file_name': os.path.basename(csv_file) if csv_file else 'unknown'
            }
    
    def process_csv_batch(self, csv_files: List[str], output_dir: str = None) -> Dict[str, Any]:
        """Process multiple CSV files and generate consolidated insights"""
        results = {}
        successful_processes = 0
        failed_processes = 0
        
        logger.info(f"Starting batch processing of {len(csv_files)} files")
        
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"Processing file {i}/{len(csv_files)}: {csv_file}")
            
            result = self.process_single_csv(csv_file)
            results[csv_file] = result
            
            if result['status'] == 'success':
                successful_processes += 1
            else:
                failed_processes += 1
            
            # Save individual results if output directory specified
            if output_dir and result['status'] == 'success':
                self._save_individual_result(result, csv_file, output_dir)
        
        # Generate batch summary
        batch_summary = self._generate_batch_summary(results, successful_processes, failed_processes)
        
        logger.info(f"Batch processing complete. Success: {successful_processes}, Failed: {failed_processes}")
        
        return {
            'results': results,
            'batch_summary': batch_summary,
            'stats': {
                'total_files': len(csv_files),
                'successful': successful_processes,
                'failed': failed_processes,
                'success_rate': successful_processes / len(csv_files) * 100 if csv_files else 0
            }
        }
    
    def process_directory(self, directory_path: str, output_dir: str = None) -> Dict[str, Any]:
        """Process all CSV files in a directory"""
        try:
            directory = Path(directory_path)
            if not directory.exists():
                raise FileNotFoundError(f"Directory not found: {directory_path}")
            
            csv_files = list(directory.glob("*.csv"))
            
            if not csv_files:
                logger.warning(f"No CSV files found in {directory_path}")
                return {'error': 'No CSV files found', 'status': 'failed'}
            
            logger.info(f"Found {len(csv_files)} CSV files in {directory_path}")
            
            csv_file_paths = [str(f) for f in csv_files]
            return self.process_csv_batch(csv_file_paths, output_dir)
            
        except Exception as e:
            logger.error(f"Error processing directory {directory_path}: {e}")
            return {'error': str(e), 'status': 'failed'}
    
    def _save_individual_result(self, result: Dict[str, Any], csv_file: str, output_dir: str):
        """Save individual processing result to file"""
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            file_name = Path(csv_file).stem
            output_file = output_path / f"{file_name}_analysis.txt"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(result['summary'])
            
            logger.info(f"Saved analysis to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving result for {csv_file}: {e}")
    
    def _generate_batch_summary(self, results: Dict[str, Any], successful: int, failed: int) -> str:
        """Generate summary of batch processing results"""
        try:
            summary = f"# Batch Processing Summary\n\n"
            summary += f"**Processing Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            summary += f"**Total Files:** {successful + failed}\n"
            summary += f"**Successful:** {successful}\n"
            summary += f"**Failed:** {failed}\n"
            summary += f"**Success Rate:** {successful/(successful+failed)*100:.1f}%\n\n"
            
            # Document type distribution
            doc_types = {}
            for file_path, result in results.items():
                if result['status'] == 'success':
                    doc_type = result['processed_data'].get('document_type', 'unknown')
                    doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
            
            if doc_types:
                summary += "## Document Type Distribution\n\n"
                for doc_type, count in sorted(doc_types.items(), key=lambda x: x[1], reverse=True):
                    summary += f"* **{doc_type.title()}:** {count} files\n"
                summary += "\n"
            
            # Failed files
            failed_files = [result.get('file_name', 'unknown') for result in results.values() 
                          if result['status'] == 'failed']
            
            if failed_files:
                summary += "## Failed Files\n\n"
                for file_name in failed_files[:10]:  # Show first 10 failed files
                    summary += f"* {file_name}\n"
                if len(failed_files) > 10:
                    summary += f"* ... and {len(failed_files) - 10} more\n"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating batch summary: {e}")
            return f"Error generating batch summary: {str(e)}"


# Usage Examples and Main Function
def main():
    """Main function demonstrating usage"""
    
    # Initialize the batch processor
    batch_processor = BatchFinancialProcessor()
    
    # Example 1: Process a single CSV file
    print("=== Processing Single File ===")
    single_result = batch_processor.process_single_csv("sample_financial_data.csv")
    if single_result['status'] == 'success':
        print(single_result['summary'])
    else:
        print(f"Error: {single_result['error']}")
    
    # Example 2: Process multiple specific files
    print("\n=== Processing Multiple Files ===")
    csv_files = [
        "spac_filing.csv",
        "insurance_report.csv", 
        "bank_statement.csv"
    ]
    
    batch_results = batch_processor.process_csv_batch(csv_files, output_dir="analysis_output")
    
    print(f"Batch processing complete!")
    print(f"Success rate: {batch_results['stats']['success_rate']:.1f}%")
    print("\nBatch Summary:")
    print(batch_results['batch_summary'])
    
    # Example 3: Process entire directory
    print("\n=== Processing Directory ===")
    directory_results = batch_processor.process_directory(
        directory_path="./financial_csvs",
        output_dir="./analysis_results"
    )
    
    if directory_results.get('status') != 'failed':
        print(f"Processed {directory_results['stats']['total_files']} files")
        print(f"Success rate: {directory_results['stats']['success_rate']:.1f}%")


if __name__ == "__main__":
    main()

processor = BatchFinancialProcessor()
result = processor.process_single_csv("./data/sec-ipo-finance/AAC/financial/combined.csv")
print(result)