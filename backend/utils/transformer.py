import pandas as pd
import re
from datetime import datetime
import numpy as np

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize column names"""
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def detect_numeric_columns(df: pd.DataFrame) -> list:
    """Detect numeric columns in the dataframe"""
    return df.select_dtypes(include=[np.number]).columns.tolist()

def detect_string_columns(df: pd.DataFrame) -> list:
    """Detect string/object columns in the dataframe"""
    return df.select_dtypes(include=['object', 'string']).columns.tolist()

def detect_date_columns(df: pd.DataFrame) -> list:
    """Detect potential date columns"""
    date_columns = []
    for col in df.columns:
        if any(date_word in col.lower() for date_word in ['date', 'time', 'created', 'updated', 'timestamp']):
            date_columns.append(col)
    return date_columns

def apply_mathematical_operations(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply mathematical operations based on prompt"""
    df_copy = df.copy()
    
    # Calculate totals/sums
    if any(word in prompt.lower() for word in ["total", "sum", "add up", "calculate sum"]):
        numeric_cols = detect_numeric_columns(df_copy)
        if len(numeric_cols) >= 2:
            df_copy["total_amount"] = df_copy[numeric_cols].sum(axis=1)
        elif len(numeric_cols) == 1:
            df_copy["total_amount"] = df_copy[numeric_cols[0]]
    
    # Calculate averages
    if any(word in prompt.lower() for word in ["average", "mean", "avg"]):
        numeric_cols = detect_numeric_columns(df_copy)
        if len(numeric_cols) >= 2:
            df_copy["average_value"] = df_copy[numeric_cols].mean(axis=1)
    
    # Calculate revenue (quantity * price patterns)
    revenue_match = re.search(r"revenue|total.*value|amount", prompt.lower())
    if revenue_match:
        # Look for quantity and price columns
        qty_cols = [col for col in df_copy.columns if any(word in col.lower() for word in ['qty', 'quantity', 'count', 'units'])]
        price_cols = [col for col in df_copy.columns if any(word in col.lower() for word in ['price', 'cost', 'rate', 'amount'])]
        
        if qty_cols and price_cols:
            qty_col = qty_cols[0]
            price_col = price_cols[0]
            if pd.api.types.is_numeric_dtype(df_copy[qty_col]) and pd.api.types.is_numeric_dtype(df_copy[price_col]):
                df_copy["revenue"] = df_copy[qty_col] * df_copy[price_col]
    
    # Specific calculation patterns (calculate new = col1 op col2)
    calc_match = re.search(r"calculate (\w+)\s*=\s*(\w+)\s*([-+*/])\s*(\w+)", prompt.lower())
    if calc_match:
        new_col, col1, op, col2 = calc_match.groups()
        if col1 in df_copy.columns and col2 in df_copy.columns:
            try:
                if op == "+": df_copy[new_col] = df_copy[col1] + df_copy[col2]
                elif op == "-": df_copy[new_col] = df_copy[col1] - df_copy[col2]
                elif op == "*": df_copy[new_col] = df_copy[col1] * df_copy[col2]
                elif op == "/": df_copy[new_col] = df_copy[col1] / df_copy[col2].replace(0, np.nan)
            except Exception:
                pass
    
    return df_copy

def apply_data_cleaning(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply data cleaning operations"""
    df_copy = df.copy()
    
    # Clean string columns
    if any(word in prompt.lower() for word in ["clean", "standardize", "format", "proper case"]):
        string_cols = detect_string_columns(df_copy)
        for col in string_cols:
            if df_copy[col].dtype == 'object':
                # Remove extra spaces and capitalize properly
                df_copy[col] = df_copy[col].astype(str).str.strip()
                df_copy[col] = df_copy[col].str.title()  # Proper case
    
    # Remove null values
    if any(phrase in prompt.lower() for phrase in ["remove null", "drop null", "exclude null", "filter null"]):
        initial_rows = len(df_copy)
        df_copy = df_copy.dropna()
        print(f"Removed {initial_rows - len(df_copy)} rows with null values")
    
    # Fill null values
    if any(phrase in prompt.lower() for phrase in ["fill null", "replace null", "handle null"]):
        numeric_cols = detect_numeric_columns(df_copy)
        string_cols = detect_string_columns(df_copy)
        
        # Fill numeric nulls with 0 or mean
        for col in numeric_cols:
            if "mean" in prompt.lower():
                df_copy[col].fillna(df_copy[col].mean(), inplace=True)
            else:
                df_copy[col].fillna(0, inplace=True)
        
        # Fill string nulls with "Unknown"
        for col in string_cols:
            df_copy[col].fillna("Unknown", inplace=True)
    
    # Text case transformations
    case_match = re.search(r"(uppercase|lowercase|upper case|lower case) (\w+)", prompt.lower())
    if case_match:
        case_type, col_name = case_match.groups()
        matching_cols = [col for col in df_copy.columns if col_name.lower() in col.lower()]
        
        for col in matching_cols:
            if df_copy[col].dtype == 'object':
                if "upper" in case_type:
                    df_copy[col] = df_copy[col].astype(str).str.upper()
                else:
                    df_copy[col] = df_copy[col].astype(str).str.lower()
    
    return df_copy

def apply_filtering(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply filtering operations"""
    df_copy = df.copy()
    
    # Filter by value patterns (filter column operator value)
    filter_match = re.search(r"filter (\w+)\s*([<>=!]+)\s*([\w\d.]+)", prompt.lower())
    if filter_match:
        col_name, operator, value = filter_match.groups()
        matching_cols = [col for col in df_copy.columns if col_name.lower() in col.lower()]
        
        if matching_cols:
            col = matching_cols[0]
            try:
                # Try to convert value to appropriate type
                if df_copy[col].dtype in ['int64', 'float64']:
                    value = float(value)
                
                if operator in [">", "gt"]:
                    df_copy = df_copy[df_copy[col] > value]
                elif operator in ["<", "lt"]:
                    df_copy = df_copy[df_copy[col] < value]
                elif operator in ["=", "==", "eq"]:
                    df_copy = df_copy[df_copy[col] == value]
                elif operator in ["!=", "<>", "ne"]:
                    df_copy = df_copy[df_copy[col] != value]
                elif operator in [">=", "gte"]:
                    df_copy = df_copy[df_copy[col] >= value]
                elif operator in ["<=", "lte"]:
                    df_copy = df_copy[df_copy[col] <= value]
            except Exception as e:
                print(f"Error filtering {col}: {e}")
    
    # Filter by status/category
    if any(phrase in prompt.lower() for phrase in ["active only", "filter active", "active customers", "active records"]):
        status_cols = [col for col in df_copy.columns if 'status' in col.lower()]
        if status_cols:
            col = status_cols[0]
            df_copy = df_copy[df_copy[col].str.lower().str.contains('active', na=False)]
    
    return df_copy

def apply_grouping_aggregation(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply grouping and aggregation operations"""
    df_copy = df.copy()
    
    # Group by patterns (group by column and aggregate column)
    group_match = re.search(r"group by (\w+)(?:\s+and\s+)?(?:(sum|mean|count|max|min|average)\s+(\w+))?", prompt.lower())
    if group_match:
        group_col_name, agg_func, agg_col_name = group_match.groups()
        
        # Find matching columns
        group_cols = [col for col in df_copy.columns if group_col_name.lower() in col.lower()]
        
        if group_cols:
            group_col = group_cols[0]
            
            if agg_func and agg_col_name:
                agg_cols = [col for col in df_copy.columns if agg_col_name.lower() in col.lower()]
                if agg_cols:
                    agg_col = agg_cols[0]
                    
                    # Map aggregation functions
                    agg_mapping = {
                        'sum': 'sum',
                        'mean': 'mean', 
                        'average': 'mean',
                        'count': 'count',
                        'max': 'max',
                        'min': 'min'
                    }
                    
                    agg_function = agg_mapping.get(agg_func, 'sum')
                    df_copy = df_copy.groupby(group_col)[agg_col].agg(agg_function).reset_index()
                    df_copy.columns = [group_col, f"{agg_function}_{agg_col}"]
            else:
                # Simple groupby count
                df_copy = df_copy.groupby(group_col).size().reset_index(name='count')
    
    return df_copy

def apply_sorting(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply sorting operations"""
    df_copy = df.copy()
    
    # Sort by column patterns
    sort_match = re.search(r"sort by (\w+)(?:\s+(asc|desc|ascending|descending))?", prompt.lower())
    if sort_match:
        col_name, order = sort_match.groups()
        matching_cols = [col for col in df_copy.columns if col_name.lower() in col.lower()]
        
        if matching_cols:
            col = matching_cols[0]
            ascending = True if not order or order in ['asc', 'ascending'] else False
            df_copy = df_copy.sort_values(by=col, ascending=ascending)
    
    return df_copy

def apply_column_operations(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply column-related operations"""
    df_copy = df.copy()
    
    # Rename columns
    rename_match = re.search(r"rename (\w+) to (\w+)", prompt.lower())
    if rename_match:
        old_name, new_name = rename_match.groups()
        matching_cols = [col for col in df_copy.columns if old_name.lower() in col.lower()]
        if matching_cols:
            old_col = matching_cols[0]
            df_copy = df_copy.rename(columns={old_col: new_name})
    
    # Add full name column
    if any(phrase in prompt.lower() for phrase in ["full name", "combine names", "full_name"]):
        first_name_cols = [col for col in df_copy.columns if any(word in col.lower() for word in ['first', 'fname'])]
        last_name_cols = [col for col in df_copy.columns if any(word in col.lower() for word in ['last', 'lname', 'surname'])]
        
        if first_name_cols and last_name_cols:
            first_col = first_name_cols[0]
            last_col = last_name_cols[0]
            df_copy['full_name'] = df_copy[first_col].astype(str) + ' ' + df_copy[last_col].astype(str)
    
    # Add category/classification columns
    if any(word in prompt.lower() for word in ["category", "classify", "group into"]):
        # Add performance categories
        if "performance" in prompt.lower():
            perf_cols = [col for col in df_copy.columns if any(word in col.lower() for word in ['score', 'rating', 'performance'])]
            if perf_cols:
                col = perf_cols[0]
                if pd.api.types.is_numeric_dtype(df_copy[col]):
                    df_copy['performance_category'] = pd.cut(
                        df_copy[col], 
                        bins=[0, 3.5, 4.0, 5.0], 
                        labels=['Needs Improvement', 'Good', 'Excellent'],
                        include_lowest=True
                    )
        
        # Add inventory status
        if any(word in prompt.lower() for word in ["inventory", "stock", "low stock"]):
            stock_cols = [col for col in df_copy.columns if any(word in col.lower() for word in ['stock', 'quantity', 'inventory'])]
            if stock_cols:
                col = stock_cols[0]
                if pd.api.types.is_numeric_dtype(df_copy[col]):
                    df_copy['stock_status'] = df_copy[col].apply(
                        lambda x: 'Low Stock' if x < 100 else 'Normal Stock'
                    )
    
    return df_copy

def apply_date_operations(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """Apply date-related operations"""
    df_copy = df.copy()
    
    # Convert to datetime
    date_match = re.search(r"convert (\w+) to datetime", prompt.lower())
    if date_match:
        col_name = date_match.group(1)
        matching_cols = [col for col in df_copy.columns if col_name.lower() in col.lower()]
        if matching_cols:
            col = matching_cols[0]
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
    
    # Add timestamp
    if any(phrase in prompt.lower() for phrase in ["add timestamp", "current time", "processing time"]):
        df_copy['processed_at'] = datetime.now()
    
    return df_copy

def transform_data(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    """
    Main transformation function that applies various transformations based on the prompt
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    try:
        # Start with a copy of the original dataframe
        df_transformed = df.copy()
        
        # Clean column names first
        df_transformed = clean_column_names(df_transformed)
        
        # Apply transformations in logical order
        df_transformed = apply_mathematical_operations(df_transformed, prompt)
        df_transformed = apply_data_cleaning(df_transformed, prompt)
        df_transformed = apply_filtering(df_transformed, prompt)
        df_transformed = apply_column_operations(df_transformed, prompt)
        df_transformed = apply_date_operations(df_transformed, prompt)
        df_transformed = apply_grouping_aggregation(df_transformed, prompt)
        df_transformed = apply_sorting(df_transformed, prompt)
        
        # Reset index to ensure clean output
        df_transformed = df_transformed.reset_index(drop=True)
        
        return df_transformed
    
    except Exception as e:
        print(f"Error in data transformation: {e}")
        return df  # Return original data if transformation fails