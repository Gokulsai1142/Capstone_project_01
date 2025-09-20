import pandas as pd
import re

def transform_data(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    
    df_copy = df.copy()
    prompt_lower = prompt.lower()

    # --- SUM numeric columns ---
    if "total" in prompt_lower:
        numeric_cols = df_copy.select_dtypes(include="number").columns
        if len(numeric_cols) > 0:
            df_copy["Total"] = df_copy[numeric_cols].sum(axis=1)

    # --- Clean string columns ---
    if "clean" in prompt_lower:
        for col in df_copy.select_dtypes(include="object").columns:
            df_copy[col] = df_copy[col].astype(str).str.strip().str.capitalize()

    # --- Remove nulls ---
    if "remove null" in prompt_lower or "drop null" in prompt_lower:
        df_copy = df_copy.dropna()

    # --- Filter rows (pattern: filter column > value) ---
    match = re.search(r"filter (\w+)\s*([<>=!]+)\s*([\w\d]+)", prompt_lower)
    if match:
        col, op, val = match.groups()
        if col in df_copy.columns:
            try:
                val = float(val) if val.isdigit() else val
                if op == ">": df_copy = df_copy[df_copy[col] > val]
                elif op == "<": df_copy = df_copy[df_copy[col] < val]
                elif op in ["=", "=="]: df_copy = df_copy[df_copy[col] == val]
                elif op in ["!=", "<>"]: df_copy = df_copy[df_copy[col] != val]
            except Exception:
                pass

    # --- Rename columns (pattern: rename old to new) ---
    match = re.search(r"rename (\w+) to (\w+)", prompt_lower)
    if match:
        old, new = match.groups()
        if old in df_copy.columns:
            df_copy = df_copy.rename(columns={old: new})

    # --- Group & Aggregate (pattern: group by col and sum col) ---
    match = re.search(r"group by (\w+) and (sum|mean|count|max|min) (\w+)", prompt_lower)
    if match:
        group_col, agg_func, agg_col = match.groups()
        if group_col in df_copy.columns and agg_col in df_copy.columns:
            df_copy = df_copy.groupby(group_col)[agg_col].agg(agg_func).reset_index()

    # --- Convert date column ---
    match = re.search(r"convert (\w+) to datetime", prompt_lower)
    if match:
        col = match.group(1)
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")

    # --- Sorting (pattern: sort by col asc/desc) ---
    match = re.search(r"sort by (\w+)(?: (asc|desc))?", prompt_lower)
    if match:
        col, order = match.groups()
        if col in df_copy.columns:
            df_copy = df_copy.sort_values(by=col, ascending=(order != "desc"))

    # --- Text casing ---
    match = re.search(r"(uppercase|lowercase) (\w+)", prompt_lower)
    if match:
        case, col = match.groups()
        if col in df_copy.columns:
            if case == "uppercase":
                df_copy[col] = df_copy[col].astype(str).str.upper()
            else:
                df_copy[col] = df_copy[col].astype(str).str.lower()

    # --- Column calculation (pattern: calculate new = col1 - col2) ---
    match = re.search(r"calculate (\w+)\s*=\s*(\w+)\s*([-+*/])\s*(\w+)", prompt_lower)
    if match:
        new_col, col1, op, col2 = match.groups()
        if col1 in df_copy.columns and col2 in df_copy.columns:
            try:
                if op == "+": df_copy[new_col] = df_copy[col1] + df_copy[col2]
                elif op == "-": df_copy[new_col] = df_copy[col1] - df_copy[col2]
                elif op == "*": df_copy[new_col] = df_copy[col1] * df_copy[col2]
                elif op == "/": df_copy[new_col] = df_copy[col1] / df_copy[col2]
            except Exception:
                pass

    return df_copy
