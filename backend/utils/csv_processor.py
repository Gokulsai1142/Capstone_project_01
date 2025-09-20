import pandas as pd
from datetime import datetime

def add_timestamp_to_csv(input_csv_path: str, output_csv_path: str):
    """
    Reads a CSV file, adds a timestamp column, and saves the modified CSV.
    """
    try:
        df = pd.read_csv(input_csv_path)
        timestamp = datetime.now().isoformat()
        df['timestamp'] = timestamp
        df.to_csv(output_csv_path, index=False)
        print(f"Successfully added timestamp to {input_csv_path} and saved to {output_csv_path}")
    except Exception as e:
        print(f"Error processing CSV: {e}")
