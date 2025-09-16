"""
This code converts our parquet files to csv files. The function is used later in data_combinator.ipynb.

"""



import pandas as pd
from pathlib import Path

def convert_parquet_to_csv(dataset_folder='datasets'):
    """
    Convert all Parquet files to CSV files in the same directory structure.
    
    Args:
        dataset_folder (str): Path to the main datasets folder
    """
    base_path = Path(dataset_folder)
    
    # Iterate through all subdirectories in the datasets folder
    for subdir in base_path.iterdir():
        if subdir.is_dir() and subdir.name.startswith('m'):
            # Find all parquet files in the subdirectory
            parquet_files = list(subdir.glob('*.parquet'))
            if parquet_files:
                try:
                    # Read parquet file
                    df = pd.read_parquet(parquet_files[0])
                    # Create CSV filename
                    csv_file = subdir / f"{subdir.name}.csv"
                    # Save as CSV
                    df.to_csv(csv_file, index=False)
                    print(f"Successfully converted {subdir.name} to CSV")
                except Exception as e:
                    print(f"Error converting {subdir.name}: {str(e)}")

if __name__ == "__main__":
    convert_parquet_to_csv() 