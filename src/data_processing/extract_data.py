#!/usr/bin/env python3
"""
Extract CSV files from downloaded zip files
"""

import zipfile
from pathlib import Path
import argparse

def extract_zip_files(data_dir="data/raw"):
    """Extract all zip files in the data directory"""
    data_path = Path(data_dir)
    
    # Find all zip files
    zip_files = list(data_path.glob("*.zip"))
    
    if not zip_files:
        print("No zip files found in data/raw/")
        return
    
    print(f"Found {len(zip_files)} zip file(s) to extract:")
    
    for zip_file in zip_files:
        print(f"\nExtracting {zip_file.name}...")
        
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # List contents
                file_list = zip_ref.namelist()
                csv_files = [f for f in file_list if f.endswith('.csv')]
                
                if csv_files:
                    for csv_file in csv_files:
                        # Extract to the same directory
                        zip_ref.extract(csv_file, data_path)
                        extracted_path = data_path / csv_file
                        file_size = extracted_path.stat().st_size
                        print(f"✓ Extracted {csv_file} ({file_size:,} bytes)")
                else:
                    print(f"✗ No CSV files found in {zip_file.name}")
                    
        except Exception as e:
            print(f"✗ Failed to extract {zip_file.name}: {e}")

def extract_specific_file(zip_filename, data_dir="data/raw"):
    """Extract a specific zip file"""
    zip_path = Path(data_dir) / zip_filename
    
    if not zip_path.exists():
        print(f"File not found: {zip_path}")
        return
    
    print(f"Extracting {zip_filename}...")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(Path(data_dir))
        print("✓ Extraction complete!")
        
    except Exception as e:
        print(f"✗ Failed to extract: {e}")

def main():
    parser = argparse.ArgumentParser(description='Extract CSV files from zip archives')
    parser.add_argument('--file', type=str, help='Specific zip file to extract')
    parser.add_argument('--all', action='store_true', help='Extract all zip files in data/raw')
    
    args = parser.parse_args()
    
    if args.file:
        extract_specific_file(args.file)
    elif args.all:
        extract_zip_files()
    else:
        # Default: extract all
        extract_zip_files()

if __name__ == "__main__":
    main()