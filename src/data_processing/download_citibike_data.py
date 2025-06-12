#!/usr/bin/env python3
"""
Citi Bike Data Downloader - Using only built-in Python libraries
Works with your existing project structure
"""

import urllib.request
import urllib.error
import zipfile
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
import argparse

class CitiBikeDownloader:
    def __init__(self, project_root=None):
        """Initialize with project structure"""
        if project_root is None:
            project_root = Path.cwd()
        
        self.project_root = Path(project_root)
        self.raw_data_dir = self.project_root / "data" / "raw"
        self.external_data_dir = self.project_root / "data" / "external"
        
        # Create directories
        for dir_path in [self.raw_data_dir, self.external_data_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def download_file(self, year_month):
        """Download Citi Bike data for specific year-month"""
        filename = f"{year_month}-citibike-tripdata.csv.zip"
        url = f"https://s3.amazonaws.com/tripdata/{filename}"
        filepath = self.raw_data_dir / filename
        
        print(f"Downloading {filename}...")
        
        class ProgressHook:
            def __init__(self):
                self.last_percent = 0
            
            def __call__(self, block_num, block_size, total_size):
                if total_size > 0:
                    downloaded = block_num * block_size
                    percent = min(100, (downloaded / total_size) * 100)
                    if percent - self.last_percent >= 5:  # Update every 5%
                        print(f"\rProgress: {percent:.1f}%", end="", flush=True)
                        self.last_percent = percent
        
        try:
            urllib.request.urlretrieve(url, filepath, reporthook=ProgressHook())
            file_size = filepath.stat().st_size
            print(f"\n✓ Downloaded {filename} ({file_size:,} bytes)")
            return filepath
            
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"\n✗ File not found - {filename} may not exist for this month")
            else:
                print(f"\n✗ HTTP Error {e.code}: {e.reason}")
            return None
        except urllib.error.URLError as e:
            print(f"\n✗ URL Error: {e.reason}")
            return None
        except Exception as e:
            print(f"\n✗ Failed to download {filename}: {e}")
            return None
    
    def extract_zip(self, zip_path):
        """Extract zip file"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get the CSV filename from the zip
                csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
                if csv_files:
                    csv_filename = csv_files[0]
                    zip_ref.extract(csv_filename, self.raw_data_dir)
                    csv_path = self.raw_data_dir / csv_filename
                    print(f"✓ Extracted {csv_filename}")
                    return csv_path
                else:
                    print("✗ No CSV file found in zip")
                    return None
        except Exception as e:
            print(f"✗ Failed to extract {zip_path}: {e}")
            return None
    
    def get_recent_months(self, num_months=3):
        """Get recent month strings in YYYYMM format"""
        months = []
        current_date = datetime.now()
        
        for i in range(num_months):
            # Calculate date for i months ago
            year = current_date.year
            month = current_date.month - i
            
            # Handle year rollover
            while month <= 0:
                month += 12
                year -= 1
            
            months.append(f"{year:04d}{month:02d}")
        
        return sorted(months)  # Chronological order
    
    def create_summary(self, downloaded_files):
        """Create a summary of downloaded data"""
        summary = {
            "download_timestamp": datetime.now().isoformat(),
            "files": []
        }
        
        for file_path in downloaded_files:
            if file_path and file_path.exists():
                file_size = file_path.stat().st_size
                
                # If it's a CSV, try to get basic info
                if file_path.suffix == '.csv':
                    try:
                        # Count lines quickly
                        with open(file_path, 'r') as f:
                            line_count = sum(1 for line in f) - 1  # Subtract header
                        
                        # Get header
                        with open(file_path, 'r') as f:
                            header = f.readline().strip().split(',')
                    except:
                        line_count = "unknown"
                        header = []
                else:
                    line_count = "N/A (zip file)"
                    header = []
                
                summary["files"].append({
                    "filename": file_path.name,
                    "size_mb": round(file_size / (1024 * 1024), 2),
                    "rows": line_count,
                    "columns": header[:5] if header else []  # First 5 columns only
                })
        
        # Save summary
        summary_path = self.external_data_dir / "download_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n📋 Summary saved to {summary_path}")
        return summary
    
    def download_recent_data(self, num_months=3, extract=True):
        """Download recent months of data"""
        months = self.get_recent_months(num_months)
        print(f"📅 Downloading {num_months} months: {', '.join(months)}")
        
        downloaded_files = []
        extracted_files = []
        
        for month in months:
            print(f"\n--- Processing {month} ---")
            
            # Download
            zip_path = self.download_file(month)
            if zip_path:
                downloaded_files.append(zip_path)
                
                # Extract if requested
                if extract:
                    csv_path = self.extract_zip(zip_path)
                    if csv_path:
                        extracted_files.append(csv_path)
        
        return downloaded_files, extracted_files

def main():
    parser = argparse.ArgumentParser(description='Download Citi Bike trip data')
    parser.add_argument('--month', type=str, help='Specific month (YYYYMM format)')
    parser.add_argument('--recent', type=int, default=3, help='Number of recent months')
    parser.add_argument('--no-extract', action='store_true', help='Keep zip files only')
    parser.add_argument('--summary', action='store_true', help='Create data summary')
    
    args = parser.parse_args()
    
    downloader = CitiBikeDownloader()
    
    if args.month:
        # Download specific month
        print(f"📥 Downloading data for {args.month}")
        zip_path = downloader.download_file(args.month)
        files = [zip_path] if zip_path else []
        
        if zip_path and not args.no_extract:
            csv_path = downloader.extract_zip(zip_path)
            if csv_path:
                files.append(csv_path)
    else:
        # Download recent months
        downloaded_files, extracted_files = downloader.download_recent_data(
            args.recent, 
            extract=not args.no_extract
        )
        files = downloaded_files + extracted_files
    
    # Create summary if requested
    if args.summary and files:
        downloader.create_summary(files)
    
    print(f"\n🎉 Download complete!")
    print(f"📁 Data location: {downloader.raw_data_dir}")
    print(f"💡 Next: Start analyzing with your src/ modules!")

if __name__ == "__main__":
    main()