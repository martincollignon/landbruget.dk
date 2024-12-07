import pandas as pd
import pdfplumber
from pathlib import Path
from ....base import Source
import os

class CropCodes(Source):
    """Danish Agricultural Crop Codes parser"""
    
    async def fetch(self) -> pd.DataFrame:
        data_path = Path(__file__).parent / 'bilag-1-afgroedekoder-2023.pdf'
        if not data_path.exists():
            raise FileNotFoundError(f"Crop codes data not found at {data_path}")
            
        # Initialize lists to store the data
        codes = []
        names = []
        categories = []
        
        current_code = None
        current_name = None
        current_category = None
        
        with pdfplumber.open(data_path) as pdf:
            # Skip first page header
            start_processing = False
            
            # Iterate through all pages
            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split('\n')
                
                for line in lines:
                    # Skip until we find the header
                    if not start_processing:
                        if "Afgrødekode Navn Engangskompensationskategori" in line:
                            start_processing = True
                        continue
                    
                    # Skip footer
                    if "Miljøstyrelsen" in line or "* Stjernemarkering" in line:
                        continue
                        
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Try to parse as new entry
                    parts = line.split(' ', 1)
                    if parts[0].isdigit():
                        # Process previous entry if exists
                        if current_code is not None and current_name is not None and current_category is not None:
                            codes.append(current_code)
                            names.append(current_name.strip())
                            categories.append(current_category.strip())
                        
                        # Start new entry
                        current_code = int(parts[0])
                        if len(parts) > 1:
                            rest = parts[1].strip()
                            # Look for category at the end
                            words = rest.split()
                            if words:
                                last_word = words[-1]
                                if any(cat in last_word for cat in ["Omdrift", "Natur", "græs"]):
                                    current_category = last_word
                                    current_name = ' '.join(words[:-1])
                                else:
                                    current_category = None
                                    current_name = rest
                    else:
                        # Continuation of previous entry
                        words = line.split()
                        if words:
                            last_word = words[-1]
                            if any(cat in last_word for cat in ["Omdrift", "Natur", "græs"]):
                                current_category = last_word
                                if current_name:
                                    current_name += " " + ' '.join(words[:-1])
                                else:
                                    current_name = ' '.join(words[:-1])
                            else:
                                if current_name:
                                    current_name += " " + line
                                else:
                                    current_name = line
            
            # Process last entry
            if current_code is not None and current_name is not None and current_category is not None:
                codes.append(current_code)
                names.append(current_name.strip())
                categories.append(current_category.strip())
        
        # Create DataFrame
        df = pd.DataFrame({
            'Afgrødekode': codes,
            'Navn': names,
            'Engangskompensationskategori': categories
        })
        
        # Sort by crop code
        df = df.sort_values('Afgrødekode').reset_index(drop=True)
        
        # Save with proper encoding and quoting
        if 'test_parser' in str(data_path):
            df.to_csv('crop_codes.csv', 
                     index=False, 
                     encoding='utf-8-sig',
                     quoting=1,  # Quote strings
                     quotechar='"',
                     sep=',')
            
            # Print some stats
            print(f"\nFound {len(df)} crop codes")
            print("\nFirst few entries:")
            print(df.head())
            print("\nLast few entries:")
            print(df.tail())
        
        return df

    async def sync(self):
        """Sync the crop codes data"""
        df = await self.fetch()
        
        # Write to temporary local parquet file
        temp_file = "/tmp/crop_codes_current.parquet"
        df.to_parquet(temp_file)
        
        # Upload to storage
        blob = self.bucket.blob(f'raw/crops/current.parquet')
        blob.upload_from_filename(temp_file)
        
        # Cleanup
        os.remove(temp_file)
        
        return len(df) 