import pandas as pd
from pathlib import Path
from ....base import Source
import os
import re

class SlaughterPremiums(Source):
    """Danish Slaughter Premiums Data parser"""

    # Using column names from the Excel file
    EXPECTED_COLUMNS = [
        'PROD_AAR',
        'CapNumber',
        'CVR',
        'Reklamebeskyttelse',
        'Navn',
        'ORDNING',
        'AntalDyr',
        'Beløb_DKK_x0020__x0028_SUMDKK_x0029_',
        'PAYMENT_DATE'
    ]

    # Rename columns to match other sources and make them more readable
    COLUMN_RENAMING = {
        'PROD_AAR': 'production_year',
        'CapNumber': 'cap_number',
        'CVR': 'cvr_number',
        'Reklamebeskyttelse': 'marketing_protection',
        'Navn': 'name',
        'ORDNING': 'scheme',
        'AntalDyr': 'number_of_animals',
        'Beløb_DKK_x0020__x0028_SUMDKK_x0029_': 'amount_dkk',
        'PAYMENT_DATE': 'payment_date'
    }

    def _extract_year_range(self, filename: str) -> tuple[str, str]:
        """Extract year range from the filename"""
        # Assuming the filename contains years in the format 'SLP_YYYY_YYYY'
        match = re.search(r'SLP_(\d{4})_(\d{4})', filename)
        if match:
            return match.group(1), match.group(2)
        else:
            raise ValueError(f"Year range not found in filename: {filename}")

    def _read_excel_files(self) -> list[pd.DataFrame]:
        """Read all Excel files in the current directory"""
        current_dir = Path(__file__).parent

        dfs = []
        for file in current_dir.glob('*.xlsx'):
            try:
                df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
                df['source_file'] = file.name
                start_year, end_year = self._extract_year_range(file.name)
                df['year_range'] = f"{start_year}-{end_year}"
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")

        if not dfs:
            raise FileNotFoundError("No Excel files found in directory")

        return dfs

    def _clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        # Ensure all expected columns exist
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Clean up CVR and CapNumber values (remove _x00 prefix)
        for col in ['CVR', 'CapNumber']:
            df[col] = df[col].str.replace(r'_x00\d+', '', regex=True)

        # Convert amount to numeric, removing any currency formatting
        df['Beløb_DKK_x0020__x0028_SUMDKK_x0029_'] = pd.to_numeric(
            df['Beløb_DKK_x0020__x0028_SUMDKK_x0029_'],
            errors='coerce'
        )

        # Convert dates to datetime
        df['PAYMENT_DATE'] = pd.to_datetime(df['PAYMENT_DATE'])

        # Rename columns
        df = df.rename(columns=self.COLUMN_RENAMING)

        return df

    async def fetch(self) -> pd.DataFrame:
        # Get list of DataFrames from Excel files
        dfs = self._read_excel_files()

        # Merge all DataFrames into one
        df = pd.concat(dfs, ignore_index=True)

        # Clean and standardize data
        df = self._clean_and_standardize(df)
        df = df.fillna('')  # Replace NaN with empty string

        # Optional: Print debug info in test environment
        if 'test_parser' in os.environ.get('ENVIRONMENT', ''):
            print(f"\nFound {len(df)} slaughter premium entries")
            print("\nColumns:", df.columns.tolist())
            print("\nFirst few entries:")
            print(df.head())

        return df

    async def sync(self):
        """Sync slaughter premiums data"""
        df = await self.fetch()

        # Write to temporary local parquet file
        temp_file = "/tmp/slaughter_premiums_current.parquet"
        df.to_parquet(temp_file)

        # Upload to storage
        blob = self.bucket.blob(f'raw/slaughter_premiums/current.parquet')
        blob.upload_from_filename(temp_file)

        # Cleanup
        os.remove(temp_file)

        return len(df)
