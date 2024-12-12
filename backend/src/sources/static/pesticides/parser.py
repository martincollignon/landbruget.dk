import pandas as pd
from pathlib import Path
from ....base import Source
import os

class Pesticides(Source):
    """Danish Pesticide Data parser"""

    # Using only English column names
    EXPECTED_COLUMNS = [
        'CompanyName',
        'CompanyRegistrationNumber',
        'StreetName',
        'StreetBuildingIdentifier',
        'FloorIdentifier',
        'PostCodeIdentifier',
        'City',
        'AcreageSize',
        'AcreageUnit',
        'Name',
        'Code',
        'PesticideName',
        'PesticideRegistrationNumber',
        'DosageQuantity',
        'DosageUnit',
        'NoPesticides'
    ]

    # Unit mappings based on README
    ACREAGE_UNITS = {
        1: 'm2',
        2: 'hektar'
    }

    DOSAGE_UNITS = {
        1: 'Gram',
        2: 'Kg',
        3: 'Mililiter',
        4: 'Liter',
        5: 'Tablets'
    }

    # Rename columns to match other sources
    COLUMN_RENAMING = {
        'CompanyRegistrationNumber': 'cvr_number',
        'Name': 'crop_type',
        'Code': 'crop_code'
    }

    SNAKE_NAMES = {
        'CompanyName': 'company_name',
        'StreetName': 'street_name',
        'StreetBuildingIdentifier': 'street_building_identifier',
        'FloorIdentifier': 'floor_identifier',
        'PostCodeIdentifier': 'post_code_identifier',
        'City': 'city',
        'AcreageSize': 'acreage_size',
        'AcreageUnit': 'acreage_unit',
        'PesticideName': 'pesticide_name',
        'PesticideRegistrationNumber': 'pesticide_registration_number',
        'DosageQuantity': 'dosage_quantity',
        'DosageUnit': 'dosage_unit',
        'NoPesticides': 'no_pesticides'
    }

    def _read_excel_files(self) -> list[pd.DataFrame]:
        """Read all Excel files in the current directory"""
        current_dir = Path(__file__).parent

        dfs = []
        for file in current_dir.glob('*.xlsx'):
            try:
                df = pd.read_excel(file, sheet_name=0, engine='openpyxl')
                df['source_file'] = file.name
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")

        if not dfs:
            raise FileNotFoundError("No Excel files found in directory")

        return dfs

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns based on COLUMN_RENAMING and SNAKE_NAMES"""
        combined_renaming = {**self.COLUMN_RENAMING, **self.SNAKE_NAMES}
        return df.rename(columns=combined_renaming)

    def _clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        # Ensure all expected columns exist
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Rename columns
        df = self._rename_columns(df)

        # Map unit codes to readable values
        df['AcreageUnit'] = df['AcreageUnit'].map(self.ACREAGE_UNITS)
        df['DosageUnit'] = df['DosageUnit'].map(self.DOSAGE_UNITS)
        df['NoPesticides'] = df['NoPesticides'].map({'SAND': True, 'FALSK': False})

        return df

    async def fetch(self) -> pd.DataFrame:
        # Get list of DataFrames from Excel files
        dfs = self._read_excel_files()

        # Merge all DataFrames into one
        df = pd.concat(dfs, ignore_index=True)

        # Clean data (map units, standardize values)
        df = self._clean_and_standardize(df)
        df = df.fillna('')  # Replace NaN with empty string

        # Optional: Print debug info in test environment
        if 'test_parser' in os.environ.get('ENVIRONMENT', ''):
            print(f"\nFound {len(df)} pesticide entries")
            print("\nColumns:", df.columns.tolist())
            print("\nFirst few entries:")
            print(df.head())

        return df

    async def sync(self):
        """Sync pesticide data"""
        df = await self.fetch()

        # Write to temporary local parquet file
        temp_file = "/tmp/pesticides_current.parquet"
        df.to_parquet(temp_file)

        # Upload to storage
        blob = self.bucket.blob(f'raw/pesticides/current.parquet')
        blob.upload_from_filename(temp_file)

        # Cleanup
        os.remove(temp_file)

        return len(df)