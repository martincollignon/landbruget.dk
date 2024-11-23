import asyncio
import os
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.cadastral import Cadastral
from src.config import SOURCES

async def main():
    cadastral = Cadastral(SOURCES["cadastral"])
    
    print("FETCHING SCHEMA DEFINITION")
    
    async with cadastral._get_session() as session:
        params = {
            'username': 'DATAFORDELER_USERNAME_PLACEHOLDER',
            'password': 'DATAFORDELER_PASSWORD_PLACEHOLDER',
            'SERVICE': 'WFS',
            'REQUEST': 'DescribeFeatureType',
            'VERSION': '1.1.0',
            'TYPENAME': 'mat:SamletFastEjendom_Gaeldende',
            'NAMESPACE': 'xmlns(mat=http://data.gov.dk/schemas/matrikel/1)'
        }
        
        async with session.get(cadastral.config['url'], params=params) as response:
            response.raise_for_status()
            schema = await response.text()
            
        # Parse schema
        root = ET.fromstring(schema)
        ns = {'xsd': 'http://www.w3.org/2001/XMLSchema'}
        
        # Write to file
        with open('cadastral_schema.txt', 'w') as f:
            f.write("Available fields in schema:\n")
            f.write("-" * 80 + "\n")
            
            for element in root.findall('.//xsd:element', ns):
                name = element.get('name')
                type = element.get('type')
                if name and type:
                    f.write(f"\nField: {name}\n")
                    f.write(f"Type: {type}\n")
                    if 'minOccurs' in element.attrib:
                        f.write(f"Min Occurrences: {element.get('minOccurs')}\n")
                    if 'maxOccurs' in element.attrib:
                        f.write(f"Max Occurrences: {element.get('maxOccurs')}\n")
                    if 'nillable' in element.attrib:
                        f.write(f"Nullable: {element.get('nillable')}\n")
        
        print(f"Schema information written to cadastral_schema.txt")

if __name__ == "__main__":
    asyncio.run(main())