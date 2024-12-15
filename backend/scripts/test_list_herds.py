import sys
import os
import logging
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parents[2])
sys.path.insert(0, project_root)

from backend.src.sources.parsers.herd_data import HerdDataParser

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Test configuration with direct secrets
    config = {
        'enabled': True,
        'type': 'herd_data',
        'project_id': 'landbrugsdata-1',  # Updated to correct project ID
        'use_google_secrets': True,
        'secrets': {
            'fvm_username': '42731978',
            'fvm_password': 'GiXOEqqWjzWa3BXWCPGT1SINA'
        }
    }

    parser = HerdDataParser(config)
    herds = parser.list_all_herds()
    logger.info(f"Found {len(herds)} herds")
    
    # Print first few herds as example
    for herd in herds[:5]:
        logger.info(f"Herd: {herd}")

if __name__ == "__main__":
    main() 