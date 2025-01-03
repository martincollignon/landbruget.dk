import asyncio
import os
from pathlib import Path
import sys
import logging
from typing import Optional
import signal
from dotenv import load_dotenv
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.chr_data import CHRDataParser
from src.sources.parsers.chr_species import CHRSpeciesParser
from src.config import SOURCES

shutdown = asyncio.Event()

def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}. Starting graceful shutdown...")
    shutdown.set()

def handle_signal(signum, frame):
    """Handle interrupt signals with detailed logging."""
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received signal {signum} ({signal_name})")
    logger.warning("Stack trace at point of interrupt:")
    for line in traceback.format_stack():
        logger.warning(line.strip())
    logger.info("Starting graceful shutdown...")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

async def main() -> Optional[int]:
    """Sync CHR data to Cloud Storage"""
    load_dotenv()
    try:
        logger.info("Starting CHR data sync process...")
        
        # Register signal handlers with detailed logging
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, handle_signal)
            logger.info(f"Registered handler for signal {sig} ({signal.Signals(sig).name})")
        
        logger.info("Fetching species data...")
        species_parser = CHRSpeciesParser(SOURCES["chr_data"])
        species_data = await species_parser.fetch()
        
        if species_data.empty:
            logger.error("Failed to fetch species data. Exiting.")
            return
            
        logger.info(f"Successfully fetched species data with {len(species_data)} records")
        
        logger.info("Initializing CHR data parser...")
        chr_data = CHRDataParser(SOURCES["chr_data"])
        
        # Process each species code
        unique_species = species_data['species_code'].unique()
        total_species = len(unique_species)
        logger.info(f"Found {total_species} unique species codes to process")
        
        # Track statistics
        processed_species = 0
        successful_species = 0
        failed_species = 0
        total_records = 0
        
        for i, species_code in enumerate(unique_species, 1):
            try:
                logger.info(f"Processing species code: {species_code} ({i}/{total_species})")
                result = await chr_data.process_species(species_code)
                if result is not None:
                    successful_species += 1
                    records = len(result)
                    total_records += records
                    logger.info(f"Successfully processed species code {species_code} ({records} records)")
                    logger.info(f"Progress: {i}/{total_species} species processed ({(i/total_species)*100:.1f}%)")
                else:
                    failed_species += 1
                    logger.warning(f"No results for species code {species_code}")
            except Exception as e:
                failed_species += 1
                logger.error(f"Error processing species code {species_code}: {str(e)}", exc_info=True)
                continue
            processed_species += 1
        
        # Log final statistics
        logger.info("\nSync process completed. Final statistics:")
        logger.info(f"Total species processed: {processed_species}/{total_species}")
        logger.info(f"Successful species: {successful_species}")
        logger.info(f"Failed species: {failed_species}")
        logger.info(f"Total records processed: {total_records:,}")
        
    except Exception as e:
        logger.error(f"Error during sync process: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1) 