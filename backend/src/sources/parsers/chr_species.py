"""Parser for CHR/species code combinations."""
import logging
from typing import Dict, List, Any, Optional
from zeep import Client
from zeep.transports import Transport
from requests import Session
from zeep.wsse.username import UsernameToken
import certifi
import pandas as pd
from datetime import datetime
from google.cloud import secretmanager
from ..base import BaseSource
import time
import asyncio

logger = logging.getLogger(__name__)

class CHRSpeciesParser(BaseSource):
    """Parser for CHR/species code combinations."""
    
    WSDL_URLS = {
        'stamdata': 'https://ws.fvst.dk/service/CHR_stamdataWS?WSDL',
        'besaetning': 'https://ws.fvst.dk/service/CHR_besaetningWS?wsdl',
        'ejendom': 'https://ws.fvst.dk/service/CHR_ejendomWS?wsdl'
    }
    
    # Constants
    BATCH_SIZE = 1000  # Maximum batch size for the API
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Get credentials from Google Secret Manager
        client = secretmanager.SecretManagerServiceClient()
        project_id = "landbrugsdata-1"
        
        def get_secret(secret_id: str) -> str:
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        
        # Get credentials
        self.username = get_secret('fvm_username')
        self.password = get_secret('fvm_password')
        
        # Initialize session with SSL verification
        session = Session()
        session.verify = certifi.where()
        self.transport = Transport(session=session)
        
        # Initialize clients for different services
        self.clients = {}
        for service_name, url in self.WSDL_URLS.items():
            self.clients[service_name] = Client(
                url,
                transport=self.transport,
                wsse=UsernameToken(self.username, self.password)
            )
        
        logger.info("Initialized CHR species parser")

    @property
    def source_id(self) -> str:
        return "chr_species"

    def get_species_usage_combinations(self) -> pd.DataFrame:
        """Get all valid species and usage code combinations."""
        try:
            request = {
                'GLRCHRWSInfoInbound': {
                    'BrugerNavn': self.username,
                    'KlientId': 'LandbrugsData',
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': 'species_usage_combinations'
                },
                'Request': {}
            }
            
            result = self.clients['stamdata'].service.ListDyrearterMedBrugsarter(request)
            
            if not hasattr(result, 'Response') or not result.Response:
                logger.error("No response from ListDyrearterMedBrugsarter")
                return pd.DataFrame()
            
            combinations = []
            for combo in result.Response:
                combinations.append({
                    'species_code': self.safe_int(getattr(combo, 'DyreArtKode', None)),
                    'species_text': self.safe_str(getattr(combo, 'DyreArtTekst', None)),
                    'usage_code': self.safe_int(getattr(combo, 'BrugsArtKode', None)),
                    'usage_text': self.safe_str(getattr(combo, 'BrugsArtTekst', None))
                })
            
            df = pd.DataFrame(combinations)
            self._upload_to_storage(df, 'species_usage_combinations')
            return df
            
        except Exception as e:
            logger.error(f"Error getting species usage combinations: {str(e)}")
            return pd.DataFrame()

    def get_herd_numbers(self, species_code: int, usage_code: Optional[int] = None) -> pd.DataFrame:
        """Get all herd numbers for a specific species code and optional usage code."""
        try:
            all_herds = set()  # Use a set to detect duplicates
            page = 1
            max_retries = 3
            bes_nr_fra = 0
            
            while True:
                logger.info(f"Fetching page {page} for species {species_code}, usage {usage_code} "
                          f"(starting from herd {bes_nr_fra}, current count: {len(all_herds)})")
                
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        request = {
                            'GLRCHRWSInfoInbound': {
                                'BrugerNavn': self.username,
                                'KlientId': 'LandbrugsData',
                                'SessionId': '1',
                                'IPAdresse': '',
                                'TrackID': f'list_herds_{species_code}_{usage_code}_{bes_nr_fra}'
                            },
                            'Request': {
                                'DyreArtKode': str(species_code),
                                'BrugsArtKode': str(usage_code) if usage_code is not None else None,
                                'BesNrFra': str(bes_nr_fra) if bes_nr_fra > 0 else None
                            }
                        }
                        
                        result = self.clients['besaetning'].service.listBesaetningerMedBrugsart(request)
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            logger.error(f"Failed to fetch page {page} after {max_retries} retries: {str(e)}")
                            return pd.DataFrame(
                                [{'herd_number': h, 'species_code': species_code, 'usage_code': usage_code} for h in sorted(list(all_herds))]
                            )
                        logger.warning(f"Retry {retry_count}/{max_retries} for page {page}: {str(e)}")
                        time.sleep(1)
                
                if not hasattr(result, 'Response'):
                    logger.warning(f"No Response attribute in result for page {page}")
                    break
                
                # Get metadata
                fra_bes_nr = getattr(result.Response, 'FraBesNr', None)
                til_bes_nr = getattr(result.Response, 'TilBesNr', None)
                api_antal = getattr(result.Response, 'antal', None)
                has_more = getattr(result.Response, 'FlereBesaetninger', False)
                
                logger.info(f"Response metadata: FraBesNr={fra_bes_nr}, TilBesNr={til_bes_nr}, "
                          f"antal={api_antal}, FlereBesaetninger={has_more}")
                
                if not hasattr(result.Response, 'BesaetningsnummerListe'):
                    logger.warning(f"No BesaetningsnummerListe in response for page {page}")
                    break
                
                if not hasattr(result.Response.BesaetningsnummerListe, 'BesNrListe'):
                    logger.warning(f"No BesNrListe in BesaetningsnummerListe for page {page}")
                    break
                
                # Get the list of herd numbers
                herd_numbers = result.Response.BesaetningsnummerListe.BesNrListe
                if not herd_numbers:
                    logger.info(f"No herds found on page {page}")
                    break
                
                # Convert to list if needed
                if not isinstance(herd_numbers, list):
                    herd_numbers = [herd_numbers]
                
                # Process numbers
                new_herds = 0
                duplicates = 0
                page_min = float('inf')
                page_max = 0
                
                for herd in herd_numbers:
                    if herd is not None:
                        try:
                            herd_int = int(herd)
                            if herd_int > 0:
                                page_min = min(page_min, herd_int)
                                page_max = max(page_max, herd_int)
                                if herd_int not in all_herds:
                                    all_herds.add(herd_int)
                                    new_herds += 1
                                else:
                                    duplicates += 1
                        except (ValueError, TypeError):
                            continue
                
                logger.info(f"Page {page} stats: {new_herds} new herds, {duplicates} duplicates")
                if new_herds > 0:
                    logger.info(f"Page range: {page_min} - {page_max}")
                
                # Check if we should continue
                if new_herds == 0:
                    logger.info("No new herds found, stopping pagination")
                    break
                
                if has_more and til_bes_nr is not None:
                    try:
                        next_bes_nr = int(til_bes_nr) + 1
                        if next_bes_nr <= bes_nr_fra:
                            logger.warning(f"Next starting point ({next_bes_nr}) is not greater than current ({bes_nr_fra})")
                            break
                        bes_nr_fra = next_bes_nr
                        page += 1
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid TilBesNr value: {til_bes_nr}")
                        break
                else:
                    logger.info("No more pages indicated by API")
                    break
                
                # Add small delay to avoid overwhelming the service
                time.sleep(0.2)
            
            total_herds = len(all_herds)
            logger.info(f"Found {total_herds} total herds across {page} pages")
            if all_herds:
                logger.info(f"Overall range: {min(all_herds)} - {max(all_herds)}")
            
            # Convert to DataFrame and upload
            df = pd.DataFrame([{'herd_number': h, 'species_code': species_code, 'usage_code': usage_code} for h in sorted(list(all_herds))])
            if not df.empty:
                self._upload_to_storage(df, f'herd_numbers_{species_code}_{usage_code}')
            return df
            
        except Exception as e:
            logger.error(f"Error getting herd numbers for species code {species_code}: {str(e)}")
            return pd.DataFrame()

    async def get_chr_numbers_async(self, species_code: int, usage_code: Optional[int] = None) -> pd.DataFrame:
        """Get all CHR numbers for a specific species code and optional usage code."""
        try:
            # First get all herd numbers
            herds_df = self.get_herd_numbers(species_code, usage_code)
            if herds_df.empty:
                return pd.DataFrame()
            
            total_herds = len(herds_df)
            logger.info(f"Processing {total_herds} herds for species {species_code}, usage {usage_code}")
            
            # Process herds in parallel using asyncio
            properties = []
            semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
            
            async def process_herd(idx: int, herd_number: int):
                async with semaphore:
                    if idx % 100 == 0:
                        logger.info(f"Processing herd {idx}/{total_herds}")
                    
                    request = {
                        'GLRCHRWSInfoInbound': {
                            'BrugerNavn': self.username,
                            'KlientId': 'LandbrugsData',
                            'SessionId': '1',
                            'IPAdresse': '',
                            'TrackID': f'herd_details_{herd_number}'
                        },
                        'Request': {
                            'BesaetningsNummer': str(herd_number),
                            'DyreArtKode': str(species_code)
                        }
                    }
                    
                    try:
                        result = self.clients['besaetning'].service.hentStamoplysninger(request)
                        
                        if hasattr(result, 'Response') and result.Response:
                            response_data = result.Response[0].Besaetning
                            chr_number = self.safe_int(getattr(response_data, 'ChrNummer', None))
                            if chr_number:
                                return {
                                    'chr_number': chr_number,
                                    'species_code': species_code,
                                    'herd_number': herd_number
                                }
                    except Exception as e:
                        logger.error(f"Error processing herd {herd_number}: {str(e)}")
                    
                    await asyncio.sleep(0.05)  # Reduced delay
                    return None
            
            # Create tasks for all herds
            tasks = []
            for idx, herd_number in enumerate(herds_df['herd_number'].tolist(), 1):
                tasks.append(process_herd(idx, herd_number))
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks)
            properties = [r for r in results if r is not None]
            
            logger.info(f"Finished processing {total_herds} herds, found {len(properties)} CHR numbers")
            df = pd.DataFrame(properties)
            if not df.empty:
                self._upload_to_storage(df, f'chr_numbers_{species_code}')
            return df
            
        except Exception as e:
            logger.error(f"Error getting CHR numbers for species code {species_code}: {str(e)}")
            return pd.DataFrame()

    def get_chr_numbers(self, species_code: int, usage_code: Optional[int] = None) -> pd.DataFrame:
        """Synchronous wrapper for get_chr_numbers_async."""
        return asyncio.run(self.get_chr_numbers_async(species_code, usage_code))

    def safe_str(self, value: Any) -> Optional[str]:
        """Safely convert value to string, return None if empty."""
        if value is None:
            return None
        try:
            val_str = str(value).strip()
            return val_str if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None
            
    def safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int, return None if not possible."""
        if value is None:
            return None
        try:
            val_str = str(value).strip()
            return int(val_str) if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None

    async def get_species_usage_combinations_async(self) -> pd.DataFrame:
        """Async version of get_species_usage_combinations."""
        return self.get_species_usage_combinations()

    async def get_herd_numbers_async(self, species_code: int, usage_code: Optional[int] = None) -> pd.DataFrame:
        """Async version of get_herd_numbers."""
        return self.get_herd_numbers(species_code, usage_code)

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from the CHR web services."""
        try:
            # Get all species/usage combinations
            logger.info("Getting species/usage combinations")
            combinations_df = await self.get_species_usage_combinations_async()
            
            if combinations_df.empty:
                logger.error("Failed to get species/usage combinations")
                return pd.DataFrame()
            
            # Upload the combinations
            self._upload_to_storage(combinations_df, 'species_usage_combinations')
            
            return combinations_df
            
        except Exception as e:
            logger.error(f"Error during fetch: {str(e)}")
            return pd.DataFrame()

    def fetch_sync(self) -> pd.DataFrame:
        """Synchronous version of fetch."""
        import asyncio
        return asyncio.run(self.fetch()) 