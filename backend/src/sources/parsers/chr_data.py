"""Parser for CHR data using listOplysninger."""
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
from pyproj import Transformer
from ..base import BaseSource
from .chr_species import CHRSpeciesParser

logger = logging.getLogger(__name__)

class CHRDataParser(BaseSource):
    """Parser for CHR data using listOplysninger."""
    
    WSDL_URLS = {
        'stamdata': 'https://ws.fvst.dk/service/CHR_stamdataWS?WSDL',
        'besaetning': 'https://ws.fvst.dk/service/CHR_besaetningWS?wsdl',
        'ejendom': 'https://ws.fvst.dk/service/CHR_ejendomWS?wsdl'
    }
    
    # Constants
    BATCH_SIZE = 100  # Further reduced batch size for better stability
    UPLOAD_THRESHOLD = 500  # Reduced threshold for batch uploads
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        try:
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
                logger.info(f"Initializing client for {service_name} service...")
                self.clients[service_name] = Client(
                    url,
                    transport=self.transport,
                    wsse=UsernameToken(self.username, self.password)
                )
                logger.info(f"Successfully initialized {service_name} client")
            
            logger.info("Successfully initialized CHR data parser with all services")
            
        except Exception as e:
            logger.error(f"Error initializing CHR data parser: {str(e)}", exc_info=True)
            raise

    @property
    def source_id(self) -> str:
        return "chr_data"

    def process_chr_numbers(self, chr_numbers: List[int], species_code: int) -> pd.DataFrame:
        """Process a list of CHR numbers and return the data."""
        results = []
        total_chr_numbers = len(chr_numbers)
        logger.info(f"Starting to process {total_chr_numbers} CHR numbers for species {species_code}")
        
        try:
            # Track unique entities
            seen_properties = set()
            seen_owners = set()
            seen_users = set()
            seen_practices = set()
            
            # Data containers
            properties_data = []
            owners_data = []
            users_data = []
            practices_data = []
            herds_data = []
            
            # Process CHR numbers in smaller batches
            for i in range(0, total_chr_numbers, self.BATCH_SIZE):
                batch = chr_numbers[i:i + self.BATCH_SIZE]
                batch_num = i//self.BATCH_SIZE + 1
                total_batches = (total_chr_numbers + self.BATCH_SIZE - 1)//self.BATCH_SIZE
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} CHR numbers)")
                
                try:
                    # Fetch detailed data for all CHR numbers in this batch
                    logger.info(f"Fetching property details for batch {batch_num}...")
                    request = {
                        'GLRCHRWSInfoInbound': {
                            'BrugerNavn': self.username,
                            'KlientId': 'LandbrugsData',
                            'SessionId': '1',
                            'IPAdresse': '',
                            'TrackID': f'property_details_batch_{batch_num}'
                        },
                        'Request': {
                            'ChrNummer': batch,
                            'DyreArtKode': str(species_code)
                        }
                    }
                    
                    result = self.clients['ejendom'].service.listOplysninger(request)
                    logger.info(f"Received response for batch {batch_num}")
                    
                    if not hasattr(result, 'Response') or not result.Response or not hasattr(result.Response, 'EjendomsOplysningerListe'):
                        logger.warning(f"No property details found in response for batch {batch_num}")
                        continue
                    
                    properties = result.Response.EjendomsOplysningerListe.EjendomsOplysninger
                    if not isinstance(properties, list):
                        properties = [properties]
                    
                    logger.info(f"Processing {len(properties)} properties in batch {batch_num}")
                    
                    # Process each property and its related data
                    for prop_idx, prop in enumerate(properties, 1):
                        logger.info(f"Processing property {prop_idx}/{len(properties)} in batch {batch_num}")
                        chr_number = self.safe_int(getattr(prop, 'ChrNummer', None))
                        if not chr_number or chr_number in seen_properties:
                            continue
                        
                        # Process property details
                        property_data = self._process_property_data(prop, chr_number)
                        if property_data:
                            # Get additional veterinary events
                            additional_events = self._get_additional_veterinary_events(chr_number)
                            if additional_events:
                                if 'veterinary_events' not in property_data:
                                    property_data['veterinary_events'] = []
                                property_data['veterinary_events'].extend(additional_events)
                            
                            properties_data.append(property_data)
                            seen_properties.add(chr_number)
                        
                        # Process herds
                        if hasattr(prop, 'Besaetninger'):
                            herds = prop.Besaetninger.Besaetning
                            if not isinstance(herds, list):
                                herds = [herds]
                            
                            logger.info(f"Processing {len(herds)} herds for property {prop_idx}")
                            for herd_idx, herd in enumerate(herds, 1):
                                logger.info(f"Processing herd {herd_idx}/{len(herds)}")
                                # Get herd number once
                                herd_number = self.safe_int(getattr(herd, 'BesaetningsNummer', None))
                                
                                # Process herd data
                                herd_rows = self._process_herd_data(herd, chr_number)
                                herds_data.extend(herd_rows)
                                
                                # Process owner info
                                if hasattr(herd, 'Ejer'):
                                    owner_data = self._process_person_data(herd.Ejer, chr_number, 'owner', herd_number)
                                    if owner_data:
                                        owner_key = f"{owner_data['cvr_number'] or owner_data['cpr_number']}_{chr_number}_{herd_number}"
                                        if owner_key not in seen_owners:
                                            owners_data.append(owner_data)
                                            seen_owners.add(owner_key)
                                
                                # Process user info
                                if hasattr(herd, 'Bruger'):
                                    user_data = self._process_person_data(herd.Bruger, chr_number, 'user', herd_number)
                                    if user_data:
                                        user_key = f"{user_data['cvr_number'] or user_data['cpr_number']}_{chr_number}_{herd_number}"
                                        if user_key not in seen_users:
                                            users_data.append(user_data)
                                            seen_users.add(user_key)
                                
                                # Process veterinary practice info
                                if hasattr(herd, 'BesPraksis'):
                                    practice_data = self._process_practice_data(herd.BesPraksis, chr_number, herd_number)
                                    if practice_data:
                                        practice_key = f"{practice_data['practice_number']}_{chr_number}_{herd_number}"
                                        if practice_key not in seen_practices:
                                            practices_data.append(practice_data)
                                            seen_practices.add(practice_key)
                    
                    # Upload batch if we have enough data
                    if len(herds_data) >= self.UPLOAD_THRESHOLD or len(properties_data) >= self.UPLOAD_THRESHOLD:
                        logger.info(f"Upload threshold reached in batch {batch_num}, uploading data...")
                        self._upload_batch_data(herds_data, properties_data, owners_data, users_data, practices_data)
                        results.append(('batch', {
                            'herds': len(herds_data),
                            'properties': len(properties_data),
                            'owners': len(owners_data),
                            'users': len(users_data),
                            'practices': len(practices_data)
                        }))
                        # Clear data containers
                        herds_data = []
                        properties_data = []
                        owners_data = []
                        users_data = []
                        practices_data = []
                        logger.info(f"Batch {batch_num} data uploaded successfully")
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_num}: {str(e)}", exc_info=True)
                    continue
            
            # Upload any remaining data
            if any([herds_data, properties_data, owners_data, users_data, practices_data]):
                logger.info("Uploading remaining data...")
                self._upload_batch_data(herds_data, properties_data, owners_data, users_data, practices_data)
                results.append(('final_batch', {
                    'herds': len(herds_data),
                    'properties': len(properties_data),
                    'owners': len(owners_data),
                    'users': len(users_data),
                    'practices': len(practices_data)
                }))
                logger.info("Remaining data uploaded successfully")
            
            logger.info(f"Completed processing all {total_chr_numbers} CHR numbers")
            return pd.DataFrame(results, columns=['batch', 'counts'])
            
        except Exception as e:
            logger.error(f"Error processing CHR numbers: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _upload_batch_data(self, herds_data: list, properties_data: list, owners_data: list, users_data: list, practices_data: list) -> None:
        """Upload batches of data to storage."""
        try:
            # Convert lists to DataFrames and upload
            if herds_data:
                logger.info(f"Converting and uploading {len(herds_data)} herd records...")
                herds_df = pd.DataFrame(herds_data)
                self._upload_to_storage(herds_df, 'herds')
                logger.info(f"Uploaded {len(herds_data)} herd records")
            
            # Extract veterinary events before uploading properties
            veterinary_events = []
            if properties_data:
                logger.info("Processing veterinary events from properties...")
                for prop in properties_data:
                    if 'veterinary_events' in prop:
                        events = prop.pop('veterinary_events')
                        for event in events:
                            event['chr_number'] = prop['chr_number']
                        veterinary_events.extend(events)
            
            if veterinary_events:
                logger.info(f"Converting and uploading {len(veterinary_events)} veterinary event records...")
                events_df = pd.DataFrame(veterinary_events)
                self._upload_to_storage(events_df, 'veterinary_events')
                logger.info(f"Uploaded {len(veterinary_events)} veterinary event records")
            
            if properties_data:
                logger.info(f"Converting and uploading {len(properties_data)} property records...")
                properties_df = pd.DataFrame(properties_data)
                self._upload_to_storage(properties_df, 'properties')
                logger.info(f"Uploaded {len(properties_data)} property records")
            
            if owners_data:
                logger.info(f"Converting and uploading {len(owners_data)} owner records...")
                owners_df = pd.DataFrame(owners_data)
                self._upload_to_storage(owners_df, 'owners')
                logger.info(f"Uploaded {len(owners_data)} owner records")
            
            if users_data:
                logger.info(f"Converting and uploading {len(users_data)} user records...")
                users_df = pd.DataFrame(users_data)
                self._upload_to_storage(users_df, 'users')
                logger.info(f"Uploaded {len(users_data)} user records")
            
            if practices_data:
                logger.info(f"Converting and uploading {len(practices_data)} practice records...")
                practices_df = pd.DataFrame(practices_data)
                self._upload_to_storage(practices_df, 'practices')
                logger.info(f"Uploaded {len(practices_data)} practice records")
                
        except Exception as e:
            logger.error(f"Error uploading batch data: {str(e)}", exc_info=True)
            raise

    def _process_related_data(self, herd: Any, chr_number: int, owners_data: list, users_data: list, practices_data: list, 
                            seen_owners: set, seen_users: set, seen_practices: set) -> None:
        """Process owner, user, and practice data for a herd."""
        try:
            herd_number = self.safe_int(getattr(herd, 'BesaetningsNummer', None))
            
            # Process owner info
            if hasattr(herd, 'Ejer'):
                owner_data = self._process_person_data(herd.Ejer, chr_number, 'owner', herd_number)
                if owner_data:
                    owner_key = f"{owner_data['cvr_number'] or owner_data['cpr_number']}_{chr_number}_{herd_number}"
                    if owner_key not in seen_owners:
                        owners_data.append(owner_data)
                        seen_owners.add(owner_key)
            
            # Process user info
            if hasattr(herd, 'Bruger'):
                user_data = self._process_person_data(herd.Bruger, chr_number, 'user', herd_number)
                if user_data:
                    user_key = f"{user_data['cvr_number'] or user_data['cpr_number']}_{chr_number}_{herd_number}"
                    if user_key not in seen_users:
                        users_data.append(user_data)
                        seen_users.add(user_key)
            
            # Process veterinary practice info
            if hasattr(herd, 'BesPraksis'):
                practice_data = self._process_practice_data(herd.BesPraksis, chr_number, herd_number)
                if practice_data:
                    practice_key = f"{practice_data['practice_number']}_{chr_number}_{herd_number}"
                    if practice_key not in seen_practices:
                        practices_data.append(practice_data)
                        seen_practices.add(practice_key)
                        
        except Exception as e:
            logger.error(f"Error processing related data for CHR {chr_number}: {str(e)}", exc_info=True)

    def _get_additional_veterinary_events(self, chr_number: int) -> List[Dict[str, Any]]:
        """Get additional veterinary events from the ejendom endpoint."""
        try:
            request = {
                'GLRCHRWSInfoInbound': {
                    'BrugerNavn': self.username,
                    'KlientId': 'LandbrugsData',
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': f'additional_events_{chr_number}'
                },
                'Request': {
                    'ChrNummer': [chr_number]
                }
            }
            
            result = self.clients['ejendom'].service.listOplysninger(request)
            
            if not hasattr(result, 'Response') or not result.Response or not hasattr(result.Response, 'EjendomsOplysningerListe'):
                return []
            
            properties = result.Response.EjendomsOplysningerListe.EjendomsOplysninger
            if not isinstance(properties, list):
                properties = [properties]
            
            all_events = []
            for prop in properties:
                if hasattr(prop, 'VeterinaereHaendelser'):
                    vet = prop.VeterinaereHaendelser
                    if hasattr(vet, 'VeterinaerHaendelse'):
                        events = vet.VeterinaerHaendelse
                        if not isinstance(events, list):
                            events = [events]
                        
                        for event in events:
                            event_data = {
                                'disease_code': self.safe_str(getattr(event, 'SygdomsKode', None)),
                                'disease_text': self.safe_str(getattr(event, 'SygdomsTekst', None)),
                                'status_code': self.safe_str(getattr(event, 'VeterinaerStatusKode', None)),
                                'status_text': self.safe_str(getattr(event, 'VeterinaerStatusTekst', None)),
                                'level_code': self.safe_str(getattr(event, 'SygdomsNiveauKode', None)),
                                'level_text': self.safe_str(getattr(event, 'SygdomsNiveauTekst', None)),
                                'status_date': self._parse_date(getattr(event, 'DatoVeterinaerStatus', None)),
                                'remarks': self.safe_str(getattr(event, 'VeterinaerHaendelseBemaerkning', None))
                            }
                            all_events.append({k: v for k, v in event_data.items() if v is not None})
            
            return all_events
            
        except Exception as e:
            logger.error(f"Error getting additional veterinary events for CHR {chr_number}: {str(e)}")
            return []

    def _process_property_data(self, prop: Any, chr_number: int) -> Dict[str, Any]:
        """Process property data from listOplysninger response."""
        property_details = {
            'chr_number': chr_number,
            'food_region_number': None,
            'food_region_name': None,
            'veterinary_dept_name': None,
            'veterinary_section_name': None
        }
        
        # Basic property info
        if hasattr(prop, 'Ejendom'):
            ejendom = prop.Ejendom
            property_details.update({
                'property_address': self.safe_str(getattr(ejendom, 'Adresse', None)),
                'property_city': self.safe_str(getattr(ejendom, 'ByNavn', None)),
                'property_postal_code': self.safe_str(getattr(ejendom, 'PostNummer', None)),
                'property_postal_district': self.safe_str(getattr(ejendom, 'PostDistrikt', None)),
                'property_municipality_code': self.safe_int(getattr(ejendom, 'KommuneNummer', None)),
                'property_municipality_name': self.safe_str(getattr(ejendom, 'KommuneNavn', None)),
                'property_created_date': self._parse_date(getattr(ejendom, 'DatoOpret', None)),
                'property_updated_date': self._parse_date(getattr(ejendom, 'DatoOpdatering', None))
            })
        
        # Food authority information
        if hasattr(prop, 'FVST'):
            fvst = prop.FVST
            property_details.update({
                'food_region_number': self.safe_int(getattr(fvst, 'FoedevareRegionsNummer', None)),
                'food_region_name': self.safe_str(getattr(fvst, 'FoedevareRegionsNavn', None)),
                'veterinary_dept_name': self.safe_str(getattr(fvst, 'VeterinaerAfdelingsNavn', None)),
                'veterinary_section_name': self.safe_str(getattr(fvst, 'VeterinaerSektionsNavn', None))
            })
        
        # Coordinates
        if hasattr(prop, 'StaldKoordinater'):
            coords = prop.StaldKoordinater
            x = self.safe_str(getattr(coords, 'StaldKoordinatX', None))
            y = self.safe_str(getattr(coords, 'StaldKoordinatY', None))
            if x and y:
                try:
                    x_float = float(x)
                    y_float = float(y)
                    property_details['stable_coordinates_utm32'] = {'x': x_float, 'y': y_float}
                    
                    # Convert UTM32 coordinates to WGS84 (latitude/longitude)
                    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326")
                    lat, lon = transformer.transform(x_float, y_float)
                    property_details['latitude'] = lat
                    property_details['longitude'] = lon
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error converting coordinates for CHR {chr_number}: {str(e)}")
        
        # Veterinary events - combine from both sources
        events = []
        
        # Events from hentCHRStamoplysninger
        if hasattr(prop, 'VeterinaereHaendelser'):
            vet = prop.VeterinaereHaendelser
            property_details['veterinary_problems'] = self.safe_str(getattr(vet, 'VeterinaereProblemer', None))
            
            if hasattr(vet, 'VeterinaerHaendelse'):
                base_events = vet.VeterinaerHaendelse
                if not isinstance(base_events, list):
                    base_events = [base_events]
                
                for event in base_events:
                    event_data = {
                        'disease_code': self.safe_str(getattr(event, 'SygdomsKode', None)),
                        'disease_text': self.safe_str(getattr(event, 'SygdomsTekst', None)),
                        'status_code': self.safe_str(getattr(event, 'VeterinaerStatusKode', None)),
                        'status_text': self.safe_str(getattr(event, 'VeterinaerStatusTekst', None)),
                        'level_code': self.safe_str(getattr(event, 'SygdomsNiveauKode', None)),
                        'level_text': self.safe_str(getattr(event, 'SygdomsNiveauTekst', None)),
                        'status_date': self._parse_date(getattr(event, 'DatoVeterinaerStatus', None)),
                        'remarks': self.safe_str(getattr(event, 'VeterinaerHaendelseBemaerkning', None))
                    }
                    events.append({k: v for k, v in event_data.items() if v is not None})
        
        # Additional events from listOplysninger
        additional_events = self._get_additional_veterinary_events(chr_number)
        if additional_events:
            events.extend(additional_events)
        
        if events:
            property_details['veterinary_events'] = events
        
        # Cooperation agreements
        if hasattr(prop, 'SamdriftNaboaftaler'):
            if hasattr(prop.SamdriftNaboaftaler, 'Samdrift'):
                samdrift = prop.SamdriftNaboaftaler.Samdrift
                if not isinstance(samdrift, list):
                    samdrift = [samdrift]
                
                property_details['cooperation_agreements'] = []
                for agreement in samdrift:
                    agreement_data = {
                        'species_code': self.safe_int(getattr(agreement, 'DyreArtKode', None)),
                        'species_text': self.safe_str(getattr(agreement, 'DyreArtTekst', None))
                    }
                    
                    if hasattr(agreement, 'ChrNumre') and hasattr(agreement.ChrNumre, 'ChrNummer'):
                        chr_list = agreement.ChrNumre.ChrNummer
                        if not isinstance(chr_list, list):
                            chr_list = [chr_list]
                        agreement_data['chr_numbers'] = [self.safe_int(chr) for chr in chr_list if self.safe_int(chr)]
                    
                    property_details['cooperation_agreements'].append(agreement_data)
        
        return {k: v for k, v in property_details.items() if v is not None}

    def _process_herd_data(self, herd: Any, chr_number: int) -> List[Dict[str, Any]]:
        """Process herd data from listOplysninger response."""
        herd_rows = []
        
        # Process herd sizes
        if hasattr(herd, 'BesStr'):
            sizes = herd.BesStr
            if not isinstance(sizes, list):
                sizes = [sizes]
            
            for size in sizes:
                if not self.safe_str(getattr(size, 'BesaetningsStoerrelseTekst', '')).lower().startswith('i alt'):
                    herd_row = {
                        'herd_number': self.safe_int(getattr(herd, 'BesaetningsNummer', None)),
                        'chr_number': chr_number,
                        'species_code': self.safe_int(getattr(herd, 'DyreArtKode', None)),
                        'species_text': self.safe_str(getattr(herd, 'DyreArtTekst', None)),
                        'usage_code': self.safe_int(getattr(herd, 'BrugsArtKode', None)),
                        'usage_text': self.safe_str(getattr(herd, 'BrugsArtTekst', None)),
                        'business_type': self.safe_str(getattr(herd, 'VirksomhedsArtTekst', None)),
                        'business_type_code': self.safe_int(getattr(herd, 'VirksomhedsArtKode', None)),
                        'herd_type': self.safe_str(getattr(herd, 'BesaetningsTypeTekst', None)),
                        'herd_type_code': self.safe_int(getattr(herd, 'BesaetningsTypeKode', None)),
                        'trade_text': self.safe_str(getattr(herd, 'OmsaetningsTekst', None)),
                        'trade_code': self.safe_int(getattr(herd, 'OmsaetningsKode', None)),
                        'is_organic': int(getattr(herd, 'Oekologisk', 'Nej') == 'Ja'),
                        'herd_status': self.safe_str(getattr(herd, 'BesaetningsStatus', None)),
                        'creation_date': self._parse_date(getattr(herd, 'DatoOpret', None)),
                        'last_update': self._parse_date(getattr(herd, 'DatoOpdatering', None)),
                        'end_date': self._parse_date(getattr(herd, 'DatoOphoer', None)),
                        'last_size_update': self._parse_date(getattr(herd, 'BesStrDatoAjourfoert', None)),
                        'animal_type': self.safe_str(getattr(size, 'BesaetningsStoerrelseTekst', None)),
                        'count': self.safe_int(getattr(size, 'BesaetningsStoerrelse', None))
                    }
                    herd_rows.append({k: v for k, v in herd_row.items() if v is not None})
        
        return herd_rows

    def _process_person_data(self, person: Any, chr_number: int, role: str, herd_number: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Process person (owner/user) data from listOplysninger response."""
        if not person:
            return None
            
        person_id = self.safe_str(getattr(person, 'CvrNummer', None)) or self.safe_str(getattr(person, 'CprNummer', None))
        if not person_id:
            return None
            
        return {
            'cvr_number': self.safe_str(getattr(person, 'CvrNummer', None)),
            'cpr_number': self.safe_str(getattr(person, 'CprNummer', None)),
            'chr_number': chr_number,
            'herd_number': herd_number,
            'role': role,
            'name': self.safe_str(getattr(person, 'Navn', None)),
            'address': self.safe_str(getattr(person, 'Adresse', None)),
            'city': self.safe_str(getattr(person, 'ByNavn', None)),
            'postal_code': self.safe_str(getattr(person, 'PostNummer', None)),
            'postal_district': self.safe_str(getattr(person, 'PostDistrikt', None)),
            'municipality_code': self.safe_int(getattr(person, 'KommuneNummer', None)),
            'municipality_name': self.safe_str(getattr(person, 'KommuneNavn', None)),
            'phone': self.safe_str(getattr(person, 'TelefonNummer', None)),
            'mobile': self.safe_str(getattr(person, 'MobilNummer', None)),
            'email': self.safe_str(getattr(person, 'Email', None)),
            'address_protection': int(getattr(person, 'Adressebeskyttelse', 'Nej') == 'Ja'),
            'advertising_protection': int(getattr(person, 'Reklamebeskyttelse', 'Nej') == 'Ja')
        }

    def _process_practice_data(self, practice: Any, chr_number: int, herd_number: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Process veterinary practice data from listOplysninger response."""
        if not practice:
            return None
            
        practice_number = self.safe_int(getattr(practice, 'PraksisNr', None))
        if not practice_number:
            return None
            
        return {
            'practice_number': practice_number,
            'chr_number': chr_number,
            'herd_number': herd_number,
            'name': self.safe_str(getattr(practice, 'PraksisNavn', None)),
            'address': self.safe_str(getattr(practice, 'PraksisAdresse', None)),
            'city': self.safe_str(getattr(practice, 'PraksisByNavn', None)),
            'postal_code': self.safe_str(getattr(practice, 'PraksisPostNummer', None)),
            'postal_district': self.safe_str(getattr(practice, 'PraksisPostDistrikt', None)),
            'phone': self.safe_str(getattr(practice, 'PraksisTelefonNummer', None)),
            'mobile': self.safe_str(getattr(practice, 'PraksisMobilNummer', None)),
            'email': self.safe_str(getattr(practice, 'PraksisEmail', None))
        }

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

    def _parse_date(self, date_value: Any) -> Optional[str]:
        """Parse date value to ISO format string."""
        if not date_value:
            return None
            
        try:
            if isinstance(date_value, datetime):
                return date_value.date().isoformat()
                
            if isinstance(date_value, str):
                # The API returns dates in YYYY-MM-DD format
                return date_value
                
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date {date_value}: {str(e)}")
            return None

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from the CHR web services."""
        try:
            # First get species data
            species_parser = CHRSpeciesParser(self.config)
            species_data = await species_parser.fetch()
            
            if species_data.empty:
                logger.error("Failed to fetch species data")
                return pd.DataFrame()
            
            # Process each species code
            results = []
            for species_code in species_data['species_code'].unique():
                logger.info(f"Processing species code: {species_code}")
                result = await self.process_species(species_code)
                if result is not None:
                    results.append(result)
            
            # Combine all results
            if results:
                return pd.concat(results, ignore_index=True)
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error during fetch: {str(e)}")
            return pd.DataFrame()

    def fetch_sync(self) -> pd.DataFrame:
        """Synchronous version of fetch."""
        import asyncio
        return asyncio.run(self.fetch())

    async def process_species(self, species_code: int) -> Optional[pd.DataFrame]:
        """Process all CHR numbers for a given species code."""
        try:
            # Get species/usage combinations for this species code
            logger.info(f"Getting species/usage combinations for species code {species_code}")
            species_parser = CHRSpeciesParser(self.config)
            combinations_df = await species_parser.get_species_usage_combinations_async()
            
            if combinations_df.empty:
                logger.warning(f"No species/usage combinations found for species code {species_code}")
                return None
            
            # Filter combinations for this species code
            species_combinations = combinations_df[combinations_df['species_code'] == species_code]
            if species_combinations.empty:
                logger.warning(f"No usage codes found for species code {species_code}")
                return None
            
            logger.info(f"Found {len(species_combinations)} usage codes for species {species_code}")
            
            # Process each usage code
            all_results = []
            for _, row in species_combinations.iterrows():
                usage_code = row['usage_code']
                try:
                    logger.info(f"Processing species code {species_code} with usage code {usage_code}")
                    
                    # Get CHR numbers for this species/usage combination
                    logger.info(f"Fetching CHR numbers for species {species_code}, usage {usage_code}...")
                    chr_numbers_df = await species_parser.get_chr_numbers_async(species_code, usage_code)
                    
                    if not chr_numbers_df.empty:
                        chr_count = len(chr_numbers_df)
                        logger.info(f"Processing {chr_count} CHR numbers for species code {species_code}, usage code {usage_code}")
                        
                        # Process in smaller batches
                        for i in range(0, chr_count, self.UPLOAD_THRESHOLD):
                            batch_df = chr_numbers_df.iloc[i:i + self.UPLOAD_THRESHOLD]
                            batch_num = i//self.UPLOAD_THRESHOLD + 1
                            total_batches = (chr_count + self.UPLOAD_THRESHOLD - 1)//self.UPLOAD_THRESHOLD
                            
                            logger.info(f"Starting batch {batch_num}/{total_batches} for species {species_code}, usage {usage_code}")
                            try:
                                logger.info(f"Processing {len(batch_df)} CHR numbers...")
                                result = self.process_chr_numbers(batch_df['chr_number'].tolist(), species_code)
                                if result is not None:
                                    all_results.append(result)
                                    logger.info(f"Successfully processed and uploaded batch {batch_num}/{total_batches}")
                            except Exception as e:
                                logger.error(f"Error processing batch {batch_num}/{total_batches}: {str(e)}", exc_info=True)
                                continue
                    else:
                        logger.warning(f"No CHR numbers found for species {species_code}, usage {usage_code}")
                        
                except Exception as e:
                    logger.error(f"Error processing usage code {usage_code} for species {species_code}: {str(e)}", exc_info=True)
                    continue
            
            # Combine all results
            if all_results:
                logger.info(f"Combining results for species {species_code}...")
                final_df = pd.concat(all_results, ignore_index=True)
                logger.info(f"Successfully processed all data for species {species_code}")
                return final_df
            
            logger.warning(f"No results generated for species code {species_code}")
            return None
            
        except Exception as e:
            logger.error(f"Error processing species code {species_code}: {str(e)}", exc_info=True)
            return None 