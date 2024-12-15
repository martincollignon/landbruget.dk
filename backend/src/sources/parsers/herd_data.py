import logging
from typing import Dict, List, Any, Optional
from zeep import Client
from zeep.transports import Transport
from requests import Session
from zeep.wsse.username import UsernameToken
import certifi
from datetime import datetime
import pandas as pd

# Import Source from the correct path
from backend.src.base import Source

logger = logging.getLogger(__name__)

class HerdDataParser(Source):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.username = self.get_secret('fvm_username')
        self.password = self.get_secret('fvm_password')
        
        # Setup secure client
        session = Session()
        session.verify = certifi.where()
        transport = Transport(session=session)
        
        # Create SOAP client for property service
        self.wsdl_url = 'https://ws.fvst.dk/service/CHR_ejendomWS?wsdl'
        self.client = Client(
            self.wsdl_url, 
            transport=transport, 
            wsse=UsernameToken(self.username, self.password)
        )
        
        logger.info("Available operations:")
        for operation in self.client.wsdl.services.values():
            for port in operation.ports.values():
                logger.info(f"{[op for op in port.binding._operations.keys()]}")

    def fetch(self) -> pd.DataFrame:
        """Required implementation of abstract method"""
        return pd.DataFrame()

    def safe_get(self, obj: Any, *attrs: str) -> Optional[Any]:
        for attr in attrs:
            try:
                obj = getattr(obj, attr)
            except AttributeError:
                return None
        return obj

    def query_herd(self, chr_number: str) -> Dict[str, Any]:
        # First get property details
        property_request = {
            'GLRCHRWSInfoInbound': {
                'KlientId': 'LandbrugsData',
                'BrugerNavn': self.username,
                'SessionId': '1',
                'IPAdresse': '',
                'TrackID': chr_number
            },
            'Request': {
                'ChrNummer': chr_number
            }
        }

        try:
            # Get property details first
            property_result = self.client.service.hentCHRStamoplysninger(property_request)
            logger.debug(f"Property response: {property_result}")
            
            if not (hasattr(property_result, 'Response') and property_result.Response):
                logger.warning(f"No property found for CHR {chr_number}")
                return None
            
            # Now get herd details
            herd_request = {
                'GLRCHRWSInfoInbound': {
                    'KlientId': 'LandbrugsData',
                    'BrugerNavn': self.username,
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': chr_number
                },
                'Request': {
                    'ChrNummer': chr_number
                }
            }
            
            herd_result = self.client.service.listBesaetninger(herd_request)
            logger.debug(f"Herd response: {herd_result}")
            
            # Combine and clean up the data
            property_data = property_result.Response[0]
            result = {
                'CHRNummer': property_data['ChrNummer'],
                'Adresse': property_data['Ejendom']['Adresse'],
                'By': property_data['Ejendom']['ByNavn'],
                'PostNummer': property_data['Ejendom']['PostNummer'],
                'PostDistrikt': property_data['Ejendom']['PostDistrikt'],
                'Kommune': property_data['Ejendom']['KommuneNavn'],
                'Koordinater': {
                    'X': property_data['StaldKoordinater']['StaldKoordinatX'],
                    'Y': property_data['StaldKoordinater']['StaldKoordinatY']
                },
                'DatoOpret': property_data['Ejendom']['DatoOpret'],
                'DatoOpdatering': property_data['Ejendom']['DatoOpdatering'],
                'Besaetninger': []
            }
            
            # Add herd information if available
            if (hasattr(herd_result, 'Response') and 
                hasattr(herd_result.Response, 'Besaetninger') and 
                hasattr(herd_result.Response.Besaetninger, 'Besaetning')):
                
                for herd in herd_result.Response.Besaetninger.Besaetning:
                    herd_info = {
                        'BesaetningsNummer': herd['BesaetningsNummer'],
                        'DyreArt': herd['DyreArtTekst'],
                        'BrugsArt': herd['BrugsArtTekst'],
                        'VirksomhedsArt': herd['VirksomhedsArtTekst'],
                        'Stoerrelse': {
                            item['BesaetningsStoerrelseTekst']: item['BesaetningsStoerrelse']
                            for item in herd['BesStr']
                        },
                        'StoerrelseOpdateret': herd['BesStrDatoAjourfoert'],
                        'Ejer': herd['Ejer']['Navn'],
                        'Bruger': herd['Bruger']['Navn'],
                        'Dyrlæge': {
                            'Navn': herd['BesPraksis']['PraksisNavn'],
                            'Adresse': herd['BesPraksis']['PraksisAdresse']
                        },
                        'Oekologisk': herd['Oekologisk'] == 'Ja',
                        'DatoOpret': herd['DatoOpret'],
                        'DatoOpdatering': herd['DatoOpdatering']
                    }
                    result['Besaetninger'].append(herd_info)
            
            return result
            
        except Exception as e:
            logger.error(f"Error querying CHR {chr_number}: {str(e)}")
            logger.debug(f"Request was: {property_request}")
            return None

    def process_result(self, chr_number: str, result: Any) -> Optional[Dict[str, Any]]:
        if hasattr(result, 'GLRCHRSvarMeddelelser'):
            messages = result.GLRCHRSvarMeddelelser.Meddelelse
            if isinstance(messages, list):
                for msg in messages:
                    logger.warning(f"Message for CHR {chr_number}: {msg.MeddelelseType} - {msg.MeddelelseTekst}")
            else:
                logger.warning(f"Message for CHR {chr_number}: {messages.MeddelelseType} - {messages.MeddelelseTekst}")
            return None

        try:
            if isinstance(result.Response, list):
                logger.info(f"Response is a list with {len(result.Response)} items")
                if not result.Response:
                    logger.warning(f"No data found for CHR {chr_number}")
                    return None
                return result.Response[0]
            return None
        except AttributeError as e:
            logger.error(f"Error processing result for CHR {chr_number}: {str(e)}")
            return None

    def sync(self) -> None:
        # Implementation for batch processing
        pass

    def list_all_herds(self) -> List[Dict[str, Any]]:
        # Create SOAP client for herd service
        herd_client = Client(
            'https://ws.fvst.dk/service/CHR_besaetningWS?wsdl',
            transport=self.client.transport,
            wsse=UsernameToken(self.username, self.password)
        )
        
        # Common animal type and usage combinations
        combinations = [
            {'dyreArtKode': 11, 'brugsArtKode': 11},  # Horses - meat
            {'dyreArtKode': 12, 'brugsArtKode': 11},  # Cattle - meat
            {'dyreArtKode': 13, 'brugsArtKode': 11},  # Pigs - meat
        ]
        
        all_herds = []
        
        for combo in combinations:
            besNrFra = 0
            has_more = True
            
            while has_more:
                request = {
                    'GLRCHRWSInfoInbound': {
                        'KlientId': 'LandbrugsData',
                        'BrugerNavn': self.username,
                        'SessionId': '1',
                        'IPAdresse': '',
                        'TrackID': f"list_{combo['dyreArtKode']}_{combo['brugsArtKode']}"
                    },
                    'Request': {
                        'DyreArtKode': combo['dyreArtKode'],
                        'BrugsArtKode': combo['brugsArtKode'],
                        'BesNrFra': besNrFra
                    }
                }

                try:
                    result = herd_client.service.listBesaetningerMedBrugsart(request)
                    logger.info(f"Got response for {combo}:")
                    logger.info(f"Response type: {type(result)}")
                    logger.info(f"Response attributes: {dir(result)}")
                    logger.info(f"Response.Response attributes: {dir(result.Response)}")
                    logger.info(f"Full response: {result}")
                    
                    if hasattr(result.Response, 'BesaetningsNumre'):
                        numbers = result.Response.BesaetningsNumre
                        if isinstance(numbers, list):
                            chr_numbers = numbers
                        else:
                            chr_numbers = getattr(numbers, 'Numre', [])
                        
                        logger.info(f"Processing {len(chr_numbers)} CHR numbers")
                        
                        # Process each CHR number
                        for chr_number in chr_numbers:
                            try:
                                herd_data = self.query_herd(str(chr_number))
                                if herd_data:
                                    all_herds.append(herd_data)
                                    if len(all_herds) % 100 == 0:
                                        logger.info(f"Processed {len(all_herds)} herds so far")
                            except Exception as e:
                                logger.error(f"Error querying herd {chr_number}: {str(e)}")
                                continue
                    
                    # Check for more results
                    has_more = result.Response.FlereBesaetninger
                    if has_more:
                        besNrFra = result.Response.TilBesNr + 1
                        logger.info(f"Moving to next batch starting from {besNrFra}")
                except Exception as e:
                    logger.error(f"Error listing herds for {combo}: {str(e)}")
                    logger.error(f"Response was: {result}")
                    has_more = False
                    continue
                        
        return all_herds