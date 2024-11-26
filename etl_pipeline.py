import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from fake_useragent import UserAgent
import logging
from datetime import datetime
import json
import re
from typing import Dict, List, Any
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

class RealEstateScraper:
    def __init__(self):
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize user agent generator
        self.ua = UserAgent()
        
        # Base URLs
        self.magicbricks_url = "https://www.magicbricks.com"
        
        # Initialize session
        self.session = requests.Session()
        
        # Target cities
        self.target_cities = [
            "mumbai", "delhi", "bangalore", "pune", "hyderabad",
            "chennai", "kolkata", "ahmedabad"
        ]
        
        # City metadata mapping
        self.city_mapping = {
            "mumbai": {"state": "Maharashtra", "tier": "Tier 1"},
            "delhi": {"state": "Delhi", "tier": "Tier 1"},
            "bangalore": {"state": "Karnataka", "tier": "Tier 1"},
            "pune": {"state": "Maharashtra", "tier": "Tier 2"},
            "hyderabad": {"state": "Telangana", "tier": "Tier 1"},
            "chennai": {"state": "Tamil Nadu", "tier": "Tier 1"},
            "kolkata": {"state": "West Bengal", "tier": "Tier 1"},
            "ahmedabad": {"state": "Gujarat", "tier": "Tier 2"}
        }

    def get_headers(self) -> Dict[str, str]:
        """Generate random headers for each request"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

    def _add_random_delay(self):
        """Add random delay between requests to avoid detection"""
        time.sleep(random.uniform(1, 3))

    def scrape_magicbricks(self, city: str) -> List[Dict]:
        """Scrape property listings from MagicBricks"""
        properties = []
        try:
            base_url = f"{self.magicbricks_url}/property-for-sale/residential-real-estate?proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment&cityName={city}"
            
            # Add error handling for initial request
            response = self.session.get(base_url, headers=self.get_headers())
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch data for {city}: Status {response.status_code}")
                return properties
            
            for page in range(1, 3):
                self._add_random_delay()
                page_url = f"{base_url}&page={page}"
                response = self.session.get(page_url, headers=self.get_headers())
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to fetch page {page} for {city}")
                    continue
                    
                soup = BeautifulSoup(response.content, 'html.parser')
                property_cards = soup.find_all('div', {'class': 'mb-srp__card'})
                
                for card in property_cards:
                    try:
                        # Check all required elements before extraction
                        title_elem = card.find('h2', {'class': 'mb-srp__card--title'})
                        price_elem = card.find('div', {'class': 'mb-srp__card__price--amount'})
                        area_elem = card.find('div', {'class': 'mb-srp__card__summary--value'})
                        location_elem = title_elem.find_next('div') if title_elem else None
                        description_elem = card.find('div', {'class': 'mb-srp__card--desc'})
                        
                        # Only process if all required elements are present
                        if all([title_elem, price_elem, area_elem, location_elem]):
                            property_data = {
                                'source': 'magicbricks',
                                'city': city,
                                'title': title_elem.text.strip(),
                                'price': self._extract_price(price_elem.text),
                                'area': self._extract_area(area_elem.text),
                                'location': location_elem.text.strip(),
                                'description': description_elem.text.strip() if description_elem else '',
                                'timestamp': datetime.now().isoformat()
                            }
                            properties.append(property_data)
                        else:
                            self.logger.warning(f"Skipping incomplete property card in {city}")
                            
                    except AttributeError as e:
                        self.logger.error(f"Error parsing property card: {str(e)}")
                        continue
                    except Exception as e:
                        self.logger.error(f"Unexpected error: {str(e)}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Error scraping MagicBricks for {city}: {str(e)}")
        
        return properties

    def _extract_price(self, price_text: str) -> float:
        """Extract and standardize price values"""
        try:
            if not price_text:
                return 0.0
                
            price_text = price_text.lower().strip()
            number = float(re.findall(r'[\d.]+', price_text)[0])
            
            if 'cr' in price_text:
                return number * 10000000
            elif 'lac' in price_text or 'lakh' in price_text:
                return number * 100000
            return number
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Failed to extract price from '{price_text}': {str(e)}")
            return 0.0

    def _extract_area(self, area_text: str) -> Dict[str, float]:
        """Extract and standardize area values"""
        try:
            if not area_text:
                return {'value': 0.0, 'unit': 'sqft'}
                
            area_text = area_text.lower().strip()
            number = float(re.findall(r'[\d.]+', area_text)[0])
            unit = 'sqmt' if ('sqmt' in area_text or 'sq.mt.' in area_text) else 'sqft'
            
            return {'value': number, 'unit': unit}
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Failed to extract area from '{area_text}': {str(e)}")
            return {'value': 0.0, 'unit': 'sqft'}

    def clean_data(self, properties: List[Dict]) -> pd.DataFrame:
        """Clean and standardize scraped data according to database schema"""
        if not properties:
            return pd.DataFrame()
                
        df = pd.DataFrame(properties)
        
        # Extract property master data 
        property_df = pd.DataFrame({
            'property_name': df['title'].str[:200],
            'total_area_sqft': df['area'].apply(
                lambda x: x['value'] * 10.764 if x['unit'] == 'sqmt' else x['value']
            ),
            'price': df['price'],
            'rera_number': df['rera_number'] if 'rera_number' in df.columns else None,
            'created_at': pd.to_datetime(df['timestamp'])
        })

        # Extract location data
        locations = df.apply(lambda row: {
            'locality': row['location'].split(',')[0].strip()[:100] if row['location'] else None,
            'city': row['city'].strip()[:100] if row['city'] else None
        }, axis=1)

        location_df = pd.DataFrame({
            'state_name': df['city'].map(
                lambda x: self.city_mapping[x.lower()]['state'] if pd.notna(x) else None
            ).str[:100],
            'city_name': df['city'].str.title().str[:100],
            'locality_name': locations.apply(lambda x: x['locality']),
            'pin_code': None,
            'created_at': pd.to_datetime(df['timestamp'])
        })

        # Extract property type data
        property_type_df = pd.DataFrame({
            'category': df['category'] if 'category' in df.columns else 'Residential',
            'sub_category': df['sub_category'] if 'sub_category' in df.columns else 'Apartment',
            'configuration': df['configuration'] if 'configuration' in df.columns else None,
            'created_at': pd.to_datetime(df['timestamp'])
        })

        # Extract builder data
        builder_df = pd.DataFrame({
            'builder_name': df['description'].apply(lambda x: self._extract_builder_name(x)).fillna('Unknown Builder').str[:200],
            'rera_id': df['rera_id'] if 'rera_id' in df.columns else None,
            'created_at': pd.to_datetime(df['timestamp'])
        })

        return property_df, location_df, property_type_df, builder_df

    def _extract_builder_name(self, description: str) -> str:
        """Extract builder name from property description"""
        match = re.search(r'property belongs to "([^"]+)"', description, re.IGNORECASE)
        return match.group(1) if match else None

    def save_to_database(self, property_df: pd.DataFrame, location_df: pd.DataFrame, property_type_df: pd.DataFrame, builder_df: pd.DataFrame):
        """Save data to PostgreSQL database with transaction handling"""
        try:
            if property_df.empty or location_df.empty or property_type_df.empty or builder_df.empty:
                self.logger.warning("No data to save to database.")
                return
                
            engine = create_engine('postgresql://postgres:admin@localhost:5432/real_estate_db')
            Session = sessionmaker(bind=engine)
            session = Session()
            
            with session.begin():
                # Insert dimensions
                if not location_df.empty:
                    location_df.to_sql('dim_location', engine, if_exists='append', index=False)
                    self.logger.info("Data successfully added to dim_location.")
                
                if not property_type_df.empty:
                    property_type_df.to_sql('dim_property_type', engine, if_exists='append', index=False)
                    self.logger.info("Data successfully added to dim_property_type.")
                
                if not builder_df.empty:
                    builder_df.to_sql('dim_builder', engine, if_exists='append', index=False)
                    self.logger.info("Data successfully added to dim_builder.")
            
                # Insert facts
                if not property_df.empty:
                    property_df.to_sql('fact_property_master', engine, if_exists='append', index=False)
                    self.logger.info("Data successfully added to fact_property_master.")
            
        except Exception as e:
            self.logger.error(f"Database error: {str(e)}", exc_info=True)
            raise
        finally:
            session.close()

    def save_to_json(self, data: List[Dict], filename: str):
        """Save raw scraped data to JSON file"""
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def run_scraper(self):
        """Run the complete ETL pipeline"""
        all_properties = []
        
        for city in self.target_cities:
            self.logger.info(f"Scraping data for {city}")
            
            try:
                # Scrape from MagicBricks
                magicbricks_properties = self.scrape_magicbricks(city)
                self.logger.info(f"Scraped {len(magicbricks_properties)} properties from MagicBricks for {city}")
                
                # Add delay between sources
                self._add_random_delay()
                
                # Save raw data
                if magicbricks_properties:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    self.save_to_json(magicbricks_properties, f'raw_data_magicbricks_{city}_{timestamp}.json')
                
                all_properties.extend(magicbricks_properties)
                
            except Exception as e:
                self.logger.error(f"Error processing {city}: {str(e)}")
                continue
        
        if all_properties:
            # Clean and transform data
            property_df, location_df, property_type_df, builder_df = self.clean_data(all_properties)
            
            # Save to database
            self.save_to_database(property_df, location_df, property_type_df, builder_df)
            
            # Save processed data to CSV as backup
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = 'output'
            os.makedirs(output_dir, exist_ok=True)
            property_df.to_csv(os.path.join(output_dir, f'processed_data_property_{timestamp}.csv'), index=False)
            location_df.to_csv(os.path.join(output_dir, f'processed_data_location_{timestamp}.csv'), index=False)
            property_type_df.to_csv(os.path.join(output_dir, f'processed_data_property_type_{timestamp}.csv'), index=False)
            builder_df.to_csv(os.path.join(output_dir, f'processed_data_builder_{timestamp}.csv'), index=False)
            
            return property_df, location_df, property_type_df, builder_df
        
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


scraper = RealEstateScraper()
property_df, location_df, property_type_df, builder_df = scraper.run_scraper()