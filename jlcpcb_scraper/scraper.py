import os
import logging
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import concurrent.futures

import requests
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
from bs4 import BeautifulSoup, SoupStrainer

from models import Part, Category, create_or_update_category, create_or_update_part

logger = logging.getLogger(__name__)


JLCPCB_KEY = os.environ.get("JLCPCB_KEY")
JLCPCB_SECRET = os.environ.get("JLCPCB_SECRET")


class JlcpcbScraper:
    def __init__(self, base_url='https://jlcpcb.com/parts', categories: list[Category] = []):
        self.session = requests.Session()
        ua = UserAgent()
        self.session.headers.update({
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Host": "jlcpcb.com",
            "User-Agent": str(ua.chrome),
        })
        self.session.mount('https://', HTTPAdapter(max_retries=3))
        self.base_url = base_url
        self.session.get(self.base_url)
        self.all_links = []
        self.categories: list[Category] = categories
        self.token: str | None = None
        self.token_expires: datetime | None = None
        self.key = JLCPCB_KEY
        self.secret = JLCPCB_SECRET
        self._obtain_token()
        logger.info('JlcpcbScraper initialized')

    def _obtain_token(self) -> None:
        if not self.key or not self.secret:
            raise RuntimeError("JLCPCB_KEY and JLCPCB_SECRET environment variables must be set")
        body = {
            "appKey": self.key,
            "appSecret": self.secret
        }
        headers = {
            "Content-Type": "application/json",
        }
        resp = requests.post("https://jlcpcb.com/external/genToken",
            json=body, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(f"Cannot obtain token {resp.json()}")
        data = resp.json()
        if data["code"] != 200:
            raise RuntimeError(f"Cannot obtain token {data}")
        self.token = data["data"]
        self.session.headers.update({
            "externalApiToken": self.token,
        })
        self.token_expires = datetime.now() + timedelta(seconds=1800)


    def get_all_links(self):
        response = self.session.get(self.base_url+'/all-electronic-components')
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            if '/parts/1st/' in link['href'] or '/parts/2nd/' in link['href']:
                self.all_links.append(link['href'])
        logger.info('All links fetched')

    def extract_categories(self, session, response):
        new_categories = []
        for component in response.get('data', {}).get('componentInfos', []):
            category_name = component.get('firstCategory')
            subcategory_name = component.get('secondCategory')
            if not self.category_exists(subcategory_name):
                new_category = Category(name=category_name, subcategory_name=subcategory_name)
                category = create_or_update_category(session, new_category)
                if category not in self.categories:
                    new_categories.append(new_category)
                    self.categories.append(new_category)
        yield [], new_categories

    def get_parts(self, session):
        # Modified to use ThreadPoolExecutor for parallel requests
        parts_fetched = True
        last_key = None
        all_parts = []
        all_categories = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
            future_to_url = {}
            while parts_fetched:
                if self.token_expires < datetime.now():
                    self._obtain_token()

                url = 'https://jlcpcb.com/external/component/getComponentInfos'
                data = {"lastKey": last_key} if last_key else None
                future = executor.submit(self.session.post, url, data=data)
                future_to_url[future] = url

                for future in concurrent.futures.as_completed(future_to_url):
                    response = future.result().json()
                    if response.get("code") == 200 and response.get('data', {}).get('componentInfos', []):
                        # Process your components here
                        parts, categories = self.extract_and_process_components(session, response)
                        all_parts.extend(parts)
                        all_categories.extend(categories)
                        last_key = response.get('data', {}).get('lastKey', None)
                        parts_fetched = bool(parts)
                        print(f"Length of parts: {len(parts)}, Length of all_parts: {len(all_parts)}")
                    else:
                        parts_fetched = False

                if not parts_fetched:
                    break  # Break the loop if no more parts are fetched

    def extract_and_process_components(self, session, response):
        new_categories = []
        new_parts = []

        # Process categories
        for component in response.get('data', {}).get('componentInfos', []):
            category_name = component.get('firstCategory')
            subcategory_name = component.get('secondCategory')
            if not self.category_exists(subcategory_name):
                new_category = Category(name=category_name, subcategory_name=subcategory_name)
                category = create_or_update_category(session, new_category)
                if category not in self.categories:
                    new_categories.append(new_category)
                    self.categories.append(new_category)

        # Process parts
        for part_data in response.get('data', {}).get('componentInfos', []):
            if part_data['stock'] == 0:
                continue
            part_subcategory_name = part_data['secondCategory']
            category = self.get_category(part_subcategory_name)
            if category is None:
                logger.error(f"Category not found for subcategory: {part_subcategory_name}")
                continue
            part_price = self.get_part_price(part_data['price'])
            new_part = Part(
                lcsc=part_data['lcscPart'],
                category_id=category.id,  # Assuming 'category' has an 'id' attribute
                mfr=part_data['mfrPart'],
                package=part_data['package'],
                joints=int(part_data['solderJoint']),
                manufacturer=part_data['manufacturer'],
                basic=part_data['libraryType'] == 'base',
                description=part_data['description'],
                datasheet=part_data['datasheet'],
                stock=int(part_data['stock']),
                price=part_price,
                last_update=datetime.now()  # Replace with the actual logic if needed
            )
            new_parts.append(new_part)
            create_or_update_part(session, new_part)  # Assuming this function handles DB insertion or update

        return new_parts, new_categories


    def parse_pagination(self, response):
        self.last_key = response.get('data', {}).get('lastKey', None)
        if not self.last_key:
            raise RuntimeError("Cannot obtain last key")

    def get_part_price(self, price: str) -> float | None:
        '''
        string input example: "'20-180:0.004285714,200-780:0.003485714,1600-9580:0.002771429,800-1580:0.003042857,9600-19980:0.002542857,20000-:0.002414286'"
        output example: 0.004285714
        '''
        try:
            if not price:
                return None
            price = price.split(',')
            price = price[0].split(':')
            return float(price[1])
        except Exception as e:
            logger.error(f'Error parsing price: {e}')
            return None

    def category_exists(self, subcategory_name: str) -> bool:
        return any([category.subcategory_name == subcategory_name for category in self.categories])

    def get_category(self, subcategory_name: str) -> Category | None:
        return next((category for category in self.categories if category.subcategory_name == subcategory_name), None)

    def get_category_by_id(self, category_id: int) -> Category | None:
        return next((category for category in self.categories if category.id == category_id), None)


if __name__ == '__main__':
    scraper = JlcpcbScraper()
    scraper.get_parts()
    # save to pkl file
    import pickle
    with open('jlcpcb.pkl', 'wb') as f:
        pickle.dump(scraper.categories, f)