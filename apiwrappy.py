import os
import csv
import time
import datetime
import collections

import requests




_VERBOSE = True


class ApiWrappyError(Exception):
    pass


class RequestEngineError(ApiWrappyError):
    pass


class KeysParserError(ApiWrappyError):
    pass


class RequestEngine:
    def GET(self, url, **kwargs):
        """ Wrapper for requests.get method
        --> requests.Response, dict
        """
        errors = 0
        default_delay_per_request = 0.1
        while True:
            try:
                time.sleep(default_delay_per_request)
                response = requests.get(url, **kwargs)
                response_json = response.json()
                return response, response_json
            
            except Exception as e:
                errors += 1
                error_msg = 'Error({}). {}: {}'.format(
                    e.__class__.__name__, self.__class__.__name__, e
                )
                if errors == self._errors_limit:
                    raise RequestEngineError(error_msg)
                
                if _VERBOSE:
                    print(error_msg)
                

class FileTools:
    CSV_HEADERS = [
        'QUERY', 'TOTAL_API', 'TOTAL_EXTRACTED', 'SOURCE_LINK', 'API_SOURCE', 'ID', 'NAME', 'IS_CLOSED', 'CATEGORY_1', 'CATEGORY_2',
        'CATEGORY_3', 'RATING', 'REVIEWS_AMOUNT', 'LIKES_AMOUNT', 'CHECKINS', 'PRICE', 'LATITUDE',
        'LONGITUDE', 'ADDRESS', 'PHONE', 'WEBSITE', 'CREATED_AT_DATE', 'RUN_TIMESTAMP'
    ]

    def create_default_input(self, filename, encoding='utf-8-sig'):
        """ Creates default input csv file, must end with '_input.csv':
        2019_input.csv, texas_input.csv etc.
        """
        headers = [
            'term', 'location', 'latitude', 'longitude', 'radius', 'limit',
            'foursquare_id', 'facebook_id'
        ]
        with open(filename, 'w', encoding=encoding) as OUT:
            OUT = csv.writer(OUT, delimiter=';', lineterminator='\n', escapechar='\\')
            OUT.writerow(headers)
    
    def csv_out(self, DATA, encoding='utf-8-sig'):
        filename = 'output_{}.csv'.format(datetime.datetime.utcnow().strftime('%d%m%Y_%H%M%S'))
        with open(filename, 'w', encoding=encoding) as OUT:
            OUT = csv.writer(OUT, delimiter=';', lineterminator='\n', escapechar='\\')
            OUT.writerow(self.CSV_HEADERS)
            
            for row in DATA['rows']:
                OUT.writerow([row.get(h) if row.get(h, '') and str(row.get(h, '')).strip() else '-' for h in self.CSV_HEADERS])

                    
class KeysParser:
    """ Key's line must start with a hash sign, "#"
    Headers must contain API provider name with a slash: "yelp_", "foursquare_" etc
    Keys must be separated from headers with the equal sign and whitespaces, " = "
    """
    KEY_PLACEHOLDER = 'set_key_here'
    PARSE_FROM = 'requirements.txt' if not os.path.exists('requirements_dev.txt') else 'requirements_dev.txt'

    if _VERBOSE:
        print('KEYS PARSED:', PARSE_FROM)

    with open(PARSE_FROM, encoding='utf-8') as IN_TXT:
        KEYS_LINES = [
            l.replace('#', '').strip() for l in IN_TXT.readlines() if l.startswith('#') and ' = ' in l.strip()
        ]

    def parse_keys(self, wrapper_alias):
        keys = {}
        for keys_line in self.KEYS_LINES:
            header, key = keys_line.split(' = ')
            header = header.lower()
            if wrapper_alias in header and key != self.KEY_PLACEHOLDER and key:
                keys[header.replace(wrapper_alias, '').strip('_')] = key

        return keys


class InputTermsParser:
    """ {title}_input.csv filename pattern for all input files assumed.
    """
    INPUT_FILES = [f for f in os.listdir() if f.endswith('_input.csv')]

    if _VERBOSE:
        print('INPUT FILES:', INPUT_FILES)

    def filter_empty_values(self, dict_to_filter):
        return {k: v for k, v in dict_to_filter.items() if v.strip()}

    def parse_terms(self, terms_headers):
        terms = []
        for filename in self.INPUT_FILES:
            with open(filename, encoding='utf-8') as IN_CSV:
                csv_data = csv.reader(IN_CSV, delimiter=';'); next(csv_data)
                for row in csv_data:
                    if not row: continue
                    term = self.filter_empty_values(
                        {k: v for k, v in zip(terms_headers, row) if not k.startswith('drop_')}
                    )
                    terms.append(term)

        return terms


class ApiStats:
    def print_api_stats_from_headers(self):
        print('STATS:')
        for k, v in self._response.headers.items():
            if k in self.STATS_HEADERS:
                print('{:>25}: {}'.format(k, v))


class Yelp__ApiWrapper(RequestEngine, KeysParser, InputTermsParser, FileTools, ApiStats):
    API_ALIAS = 'yelp'
    TERMS_HEADERS = [
        'term', 'location', 'latitude', 'longitude', 'radius', 'limit'
    ]
    ENDPOINTS = {
        'BUSINESS_SEARCH': 'https://api.yelp.com/v3/businesses/search',
    }
    STATS_HEADERS = ['RateLimit-DailyLimit', 'RateLimit-Remaining', 'RateLimit-ResetTime']

    def __init__(self, *, timeout=3, errors_limit=3, delay_per_request=0.1):
        """ Currently Yelp fusion API limits maximim results returned (1000).
        API sets limit per offset maximum results returned (50).
        
        self.yelp_api_results_limit and self.limit_max_per_offset shall not be increased,
        unless source API limits are extended.
        
        Source info link:
            https://www.yelp.com/developers/faq
        """
        self.yelp_api_results_max_limit = 1000
        self.limit_max_per_offset = 50
        self._errors_limit = errors_limit
        self._timeout = timeout
        self._delay_per_request = delay_per_request

        self.keys = self.parse_keys(self.API_ALIAS)
        self._terms = self.parse_terms(self.TERMS_HEADERS)
        self._headers = {'Authorization': 'Bearer {}'.format(self.keys.get('apikey', ''))}

    def business_search_endpoint(self, query):
        """ This endpoint returns up to 1000 businesses based on the provided search criteria.
        It has some basic information about the business.
        """
        self._response, self._response_json = self.GET(
            self.ENDPOINTS['BUSINESS_SEARCH'], headers=self._headers,
            timeout=self._timeout, params=query
        )

    def parse_business_search_json_response(self, businesses, DATA, search_term_str):
        
        if _VERBOSE:
            print('  amount of businesses extracted: {:>5} (Total API ~ {:>5})'.format(
                len(businesses), self._response_json.get('total', '-')
                ), end='\n'*2
            )

        for business in businesses:
            ROW = {}
            ROW['API_SOURCE'] = self.API_ALIAS
            ROW['SOURCE_LINK'] = business.get('url', '-')
            
            ROW['QUERY'] = search_term_str
            ROW['TOTAL_API'] = self._response_json.get('total', '-')
            ROW['TOTAL_EXTRACTED'] = len(businesses)
        
            ROW['ID'] = business.get('id', '-')
            ROW['NAME'] = business.get('name', '-')
            ROW['IS_CLOSED'] = str(business.get('is_closed', '-'))

            categories = business.get('categories')
            if categories:
                category_num = 0
                for category in categories:
                    category_num += 1
                    category_header = 'CATEGORY_{}'.format(category_num)
                    ROW[category_header] = category.get('alias', '-')

            ROW['REVIEWS_AMOUNT'] = business.get('review_count', '-')
            ROW['RATING'] = business.get('rating', '-')
            ROW['PRICE'] = len(business.get('price')) if '$' in business.get('price', '-') else '-'
            ROW['LATITUDE'] = business.get('coordinates', {}).get('latitude', '-')
            ROW['LONGITUDE'] = business.get('coordinates', {}).get('longitude', '-')
            ROW['ADDRESS'] = ', '.join(business.get('location', {}).get('display_address', '-'))
            ROW['PHONE'] = business.get('phone', '-').replace('+', '')
            ROW['RUN_TIMESTAMP'] = datetime.datetime.utcnow().strftime('%d.%m.%Y/%H:%M:%S')

            DATA['rows'].append(ROW)

    def run(self, DATA):
        for term in self._terms:
            if not any(True for i in ['location', 'latitude', 'longitude'] if term.get(i)):
                continue

            search_term_str = '|'.join([str(v) for v in term.values() if str(v).strip()])

            if _VERBOSE:
                print('+ {}'.format(search_term_str))
            
            businesses = []
            term_limit_user = int(term.get('limit')) if term.get('limit') else None
            offset = 0
            if term_limit_user and term_limit_user > self.limit_max_per_offset:
                term['limit'] = self.limit_max_per_offset
                
                while offset < term_limit_user and offset < self.yelp_api_results_max_limit:
                    term['offset'] = offset
                    self.business_search_endpoint(term)
                    time.sleep(self._delay_per_request)
                    businesses_from_json_response = self._response_json.get('businesses', [])

                    if _VERBOSE and offset % 100 == 0:
                        print('  offset {}'.format(offset))

                    offset += 50
                    
                    if businesses_from_json_response:
                        businesses.extend(businesses_from_json_response)
                    else:
                        break
            else:
                self.business_search_endpoint(term)
                businesses.extend(self._response_json['businesses'])

            self.parse_business_search_json_response(businesses, DATA, search_term_str)


class Foursquare__ApiWrapper(RequestEngine, KeysParser, InputTermsParser, FileTools, ApiStats):
    API_ALIAS = 'foursquare'
    TERMS_HEADERS = [
        'query', 'near', 'latitude', 'longitude', 'radius', 'limit', 'foursquare_id'
    ]
    ENDPOINTS = {
        'VENUE_SEARCH': 'https://api.foursquare.com/v2/venues/search',
        'VENUE_DETAILS': 'https://api.foursquare.com/v2/venues/{}'
    }
    STATS_HEADERS = ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'Date']

    def __init__(self, *, errors_limit=3, delay_per_request=0.1):
        self._errors_limit = errors_limit
        self._delay_per_request = delay_per_request
        
        self.keys = self.parse_keys(self.API_ALIAS)
        self._client_id = self.keys.get('client_id', '')
        self._client_secret = self.keys.get('client_secret', '')
        self._terms = self.parse_terms(self.TERMS_HEADERS)
        self._term_current_date = datetime.date.today().strftime('%Y%m%d')

    def _update_term_credentials(self, term):
        term['client_id'] = self._client_id
        term['client_secret'] = self._client_secret
        term['v'] = self._term_current_date
                     
    def venue_details_endpoint(self, term):
        """ Gives the full details about a venue including location, tips, and categories.
        If the venue ID given is one that has been merged into another “master” venue, the
        response will show data about the “master” instead of giving you an error.
        """
        self._response, self._response_json = self.GET(
            self.ENDPOINTS['VENUE_DETAILS'].format(term.pop('foursquare_id')), params=term
        )

    def venue_search_endpoint(self, term):
        """ Returns a list of venues near the current location, optionally matching a search term.
        """
        self._response, self._response_json = self.GET(self.ENDPOINTS['VENUE_SEARCH'], params=term)

    def parse_venues_search_json_response(self):
        venues = self._response_json.get('response', {}).get('venues', {})
        return [{'foursquare_id': venue['id']} for venue in venues if venue.get('id')]
    
    def parse_venue_details_json_response(self, DATA, search_term_str):
        venue = self._response_json['response']['venue']

        ROW = {}
        ROW['QUERY'] = search_term_str
        ROW['SOURCE_LINK'] = venue.get('canonicalUrl', '-')
        ROW['API_SOURCE'] = self.API_ALIAS
        ROW['ID'] = venue.get('id', '-')
        ROW['NAME'] = venue.get('name', '-')
        ROW['PHONE'] = venue.get('contact', {}).get('phone', '-')

        ROW['ADDRESS'] = ', '.join(venue.get('location', {}).get('formattedAddress', '-'))
        ROW['LATITUDE'] = venue.get('location', {}).get('lat', '-')
        ROW['LONGITUDE'] = venue.get('location', {}).get('lng', '-')

        categories = venue.get('categories')
        if categories:
            category_num = 0
            for category in categories:
                category_num += 1
                category_header = 'CATEGORY_{}'.format(category_num)
                ROW[category_header] = category.get('name', '-')
        
        ROW['CHECKINS'] = venue.get('stats', {}).get('checkinsCount', '-')
        ROW['WEBSITE'] = venue.get('url', '-')
        ROW['PRICE'] = venue.get('price', {}).get('tier', {})
        ROW['RATING'] = venue.get('rating', '-')
        ROW['REVIEWS_AMOUNT'] = venue.get('ratingSignals', '-')
        ROW['CREATED_AT'] = venue.get('createdAt', '-')
        ROW['LIKES_AMOUNT'] = venue.get('likes', {}).get('count', '-')

        if isinstance(ROW['CREATED_AT'], int):
            converted_epoch = datetime.datetime.fromtimestamp(ROW['CREATED_AT']).strftime('%d-%m-%Y')
        else:
            converted_epoch = '-'
        ROW['CREATED_AT_DATE'] = converted_epoch
        ROW['RUN_TIMESTAMP'] = datetime.datetime.utcnow().strftime('%d.%m.%Y/%H:%M:%S')
         
        DATA['rows'].append(ROW)
            
    def run(self, DATA):
        venues_details_terms = collections.defaultdict(list)
        for term in self._terms:
            if not any(True for i in ['q', 'near', 'latitude', 'longitude', 'foursquare_id'] if term.get(i)):
                continue
            
            time.sleep(self._delay_per_request)
            search_term_str = '|'.join([v for k, v in term.items() if k != 'client_id' and k != 'client_secret'])

            if _VERBOSE:
                print('+ {}'.format(search_term_str))
                
            self._update_term_credentials(term)
            if term.get('latitude') and term.get('longitude'):
                term['ll'] = '{},{}'.format(term.pop('latitude'), term.pop('longitude'))
            
            if term.get('foursquare_id'):
                venues_details_terms[search_term_str].append({'foursquare_id': term.get('foursquare_id')})
                print('  foursquare id added to the queue', end='\n'*2)
                continue
                
            self.venue_search_endpoint(term)
            ids = self.parse_venues_search_json_response()
            venues_details_terms[search_term_str].extend(ids)

            if _VERBOSE:
                print('  amount of venues extracted: {}'.format(len(ids)), end='\n'*2)
            
        for search_term_str, terms in venues_details_terms.items():
            for term in terms:
                self._update_term_credentials(term)
                self.venue_details_endpoint(term)
                self.parse_venue_details_json_response(DATA, search_term_str)


class Facebook__ApiWrapper(RequestEngine, KeysParser, InputTermsParser, FileTools, ApiStats):
    API_ALIAS = 'facebook'
    TERMS_HEADERS = [
        'q', 'drop_1', 'latitude', 'longitude', 'radius', 'limit', 'drop_3', 'facebook_id'
    ]
    
    PLACES_CATEGORIES = [
        'ARTS_ENTERTAINMENT', 'EDUCATION', 'FITNESS_RECREATION',
        'FOOD_BEVERAGE', 'HOTEL_LODGING', 'MEDICAL_HEALTH',
        'SHOPPING_RETAIL', 'TRAVEL_TRANSPORTATION'
    ]

    ENDPOINTS = {
        'PLACES_SEARCH': 'https://graph.facebook.com/v3.2/search',
        'PLACE_INFORMATION': 'https://graph.facebook.com/v3.3/{}'
    }

    ID_FIELDS = [
        'about', 'website', 'category_list', 'checkins', 'cover', 'engagement', 'hours', 'id',
        'is_always_open', 'app_links', 'is_permanently_closed', 'is_verified', 'description',
        'link', 'location', 'name', 'overall_star_rating', 'parking', 'payment_options', 'phone',
        'price_range', 'rating_count', 'restaurant_services', 'restaurant_specialties',
        'single_line_address'
    ]

    STATS_HEADERS = ['x-app-usage']

    def __init__(self, *, errors_limit=3, delay_per_request=0.1):
        self._errors_limit = errors_limit
        self._delay_per_request = delay_per_request
        
        self.keys = self.parse_keys(self.API_ALIAS)
        self._access_token = self.keys.get('access_token', '')
        
        self._terms = self.parse_terms(self.TERMS_HEADERS)

    def _update_term_credentials(self, term):
        term['access_token'] = self._access_token

    def places_search_endpoint(self, term):
        """ q(term) or center required.
        """
        self._response, self._response_json = self.GET(
            self.ENDPOINTS['PLACES_SEARCH'], params=term
        )

    def places_info_endpoint(self, term):
        """ q(term) or center required.
        """
        self._response, self._response_json = self.GET(
            self.ENDPOINTS['PLACE_INFORMATION'].format(term.pop('facebook_id')), params=term
        )

    def parse_places_details_json_response(self):
        places = self._response_json.get('data', [])
        return [{'facebook_id': place['id']} for place in places if place.get('id')]

    def parse_places_info_json_response(self, DATA, search_term_str):
        place = self._response_json

        ROW = {}
        ROW['QUERY'] = search_term_str
        ROW['API_SOURCE'] = self.API_ALIAS
        ROW['SOURCE_LINK'] = place.get('link', '-')
        ROW['ID'] = "'{}".format(place.get('id', '-'))
        ROW['NAME'] = place.get('name', '-')
        ROW['PHONE'] = place.get('phone', '-')

        ROW['ADDRESS'] = place.get('single_line_address', '-')
        ROW['LATITUDE'] = place.get('location', {}).get('latitude', '-')
        ROW['LONGITUDE'] = place.get('location', {}).get('longitude', '-')

        categories = place.get('category_list')
        if categories:
            category_num = 0
            for category in categories:
                category_num += 1
                category_header = 'CATEGORY_{}'.format(category_num)
                ROW[category_header] = category.get('name', '-')
        
        ROW['WEBSITE'] = place.get('website', '-')
        ROW['PRICE'] = len(place.get('price_range')) if '$' in place.get('price_range', '-') else '-'
        ROW['REVIEWS_AMOUNT'] = str(place.get('rating_count', '-')).replace('.', ',')
        ROW['RATING'] = place.get('overall_star_rating', '-')
        ROW['IS_CLOSED'] = str(place.get('is_permanently_closed', '-'))
        ROW['LIKES_AMOUNT'] = place.get('engagement', {}).get('count', '-')
        ROW['CHECKINS'] = place.get('checkins', '-')
        ROW['RUN_TIMESTAMP'] = datetime.datetime.utcnow().strftime('%d.%m.%Y/%H:%M:%S')
 
        DATA['rows'].append(ROW)

    def run(self, DATA):
        places_details_terms = collections.defaultdict(list)
        for term in self._terms:
            if not any(True for i in ['q', 'latitude', 'longitude', 'facebook_id'] if term.get(i)):
                continue
            
            time.sleep(self._delay_per_request)
            search_term_str = '|'.join([v for k, v in term.items() if k != 'access_token'])
            
            if _VERBOSE:
                print('+ {}'.format(search_term_str))

            self._update_term_credentials(term)
            term['type'] = 'place'

            if term.get('facebook_id'):
                places_details_terms[search_term_str].append({'facebook_id': term.get('facebook_id')})
                print('  facebook id added to the queue', end='\n'*2)
                continue

            if not term.get('limit'):
                term['limit'] = 100

            if term.get('latitude') and term.get('longitude'):
                term['center'] = '{},{}'.format(term.pop('latitude'), term.pop('longitude'))

            self.places_search_endpoint(term)
            ids = self.parse_places_details_json_response()
            places_details_terms[search_term_str].extend(ids)  

            if _VERBOSE:
                print('  amount of places extracted: {}'.format(len(ids)), end='\n'*2)

        for search_term_str, terms in places_details_terms.items():
            for term in terms:
                term['fields'] = ','.join(self.ID_FIELDS)
                self._update_term_credentials(term)
                self.places_info_endpoint(term)
                self.parse_places_info_json_response(DATA, search_term_str)


def main(wrapper_classes):
    filetools = FileTools()
    default_input_filename = '_input.csv'
    
    if not os.path.exists(default_input_filename):
        filetools.create_default_input(default_input_filename)

    DATA = collections.defaultdict(list)
    for wrapper_class in wrapper_classes:
        wrapper = wrapper_class()

        if _VERBOSE:
            print('-'*60)
            print('RUNNING:', wrapper_class.__name__, end='\n'*2)

        try:
            if not wrapper.keys: raise KeysParserError('Keys not set')
            
            wrapper.run(DATA)

            if _VERBOSE:
                wrapper.print_api_stats_from_headers()
                
            if _VERBOSE and wrapper._response.status_code != 200:
                print('\n:{}: {}'.format(
                    wrapper.__class__.__name__.upper(), wrapper._response.text
                    )
                )
                
        except KeysParserError as e:
            print(e)
        
        except Exception as e:
            print('*'*3, '\nFAILED {} : {}'.format(wrapper.__class__.__name__, e))

    filetools.csv_out(DATA)




if __name__ == '__main__':
    main(wrapper_classes=[
            Yelp__ApiWrapper,
            Foursquare__ApiWrapper,
            Facebook__ApiWrapper
        ]
    )
