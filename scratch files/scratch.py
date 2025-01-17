from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent='postal_code_to_region')
postal_code = '90027'
location = geolocator.geocode({'postalcode': postal_code, 'country': 'United States'})
if location:
    region = location.raw.get('address', {}).get('state')
    print(f'Region for postal code {postal_code} is: {region}')
else:
    print(f'Region not found for postal code {postal_code}.')