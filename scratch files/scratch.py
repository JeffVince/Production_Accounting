from geopy.geocoders import Nominatim

# Initialize the geolocator
geolocator = Nominatim(user_agent="postal_code_to_region")

# Enter the postal code
postal_code = "90027"  # Example: Los Angeles, CA

# Perform geocoding
location = geolocator.geocode({"postalcode": postal_code, "country": "United States"})

# Extract the region code (state) if available
if location:
    region = location.raw.get("address", {}).get("state")
    print(f"Region for postal code {postal_code} is: {region}")
else:
    print(f"Region not found for postal code {postal_code}.")