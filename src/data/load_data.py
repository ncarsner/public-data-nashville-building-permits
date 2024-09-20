import requests
import csv
from configparser import ConfigParser

# Read the configuration file
config = ConfigParser(interpolation=None)
config.read('config.ini')
endpoint = config['endpoints']['building_permits_geojson']

# Fetch the data from the URL
response = requests.get(endpoint)
response.raise_for_status()  # Raise an error for bad status codes

# Parse the data and extract the headers
data = response.json()
headers = list(data['features'][0]['properties'].keys())

# Open the CSV file for writing
with open('./data/raw/building_permits.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    
    # Write the headers
    csvwriter.writerow(headers)
    
    # Write the data rows
    for feature in data['features']:
        properties = feature['properties']
        row = [properties.get(header) for header in headers]
        csvwriter.writerow(row)
