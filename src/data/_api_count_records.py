import requests
import pandas as pd

# Define the API endpoint
url = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Building_Permit_Applications_Feature_Layer_view/FeatureServer/0/query"

# Define the parameters
params = {
    "outFields": "*",
    "where": "1=1",
    "f": "geojson",
    "resultOffset": 0,
    "resultRecordCount": 1000  # Adjust this value if the API allows more records per request
}

all_features = []
while True:
    # Send the GET request
    response = requests.get(url, params=params)
    
    # Check for a successful response
    if response.status_code == 200:
        data = response.json()
        features = data.get("features", [])
        all_features.extend(features)
        
        # Check if there are more records to fetch
        if len(features) < params["resultRecordCount"]:
            break
        
        # Update the resultOffset for the next batch of records
        params["resultOffset"] += params["resultRecordCount"]
    else:
        print(f"Error: {response.status_code} - {response.text}")
        break

# Extract the properties from the features
properties_list = [feature["properties"] for feature in all_features]

# Convert the list of properties to a DataFrame
df = pd.DataFrame(properties_list)

# Write the DataFrame to a CSV file
df.to_csv('./data/raw/building_permits.csv', index=False)

# Print the total number of records
count = len(all_features)
print(f"Number of records: {count}")
