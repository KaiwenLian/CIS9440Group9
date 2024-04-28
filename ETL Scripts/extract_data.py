import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from pathlib import Path
from azure.storage.blob import BlobServiceClient, BlobClient
from tqdm import tqdm
import io
import zipfile
import datacommons_pandas as dc
from gcp_functions import upload_dataframe_to_gcs


with open('config.json') as config_file:
    config = json.load(config_file)

YOUR_BUCKET_NAME = config["bucket_name"]
PROJECT_ID = config["project_id"]
api_key = config["api_census_key"]


## Extract data for levels fyi

# Initialize the connection to the Azure Blob Storage
connection_string = config["azure_connection_string"]
container_name = 'levelsfyidata'
folder_path = 'responses/20240221'

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Create a container client
container_client = blob_service_client.get_container_client(container_name)

# List all blobs in the specified folder
blobs_list = container_client.list_blobs(name_starts_with=folder_path)
first_blob = next(iter(blobs_list), None)

levels_fyi_df = pd.DataFrame()
for blob in tqdm(blobs_list):
    blob_client = container_client.get_blob_client(blob)
    blob_data = blob_client.download_blob().readall()
    json_data = json.loads(blob_data)
    blob_df = pd.json_normalize(json_data, record_path=['rows'])
    levels_fyi_df = pd.concat([levels_fyi_df, blob_df], ignore_index=True)

print()
#levels_fyi_df.to_csv("levels_fyi.csv", index=False)
print("Finished extracting and saving levels fyi data")
print()

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, levels_fyi_df, "levels_fyi", PROJECT_ID)




## Extract data for MIT living wages

# Path to the folder containing your JSON files
directory_path = Path("C:/Users/Mudas/Documents/school/Baruch/Data Warehouse/mitlivingwage")

dfs = []

for file_path in tqdm(directory_path.glob("*.json"), desc="Reading JSON files"):
    with open(file_path, 'r') as file:
        data = json.load(file)
    if 'annualSalaryList' in data:
        df = pd.DataFrame(data['annualSalaryList'])
        df['countyName'] = data['countyName']
        df['stateName'] = data['stateName']
        dfs.append(df)
    else:
        print(f"'annualSalaryList' key not found in {file_path.name}")

# Concatenate all DataFrames in the list into a single DataFrame
living_wage_df = pd.concat(dfs, ignore_index=True)
print()

#living_wage_df.to_csv("mit_living_wages.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, living_wage_df, "mit_living_wages", PROJECT_ID)

print("Finished extracting and saving MIT living wages data")
print()

## Extract data for minimum wages


url = "https://minimumwage.com/in-your-state/"
response = requests.get(url)
response.raise_for_status()  # This will raise an error if the request failed

soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('table')

headers = [header.text for header in table.find_all('th')]

rows = []
for row in table.find_all('tr'):
    columns = row.find_all('td')
    if columns:
        rows.append([column.text for column in columns])


clean_headers = [header.strip() for header in headers[:-1]]
clean_rows = [[col.strip() for col in row[:-1]] for row in rows]

minimum_wage_df = pd.DataFrame(clean_rows, columns=clean_headers)

#minimum_wage_df.to_csv("minimum_wage_per_state.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, minimum_wage_df, "minimum_wage_per_state", PROJECT_ID)

print("Finished extracting and saving minimum wage data")
print()


## extract start ups data

url = 'https://topstartups.io/startup-salary-equity-database/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

data_rows = []  # Initialize an empty list to hold the data

# Assuming the data is in a table, find the container (e.g., tbody, table)
container = soup.find('table')  # Modify this according to the actual structure

for row in tqdm(container.find_all('tr')):
    cells = row.find_all('td')
    if len(cells) >= 7:  # Assuming you need at least 7 cells for Job Title, Location, Date, and Salary
        job_title = cells[0].text.strip()
        location = cells[6].text.strip()
        date = cells[-1].text.strip()
        salary = cells[1].text.strip()
        num_employees = cells[10].text.strip()
        data_rows.append({'job_title': job_title, 'location': location, 'salary': salary ,"num_employees":num_employees,'date': date})

# Convert the list of dictionaries to a DataFrame
startups_df = pd.DataFrame(data_rows)



#startups_df.to_csv("startups.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, startups_df, "startups_jobs", PROJECT_ID)

print("Finished extracting and saving start ups data")
print()

## extract DMA data

# URL of the JSON data
url = 'https://gist.githubusercontent.com/curran/226a646101709048e3b006173a757ea7/raw/tv.json'

# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful (HTTP status code 200)
if response.status_code == 200:
    # Parse the JSON content of the response
    data = response.json()
    dma_df = pd.DataFrame(data).T
else:
    print(f'Failed to retrieve data: HTTP {response.status_code}')

#dma_df.to_csv("dma.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, dma_df, "dma_data", PROJECT_ID)

print("Finished extracting and saving DMA data")
print()

# Example API key placeholder
api_key = '47bd3da04feb2c5b78d1652c5d8db076d41328b5'

url = f"https://api.census.gov/data/2020/dec/pl?get=NAME,P1_001N&for=county:*&in=state:*&key={api_key}"

# Make a GET request to the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the response JSON
    data = response.json()

    # Convert to a DataFrame
    census_df = pd.DataFrame(data[1:], columns=data[0])

    # Construct the GEOID column
    census_df['county_geo_id'] = census_df['state'] + census_df['county']
else:
    print(f'Failed to retrieve data: {response.status_code}')

#census_df.to_csv("2020_census_data.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, census_df, "2020_census_data", PROJECT_ID)

print("Finished extracting and saving census data")

## get gazetter data to get cities rolled up to counties for census data
url = 'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_place_national.zip'

# Make a GET request to download the zip file
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Open the zip file in memory
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    # Extract the file name within the zip file
    file_name = zip_file.namelist()[0]
    # Read the extracted file into a pandas DataFrame
    gaz_df = pd.read_csv(zip_file.open(file_name), sep='\t', dtype=str)[["USPS","GEOID","ANSICODE","NAME"]]
    # Display the first few rows of the DataFrame
else:
    print(f'Failed to download file: {response.status_code}')

gaz_df["type_of_place"] = gaz_df["NAME"].apply(lambda i: i.split(" ")[-1])

remove_list = [" " + n for n in gaz_df["type_of_place"].unique()]
for n in gaz_df["type_of_place"].unique():
    gaz_df["NAME"] = gaz_df["NAME"].apply(lambda i: str(i).replace(n,""))

gaz_df['NAME'] = gaz_df['NAME'].str.strip()

cols_list = gaz_df.columns.tolist()

cols_list = [col.lower() for col in cols_list]

gaz_df.columns = cols_list

gaz_df = gaz_df.rename(columns={"usps":"state","geoid":"geo_id","ansicode":"ansi_code"})

# Prefixing GeoIDs with "geoId/"
# Prepare city GeoIDs list
geo_list = ["geoId/" + geo_id for geo_id in gaz_df['geo_id'].unique()]

# Initialize an empty dictionary for city_id to counties mapping
city_to_counties = {}


# Function to process each partition
def process_partition(geo_partition):
    contained_counties = dc.get_property_values(geo_partition, 'containedInPlace')
    county_geo_ids = set()
    for counties in contained_counties.values():
        county_geo_ids.update(counties)
    county_names = dc.get_property_values(list(county_geo_ids), 'name')

    # Update city_to_counties dictionary
    for city_id, counties in contained_counties.items():
        # Remove 'geoId/' prefix and update mapping
        clean_city_id = city_id.replace('geoId/', '')
        city_to_counties[clean_city_id] = [county_names.get(county_id, ['Unknown'])[0] for county_id in counties]


# Assuming you split the geo_list due to limitations, adjust the ranges as necessary
process_partition(geo_list[0:20000])  # Process the first partition
process_partition(geo_list[20000:])  # Process the second partition

# Convert the city_to_counties dictionary into a DataFrame
df_city_to_counties = pd.DataFrame(list(city_to_counties.items()), columns=['geo_id', 'county'])

df_atomic_counties = df_city_to_counties.explode('county')

# Remove any digits (which in this case are zip codes) and additional commas from the 'county' column
df_atomic_counties['county'] = df_atomic_counties['county'].str.replace(r'\, \d+', '', regex=True)

gaz_df = gaz_df.merge(df_atomic_counties, on='geo_id', how='left')

gaz_df = gaz_df[["state","county","name","type_of_place","geo_id","ansi_code"]]
pd.set_option('display.max_rows', None)

#gaz_df.to_csv("gazetteer_data.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, gaz_df, "gazetteer_data", PROJECT_ID)

print("Finished extracting and saving gazetteer data")

url = 'https://job-search-api.efinancialcareers.com/v1/efc/jobs/search'
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/json',
    'x-api-key': 'zvDFWwKGZ07cpXWV37lpO5MTEzXbHgyL4rKXb39C',
    'Origin': 'https://www.efinancialcareers.com',
    'Connection': 'keep-alive',
    'Referer': 'https://www.efinancialcareers.com/',
    'TE': 'trailers'
}

params = {
    'countryCode2': 'US',
    'radius': 50,
    'radiusUnit': 'mi',
    'page': 1,
    'pageSize': 900,
    'searchId': 'e6b131fe-967c-4014-9e23-c1afb0fc088d',
    'facets': 'locationPath|salaryRange|sectors|employmentType|experienceLevel|workArrangementType|salaryCurrency|minSalary|maxSalary|positionType|postedDate|clientBrandNameFilter',
    'currencyCode': 'USD',
    'culture': 'en',
    'recommendations': 'true',
    'fj': 'false',
    'includeRemote': 'true',
    'includeUnspecifiedSalary': 'true'
}

response = requests.get(url, headers=headers, params=params)
data = response.json()
#print(type(data))
#print(data.keys())


job_data = []

for item in data['data']:
    job_info = {
        'job_title': item['title'],
        'city': item['jobLocation']['city'],
        'state': item['jobLocation']['state'],
        'salary': item['salary'],
        'employmentType': item['employmentType'],
        #'date': item['expirationDate'],
        #'id': item['id'],
        #'detailsPageUrl': item['detailsPageUrl'],
        #'state': item['jobLocation']['state'],
        #'country': item['jobLocation']['country'],
        'date': item['postedDate'],
        #'workArrangementType': item.get('workArrangementType', None),
        #'isExternalApplication': item['isExternalApplication'],
        #'summary': item['summary'],
        #'description': item['description']
    }
    job_data.append(job_info)


efinancial_df = pd.DataFrame(job_data)
efinancial_df.to_csv("efinancial.csv", index=False)

## push data to GCS

upload_dataframe_to_gcs(YOUR_BUCKET_NAME, efinancial_df, "efinancial_jobs", PROJECT_ID)

print("Finished extracting and saving efinancial data")
print()
