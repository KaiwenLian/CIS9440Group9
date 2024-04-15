import numpy as np
import pandas as pd
from fuzzywuzzy import process
import re
from gcp_functions import upload_dataframe_to_gcs, read_csv_from_gcs, upload_table_to_bq
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import sys

## CREATING SCHEMA FOR DATA WAREHOUSE

# # Load configuration from config.json file
# with open('config.json') as config_file:
#     config = json.load(config_file)
#
# key_path = config['key_path']
#
# # Construct a BigQuery client object.
# credentials = service_account.Credentials.from_service_account_file(
#     key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
# )
#
# client = bigquery.Client(credentials=credentials, project=credentials.project_id)
#
# # Path to your SQL file
# sql_file_path = 'living_wages_dw.sql'
#
# # Read your SQL file
# with open(sql_file_path, 'r') as file:
#     sql_commands = file.read().split(';')  # Assuming your commands are separated by ';'
#
# # Execute each command from the SQL file
# for command in sql_commands:
#     if command.strip() == "":
#         continue  # Skipping empty commands
#     print(f"Executing command: {command}")
#     # Run a query job
#     query_job = client.query(command)
#     query_job.result()  # Wait for the job to complete
#
# print("Schema creation is complete.")

## GCP configuration

YOUR_BUCKET_NAME = 'staging-group9-dw'
PROJECT_ID = 'dw-group-project'

## clean and transform levels_fyi_data

state_abbreviations = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN",
    "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC", "Puerto Rico": "PR", "Remote":"Remote"
}

levels_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/levels_fyi_20240415170012.csv")

levels_df['offerDate'] = levels_df['offerDate'].str.slice(start=0, stop=24)
# Now, convert the 'offerDate' column to datetime without specifying a format
levels_df['offerDate'] = pd.to_datetime(levels_df['offerDate'])
# Format the datetime as 'YYYYMMDD' string and then convert to integer
levels_df['offerDate'] = levels_df['offerDate'].dt.strftime('%Y%m%d').astype(int)

## getting short hand for state

levels_df["state_short"] = levels_df.location.apply(lambda i: str(i).split(', ')[-1])
levels_df["state"] = levels_df["state_short"].map({abbrev: state for state, abbrev in state_abbreviations.items()})
levels_df["city"] = levels_df.location.apply(lambda i: str(i).split(', ')[0])

## only interested in levels in USA

levels_df = levels_df[levels_df.state_short.isnull() != True]
levels_df = levels_df[levels_df.state_short.isin(state_abbreviations.values())]

## replace the brackets and quotes in the tags column

levels_df["tags"] = levels_df["tags"].apply(lambda i: str(i).replace("[",""))\
.apply(lambda i: str(i).replace("]","")).apply(lambda i: str(i).replace("'",""))\
    .apply(lambda i: str(i).replace(",","")).apply(lambda i: str(i).replace("-",""))\
    .apply(lambda i: np.nan if i == 'nan' else i)

## dropping columns that are more than 50% missing

missing_percentages = levels_df.isnull().mean() * 100
columns_to_drop = missing_percentages[missing_percentages > 50].index

levels_df = levels_df.drop(columns=columns_to_drop)
levels_df = levels_df.drop(columns=["location","companyInfo.slug","exchangeRate","companyInfo.registered",
                                    "baseSalaryCurrency","countryId","cityId",'compPerspective',"totalCompensation"])


## rename column to snake case
def to_snake_case(column_name):
    # Replace dots with underscores
    column_name = column_name.replace('.', '_')
    # Insert underscores before capital letters and convert to lowercase
    return ''.join(['_' + i.lower() if i.isupper() else i for i in column_name]).lstrip('_')

# Apply the conversion function to each column name in the list
snake_case_columns = [to_snake_case(column) for column in levels_df.columns]

levels_df.columns = snake_case_columns



## Rearrange columns

levels_df = levels_df[['uuid','state','state_short', 'city', 'title', 'job_family', 'level', 'focus_tag',
       'years_of_experience', 'years_at_company', 'years_at_level',
       'offer_date', 'work_arrangement', 'dma_id', 'base_salary', 'company_info_icon', 'company_info_name']]

levels_df = levels_df.rename(columns = {"base_salary":"salary","uuid":"job_id"})
levels_df = levels_df.drop_duplicates(subset = ["job_id"])

## convert floats to int

for n in ["salary","dma_id"]:
    levels_df[n] = levels_df[n].astype(int)

#assert levels_df.company_info_name.isnull().sum() == 0, "company_info_name column has missing values"

def find_similar_names(name_to_check, all_names, threshold=90):
    similar_names = process.extract(name_to_check, all_names, limit=None)
    return [name for name, score in similar_names if score >= threshold]

unique_names = levels_df['company_info_name'].unique()

# for name in unique_names:
#     similar_names = find_similar_names(name, [n for n in unique_names if n != name])
#     if similar_names:
#         print(f"Names similar to '{name}': {similar_names}")
## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE

for n in ["years_at_company","years_at_level"]:
    levels_df[n] = levels_df[n].apply(lambda i: str(i).replace("0-1","0"))
    levels_df[n] = levels_df[n].apply(lambda i: str(i).replace("2-4", "3"))
    levels_df[n] = levels_df[n].apply(lambda i: str(i).replace("5-10", "7"))
    levels_df[n] = levels_df[n].apply(lambda i: str(i).replace("+",""))
    levels_df = levels_df[~levels_df[n].str.contains('-', na=False)]
    levels_df[n] = levels_df[n].astype(float)


print(levels_df.head())

## clean and transform mit living wages data

living_wage_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/mit_living_wages_20240415170017.csv")

living_wage_df = pd.read_csv("../mit_living_wages.csv")

living_wage_df.typicalAnnualSalary = living_wage_df.typicalAnnualSalary.apply(lambda i: str(i).replace("$","")).apply(lambda i: str(i).replace(",",""))
living_wage_df.typicalAnnualSalary = living_wage_df.typicalAnnualSalary.astype(int)



cols_list = ["occupational_area","annual_salary","location_name","state"]
living_wage_df.columns = cols_list

living_wage_df["occupational_area"] = living_wage_df["occupational_area"].apply(lambda i: i.replace('"',''))

living_wage_df["location_name"] = living_wage_df["location_name"].apply(lambda i: str(i).split(",")[0])

living_wage_df['state_short'] = living_wage_df['state'].map(state_abbreviations)
living_wage_df["location_name"] = living_wage_df["location_name"].apply(lambda i: i.replace(" County",""))
living_wage_df["location_name"] = living_wage_df["location_name"].apply(lambda i: i.replace(" city",""))
living_wage_df["location_name"] = living_wage_df["location_name"].apply(lambda i: i.replace(" Borough",""))
living_wage_df["location_name"] = living_wage_df["location_name"].apply(lambda i: i.replace("New York-Newark-Jersey City, NY","Newark"))
living_wage_df["location_name"] = living_wage_df["location_name"].apply(lambda i: i.split("-")[0])

living_wage_df = living_wage_df.rename(columns={"annual_salary":"salary" ,"location_name":"county"})
living_wage_df_newark = living_wage_df[living_wage_df["county"] == "Newark"]
living_wage_df_newark["county"] = "Jersey"
living_wage_df = pd.concat([living_wage_df, living_wage_df_newark])
living_wage_df = living_wage_df.sort_values(by = ["state","county"])

## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE

#print(living_wage_df.head())

category_map = {
    'Marketing Operations': 'Business & Financial Operations',
    'Software Engineer': 'Computer & Mathematical',
    'Mechanical Engineer': 'Architecture & Engineering',
    'Program Manager': 'Management',
    'Business Analyst': 'Business & Financial Operations',
    'Software Engineering Manager': 'Computer & Mathematical',
    'Recruiter': 'Business & Financial Operations',
    'Geological Engineer': 'Architecture & Engineering',
    'Accountant': 'Business & Financial Operations',
    'Project Manager': 'Management',
    'Business Development': 'Business & Financial Operations',
    'Technical Program Manager': 'Computer & Mathematical',
    'Product Designer': 'Arts, Design, Entertainment, Sports, & Media',
    'Financial Analyst': 'Business & Financial Operations',
    'Sales': 'Sales & Related',
    'Data Science Manager': 'Computer & Mathematical',
    'Human Resources': 'Business & Financial Operations',
    'Product Design Manager': 'Arts, Design, Entertainment, Sports, & Media',
    'Solution Architect': 'Computer & Mathematical',
    'Venture Capitalist': 'Business & Financial Operations',
    'Product Manager': 'Management',
    'Biomedical Engineer': 'Architecture & Engineering',
    'Administrative Assistant': 'Office & Administrative Support',
    'Technical Writer': 'Arts, Design, Entertainment, Sports, & Media',
    'Civil Engineer': 'Architecture & Engineering',
    'Chief of Staff': 'Management',
    'Management Consultant': 'Management',
    'Legal': 'Legal',
    'Hardware Engineer': 'Architecture & Engineering',
    'Copywriter': 'Arts, Design, Entertainment, Sports, & Media',
    'Marketing': 'Business & Financial Operations',
    'Customer Service': 'Office & Administrative Support',
    'Data Scientist': 'Computer & Mathematical',
    'Security Analyst': 'Computer & Mathematical',
    'Information Technologist': 'Computer & Mathematical',
    'Industrial Designer': 'Arts, Design, Entertainment, Sports, & Media',
    'Founder': 'Management',
    'Fashion Designer': 'Arts, Design, Entertainment, Sports, & Media',
    'Investment Banker': 'Business & Financial Operations'
}

levels_df['occupational_area'] = levels_df["job_family"].map(category_map)

living_wage_df = living_wage_df.rename(columns = {"salary":"mit_estimated_salary"})


## clean and transform minimum wage data

minimum_wage_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/minimum_wage_per_state_20240415170019.csv")

minimum_wage_df = pd.read_csv("../minimum_wage_per_state.csv")

cols_list = minimum_wage_df.columns.tolist()
cols_list = [col.replace(" ", "_").lower() for col in cols_list]
minimum_wage_df.columns = cols_list

for n in ["minimum_wage", "tipped_wage"]:

    minimum_wage_df[n] = minimum_wage_df[n].apply(lambda i: i.replace("$",""))
    minimum_wage_df[n] = minimum_wage_df[n].astype(float)


minimum_wage_df['state_short'] = minimum_wage_df['state'].map(state_abbreviations)

minimum_wage_df = minimum_wage_df[["state", "state_short", "minimum_wage", "tipped_wage"]]

## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE

print(minimum_wage_df.head())


## clean and transform start up jobs data

start_ups_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/startups_jobs_20240415170023.csv")


start_ups_df["num_employees"] = start_ups_df["num_employees"].apply(lambda i: str(i).split(" ")[0])\
    .apply(lambda i: str(i).split("-")[-1]).apply(lambda i: str(i).replace("+",""))
start_ups_df["num_employees"] = start_ups_df["num_employees"].astype(int)

start_ups_df["date"] = start_ups_df["date"].apply(lambda i: str(i).replace("/",""))
start_ups_df["date"] = start_ups_df["date"].astype(int)

start_ups_df['salary'] = start_ups_df['salary'].str.replace(r"\+.*", "", regex=True)
start_ups_df['salary'] = start_ups_df['salary'].str.replace("$","")
start_ups_df['salary'] = start_ups_df['salary'].str.replace(",","")
start_ups_df['salary'] = start_ups_df['salary'].astype(int)

location_mapping = {
    'San Francisco': 'CA',
    'San Francisco Bay Area': 'CA',
    'New York City': 'NY',
    'New York': 'NY',
    'NYC': 'NY',
    # Add more mappings as needed
}


def map_location_to_state(location, state_abbreviations):
    # Simplified mapping for cities to states, expand as needed
    city_to_state = {
        "San Francisco": "California", "San Francisco Bay Area": "California",
        "New York City": "New York", "Los Angeles": "California", "Seattle": "Washington",
        "Austin": "Texas", "Boston": "Massachusetts", "Chicago": "Illinois", "Dallas": "Texas",
        "Houston": "Texas", "Miami": "Florida", "San Diego": "California", "Washington DC": "District of Columbia",
        "Remote": "Remote", "Remote US": "Remote"
        # Add more mappings as necessary
    }
    # Handle generic 'Remote' cases or specific 'Remote US' cases by returning None or a specific value
    if 'UAE' in location or 'Estonia' in location or 'Remote Switzerland' in location:
        return None

    # Map city names to states, and then states to abbreviations
    if location in city_to_state:
        state_name = city_to_state[location]
        return state_abbreviations.get(state_name, None)

    # Directly map known state names to abbreviations
    return state_abbreviations.get(location, None)


start_ups_df['state_short'] = start_ups_df['location'].apply(lambda x: map_location_to_state(x, state_abbreviations))

# Drop rows with None in 'location' if needed

start_ups_df = start_ups_df[start_ups_df["state_short"].isnull() != True]

start_ups_df["location"] = start_ups_df["location"].replace(["San Francisco Bay Area","New York City","Remote US"],["San Francisco","New York","Remote"])

start_ups_df = start_ups_df.rename(columns = {"location":"city"})

start_ups_df = start_ups_df[["job_title","city","state_short","salary","num_employees","date"]]

## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE

print(start_ups_df.head())


## transform census data


census_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/2020_census_data_20240415170027.csv")


census_df["county"] = census_df["NAME"].apply(lambda i: i.split(",")[0])
census_df["county"] = census_df["county"].apply(lambda i:str(i).replace('"',''))
census_df["county"] = census_df["county"].apply(lambda i: i.replace(" County",""))
census_df["state"] = census_df["NAME"].apply(lambda i: i.split(", ")[1])
census_df["total_population"] = census_df["P1_001N"].astype(int)
census_df = census_df[["county_geo_id","county", "state", "total_population"]]
census_df["state_short"] = census_df["state"].map(state_abbreviations)
census_df["county"] = census_df["county"].apply(lambda i: i.replace(" Municipio",""))
census_df["county"] = census_df["county"].apply(lambda i: i.replace(" Municipality",""))
census_df["county"] = census_df["county"].apply(lambda i: i.replace(" City and Borough",""))
census_df["county"] = census_df["county"].apply(lambda i: i.replace(" city",""))
census_df["county_geo_id"] = census_df["county_geo_id"].astype(str).str.zfill(5)

census_df = census_df[["county_geo_id",'state','state_short','county','total_population']]

print(census_df.head(10))


## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE



print(census_df.head())

## transform gazetter data

gaz_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/gazetteer_data_20240415170051.csv")

gaz_df = pd.read_csv("gazetteer_data.csv", dtype=str)
gaz_df = gaz_df.rename(columns = {"state":"state_short"})
gaz_df["state"] = gaz_df["state_short"].map({abbrev: state for state, abbrev in state_abbreviations.items()})
ny_gaz_df = gaz_df[(gaz_df["county"] == "New York") & (gaz_df["name"]== "New York")]
gaz_df = gaz_df[~gaz_df.county.isin(state_abbreviations.keys())]
gaz_df = pd.concat([gaz_df, ny_gaz_df])
gaz_df = gaz_df.sort_values(by = ["state","county","name"])
gaz_df['county'] = gaz_df['county'].astype(str)
gaz_df = gaz_df[~gaz_df['county'].str.match(r'^\s*\d{5}\s*$')]
gaz_df = gaz_df[~gaz_df['county'].str.strip().str.isdigit()]
gaz_df = gaz_df[gaz_df['county'].str.lower() != 'nan']
gaz_df['county'] = gaz_df['county'].str.strip()
gaz_df["county"] = gaz_df["county"].apply(lambda i: i.replace(" County",""))

gaz_df = gaz_df[['state','state_short','county','name','type_of_place','geo_id','ansi_code']]
gaz_df = gaz_df.rename(columns = {"name":"location_name"})


## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE
print(gaz_df.head())
## transform DMA data

dma_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/dma_data_20240415170025.csv")

dma_df = pd.read_csv("dma.csv")



dma_df = dma_df.rename(columns = {"Designated Market Area (DMA)":"location_name","Rank":"rank","TV Homes":"tv_homes","% of US":"percent_of_united_states","DMA Code":"dma_id"})

dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.split(",")[0])
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.split(" (")[0])
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.split("(")[0])
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.split("-")[0])
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.split(" &")[0])
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.split("&")[0])
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace(" City",""))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Ft.","Fort"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Sacramnto","Sacramento"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("SantaBarbra","Santa Barbara"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Idaho Fals","Idaho Falls"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Honolulu","Honolulu"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Rochestr","Rochester"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Greenvll","Greenville"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("Wilkes Barre","Wilkes-Barre"))
dma_df["location_name"] = dma_df["location_name"].apply(lambda i: i.replace("St Joseph","St. Joseph"))

dma_df = dma_df.sort_values(by="rank")

## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE

print(dma_df.head())


## transform efinancial data

efinancial_df = read_csv_from_gcs(bucket_name=YOUR_BUCKET_NAME, blob_name="2024-04-15/efinancial_jobs_20240415170055.csv")

# Define a function to remove string values from salary
def extract_salary(salary_str):
    # Use regular expression to find numeric values
    match = re.search(r'\d[\d,]*\d', salary_str)
    if match:
        value = float(match.group().replace(',', ''))
        # Check if the value is less than 1000, implying it's in thousands
        if value < 1000:
            return value * 1000  # Convert to full amount in dollars
        else:
            return value
    else:
        return 0


efinancial_df['salary'] = efinancial_df['salary'].apply(extract_salary)
efinancial_df = efinancial_df.dropna(subset=['salary'])


# Define a function to extract year and month from date
def extract_year_month(date_str):
    match = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return year * 100 + month
    else:
        return None


efinancial_df['date'] = efinancial_df['date'].apply(extract_year_month)


def extract_state_abbreviation(state_str):
    state_name = str(state_str).split(', ')[-1]
    return state_abbreviations.get(state_name)


# Apply the function to the 'state' column to create a new column 'state_short'
efinancial_df['state_short'] = efinancial_df['state'].apply(extract_state_abbreviation)
df_job_data = efinancial_df[efinancial_df['state_short'].isin(state_abbreviations.values())]
df_job_data = df_job_data.drop(columns=['state'])
df_job_data.insert(loc=2, column='state_short', value=df_job_data.pop('state_short'))

df_job_data.job_title = df_job_data.job_title.apply(lambda i: i.replace('"',''))
df_job_data.job_title = df_job_data.job_title.apply(lambda i: i.replace("2025 ", ""))


df_job_data.to_csv("efinancial.csv", index=False)

## INSERT DATA INGESTION TO DATAWARE HOUSE CODE HERE
print(df_job_data)


## getting fact table

levels_facts_df = levels_df[["job_id", "dma_id", "state", "state_short","city","salary","years_of_experience","years_at_company","years_at_level", "occupational_area"]].copy()
minimum_facts_df = minimum_wage_df[["state","minimum_wage","tipped_wage"]].copy()
county_facts_df = census_df[["county_geo_id","state","state_short","county","total_population"]].copy()
county_facts_df = county_facts_df.merge(gaz_df[["county","state_short","location_name","type_of_place"]], on = ["county","state_short"], how = "left")
county_facts_df = county_facts_df.rename(columns = {"location_name":"city"})
print(county_facts_df.head())

living_wage_fact_df = living_wage_df.merge(gaz_df[["county","state_short","location_name","type_of_place"]], on = ["county","state_short"], how = "left")
levels_facts_df = levels_facts_df.merge(living_wage_fact_df[["occupational_area","location_name", "state_short","mit_estimated_salary"]]\
                                        .rename(columns={"location_name":"city"}), on = ["occupational_area","city","state_short"], how = "left")
levels_facts_df.drop_duplicates(subset = ["job_id"], inplace=True)
levels_facts_df = levels_facts_df.rename(columns = {"mit_estimated_salary":"mit_estimated_baseline_salary"})
## merge with dma_df
levels_facts_df = levels_facts_df.merge(dma_df[["dma_id","rank","tv_homes","percent_of_united_states"]], on = "dma_id", how = "left")
## merge with minimum_wage_df
levels_facts_df = levels_facts_df.merge(minimum_wage_df[["state","minimum_wage","tipped_wage"]], on = "state", how = "left")
## merge with county_facts_df
levels_facts_df = levels_facts_df.merge(county_facts_df[["state","county","city","county_geo_id","total_population"]], on = ["city","state"], how = "left")
## getting average total_population because of many counties rolling up to one city
mean_population_df = levels_facts_df.groupby("job_id")["total_population"].mean().reset_index()
levels_facts_df = levels_facts_df.drop(columns=["total_population"])
levels_facts_df = levels_facts_df.merge(mean_population_df, on = "job_id", how = "left")
levels_facts_df = levels_facts_df.rename(columns = {"total_population":"county_avg_total_population"})
levels_facts_df["county_avg_total_population"] = round(levels_facts_df["county_avg_total_population"],0)

levels_facts_df = levels_facts_df.drop(columns=["state","state_short","city","occupational_area", "county"])
levels_facts_df = levels_facts_df.drop_duplicates(subset = ["job_id"])

print(levels_facts_df.head(100))
print(levels_facts_df.info())

## get dma dimension

dim_dma_df = dma_df[["dma_id","location_name"]].copy()
print(dim_dma_df.head())

## get dim location dimension

dim_location_df = gaz_df[["geo_id","state","state_short","county","location_name","type_of_place"]].copy()
dim_location_df = dim_location_df.merge(census_df[["county_geo_id","state","county"]], on = ["county","state"], how = "left")
dim_location_df = dim_location_df.rename(columns = {"geo_id":"city_town_geo_id"})

print(dim_location_df.head())
print(dim_location_df.info())



