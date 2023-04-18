import requests
import math
import urllib.request
from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
from http.client import IncompleteRead
import json
def loadProperty(locdata):
    url = 'https://cost-of-living-and-prices.p.rapidapi.com/prices'
    headers = {
        "X-RapidAPI-Key": "433287968emsh1f75307f266f4cdp1f28b1jsne1c662e85d40",
        "X-RapidAPI-Host": "cost-of-living-and-prices.p.rapidapi.com",
        'Content-Type': 'application/json'
    }
    params = {"country_name": 'United States'}
    # make the API request and store the JSON response in MongoDB
    response = requests.get(url,headers=headers,params=params)
    if response.ok:
        data = response.json()
        x = locdata.insert_one(data)
        print('Data stored successfully in MongoDB!')
    else:
        print(f'Request failed with status code {response.status_code}.')

    print(data)
    return json
def FemaToAPI(database):
    try:
        #pull in the data fith a filter active url
        baseUrl = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$filter=incidentType%20eq%20%27Severe%20Storm%27&$inlinecount=allpages&$select=id&$top=1"
        #replace invalid commas in the string
        url = baseUrl.replace(" ","")
        #due to the nature of the data it is generally restricted to the amount of lines if can produce the following code allows it to run 
        #multiple pulls in order to extract all data in the api availible to us
        top = 1000
        skip = 0 
        webUrl = urllib.request.urlopen(url)
        result = webUrl.read()
        jsonData = json.loads(result.decode())
        recCount = jsonData['metadata']['count']
        loopNum = math.ceil(recCount / top)
        outFile = open("Fema.json", "w")
        i = 0
        while (i < loopNum):
            baseUrl = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$filter=incidentType%20eq%20%27Severe%20Storm%27&$inlinecount=allpages&$skip=" + str(skip)+"&$top=" + str(top)
            url = baseUrl.replace(" ","")
            webUrl = requests.get(url + "&$metadata=off&$format=jsona")
            result = webUrl.json()
            for entry in result:
                database.insert_one(entry)
            i+=1
            skip = i * top
            print("Iteration " + str(i) + " done")
        outFile.close()
    except IncompleteRead:
        pass

def loadCensus(collection):
    with open('./censusdata.json', 'r') as f:
        data = json.load(f)
    collection.insert_many(data)
    f.close()
def createMongo(client):
    db = client['JobRisk_DB']
    FemaCollection = db['Storm_Collection']
    PropertyCollection = db['Property_Collection']
    CensusCollection = db['Census_Collection']
    FemaToAPI(db.Storm_Collection)
    loadProperty(db.Property_Collection)
    loadCensus(db.Census_Collection)
    return db


def clean_json(x):
    "Create apply function for decoding JSON"
    return json.loads(x)

def storeInMongo(data, table):
    return 0



client= MongoClient("mongodb://%s:%s@127.0.0.1" %('dap','dap'))
print(client.list_database_names())
if 'JobRisk_DB' not in client.list_database_names():
    db = createMongo(client)
    print("hi")
else:
    db = client['JobRisk_DB']
    print(db.list_collection_names())







FemaDataFrame = create_dagster_pandas_dataframe_type(
    name="FemaDataFrame",
    columns=[
        PandasColumn.string_column("original_disaster_id",non_nullable=True, unique=True),
        PandasColumn.integer_column("disaster_number", non_nullable=True),
        PandasColumn.datetime_column("Incident_start_date",
            min_datetime=datetime(year=2003, month=1, day=1),
            max_datetime=datetime(year=2023, month=12, day=31)),
        PandasColumn.datetime_column("Incident_end_date",
            min_datetime=datetime(year=2003, month=1, day=1),
            max_datetime=datetime(year=2023, month=12, day=31)),
        PandasColumn.string_column("state_code", non_nullable=True),
        PandasColumn.string_column("Incident_Category", non_nullable=True),
        PandasColumn.string_column("Incident_Description", non_nullable=True),
        PandasColumn.string_column("fips_County", non_nullable=True),
        PandasColumn.string_column("fips_state", non_nullable=True),
        PandasColumn.string_column("County_Name", non_nullable=True)
    ],
)

def is_tuple(_, value):
    return isinstance(value, tuple) and all(
        isinstance(element, datetime) for element in value
    )

DateTuple = DagsterType(
    name="DateTuple",
    type_check_fn=is_tuple,
    description="A tuple of scalar values",
)
Fema_columns = {
    "_id": "_id",
    "disasterNumber":"disaster_number",
    "incidentBeginDate":"Incident_date",
    "state": "state_code",
    "incidentType":"Incident_Category",
    "declarationTitle": "Incident_Description",
    "fipsCountyCode": "fips_County",
    "fipsStateCode": "fips_state",
    "designatedArea": "County_Name"
}

@op(ins={'start': In(bool)}, out=Out(FemaDataFrame))
def extract_Incident(start,db) -> FemaDataFrame:
    db = db
    Fema = pd.DataFrame(db.FemaCollection.find({}))
    Fema.drop(
        columns=["femaDeclarationString", "declarationType","declarationDate","fyDeclared","ihProgramDeclared","iaProgramDeclared","paProgramDeclared","hmProgramDeclared","incidentEndDate","disasterCloseoutDate","placeCode","declarationRequestNumber","lastIAFilingDate","lastRefresh","hash","id"],
        axis=1,
        inplace=True
    )
    Fema.rename(
        columns=Fema_columns,
        inplace=True
    )
    db.close()
    return Fema

@op(ins={'Fema': In(FemaDataFrame)}, out=Out(None))
def stage_extracted_disasters(Fema):
    Fema.to_csv("staging/fema_disasters.csv",index=False,sep="\t")

PriceDataFrame = create_dagster_pandas_dataframe_type(
    name="PriceDataFrame",
    columns=[
        PandasColumn.string_column("original_disaster_id",non_nullable=True, unique=True),
        PandasColumn.integer_column("disaster_number", non_nullable=True),
        PandasColumn.datetime_column("Incident_date",
            min_datetime=datetime(year=2003, month=1, day=1),
            max_datetime=datetime(year=2023, month=12, day=31)),
        PandasColumn.string_column("state_code", non_nullable=True),
        PandasColumn.string_column("Incident_Category", non_nullable=True),
        PandasColumn.string_column("Incident_Description", non_nullable=True),
        PandasColumn.string_column("fips_County", non_nullable=True),
        PandasColumn.string_column("fips_state", non_nullable=True),
        PandasColumn.string_column("County_Name", non_nullable=True)
    ],
)

def is_tuple(_, value):
    return isinstance(value, tuple) and all(
        isinstance(element, datetime) for element in value
    )

DateTuple = DagsterType(
    name="DateTuple",
    type_check_fn=is_tuple,
    description="A tuple of scalar values",
)
Fema_columns = {
    "_id": "_id",
    "disasterNumber":"disaster_number",
    "incidentBeginDate":"Incident_date",
    "state": "state_code",
    "incidentType":"Incident_Category",
    "declarationTitle": "Incident_Description",
    "fipsCountyCode": "fips_County",
    "fipsStateCode": "fips_state",
    "designatedArea": "County_Name"
}

@op(ins={'start': In(bool)}, out=Out(FemaDataFrame))
def extract_Incident(start,db) -> FemaDataFrame:
    db = db
    Fema = pd.DataFrame(db.FemaCollection.find({}))
    Fema.drop(
        columns=["lat", "long","country_code","city_name"],
        axis=1,
        inplace=True
    )
    Fema.rename(
        columns=Fema_columns,
        inplace=True
    )
    db.close()
    return Fema

@op(ins={'Fema': In(FemaDataFrame)}, out=Out(None))
def stage_extracted_disasters(Fema):
    Fema.to_csv("staging/cost_of_living.csv",index=False,sep="\t")


