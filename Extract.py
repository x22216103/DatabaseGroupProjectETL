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
    url = "https://realtor.p.rapidapi.com/properties/v3/list"
    payload = {
        "limit": 1,
        "offset": 0,
        "status": ["for_sale"],
        "city":locdata,
        "sort": {
            "direction": "desc",
            "field": "list_date"
        }
    }
    headers = {
        "content-type": "application/json",
        "X-RapidAPI-Key": "0a35cf9ab0msh2166684d6e08d6cp18f35ajsnc9c8142f43bf",
        "X-RapidAPI-Host": "realtor.p.rapidapi.com"
    }
    response = requests.request("POST", url, json=payload, headers=headers)
    json = response.json()
    return json
def FemaToAPI(database):
    try:
        baseUrl = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$filter=incidentType%20eq%20%27Severe%20Storm%27&$inlinecount=allpages&$select=id&$top=1"
        url = baseUrl.replace(" ","")
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
                print(entry)
                print('\n')
                database.insert_one(entry)
            i+=1
            skip = i * top
            print("Iteration " + str(i) + " done")
        outFile.close()
    except IncompleteRead:
        pass

def loadCensus(url):
    return 0
def createMongo():
    client= MongoClient("mongodb://%s:%s@127.0.0.1" %('dap','dap'))
    database = client['Property_Cost']
    FemaCollection = database['Storm_Collection']
    PropertyCollection = database['Property_Collection']
    CensusCollection = database['Census_Collection']


def clean_json(x):
    "Create apply function for decoding JSON"
    return json.loads(x)

def storeInMongo(data, table):
    return 0



db=createMongo()
mongo_connection_string="mongodb://dap:dap@127.0.0.1"
FemaToAPI(db.FemaCollection)

FemaDataFrame = create_dagster_pandas_dataframe_type(
    name="FemaDataFrame",
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
    conn = MongoClient(mongo_connection_string)
    db = conn["Property_Cost"]
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


