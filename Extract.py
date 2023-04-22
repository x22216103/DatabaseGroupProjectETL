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
                database.insert_one(entry)
            i+=1
            skip = i * top
            print("Iteration " + str(i) + " done")
        outFile.close()
    except IncompleteRead:
        pass
def createMongo():
    client= MongoClient("mongodb://%s:%s@127.0.0.1" %('dap','dap'))
    database = client['JobRisk_Backup']
    FemaCollection = database['Storm_Collection']
    PropertyCollection = database['Property_Collection']
    CensusCollection = database['Census_Collection']
    return database

#db=createMongo()
mongo_connection_string="mongodb://dap:dap@127.0.0.1"
#FemaToAPI(db.FemaCollection)

FemaDataFrame = create_dagster_pandas_dataframe_type(
    name="FemaDataFrame",
    columns=[
        PandasColumn.string_column("_id",non_nullable=True, unique=True),
        PandasColumn.string_column("incident_date",
            non_nullable=True),
        PandasColumn.string_column("state_code", non_nullable=True),
        PandasColumn.string_column("incident_category", non_nullable=True),
        PandasColumn.string_column("incident_description", non_nullable=True),
        PandasColumn.string_column("fips_county", non_nullable=True),
        PandasColumn.string_column("fips_state", non_nullable=True),
        PandasColumn.string_column("county_name", non_nullable=True)
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
    "incidentBeginDate":"incident_date",
    "incidentEndDate":"incident_end_date",
    "state": "state_code",
    "incidentType":"incident_category",
    "declarationTitle": "incident_description",
    "fipsCountyCode": "fips_county",
    "fipsStateCode": "fips_state",
    "designatedArea": "county_name"
}

@op(ins={'start': In(bool)}, out=Out(FemaDataFrame))
def extract_Incident(start) -> FemaDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["JobRisk_Backup"]
    Fema = pd.DataFrame(db.FemaCollection.find())
    Fema.drop(
        columns=["disasterNumber","femaDeclarationString", "declarationType","declarationDate","fyDeclared","ihProgramDeclared","iaProgramDeclared","paProgramDeclared","hmProgramDeclared","incidentEndDate","disasterCloseoutDate","placeCode","declarationRequestNumber","lastIAFilingDate","lastRefresh","hash","id"],
        axis=1,
        inplace=True
    )
    Fema.rename(
        columns=Fema_columns,
        inplace=True
    )
    conn.close()
    return Fema

@op(ins={'Fema': In(FemaDataFrame)}, out=Out(None))
def stage_extracted_disasters(Fema):
    Fema.to_csv("staging/fema_disasters.csv",index=False,sep="\t")


