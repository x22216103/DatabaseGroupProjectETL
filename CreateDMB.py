import requests
import math
import csv
import urllib.request
from pymongo.errors import *
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from http.client import IncompleteRead
from requests.exceptions import ConnectionError, ReadTimeout, TooManyRedirects, RequestException

import json

def createMongo(client):
    try:
        database = client['JobRisk_Backup']
        FemaCollection = database['Storm_Collection']
        PropertyCollection = database['Cost_of_Property_Collection']
        CensusCollection = database['Census_Collection']
        print("Connected to MongoDB successfully.")
        return database
    except PyMongoError as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None

def CensusFileRead(db):
    try:
        with open('./censusdata.json', 'r') as f:
            data = json.load(f)
        for entry in data:
            db.insert_one(entry)
        print("Data inserted successfully.")
    except FileNotFoundError:
        print("Error: File not found.")
    except ValueError as e:
        print(f"Error loading JSON data: {str(e)}")
    except PyMongoError as e:
        print(f"Error inserting data into MongoDB: {str(e)}")


def costFilePull(db):
    try:
        df = pd.read_csv('AVGHousePrice.csv', delimiter='\t')
        db.insert_many(df.to_dict('records'))
        print("Data inserted successfully.")
    except FileNotFoundError:
        print("Error: File not found.")
    except PyMongoError as e:
        print(f"Error inserting data into MongoDB: {str(e)}")
   


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
        i = 0
        while (i < loopNum):
            try:
                baseUrl = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$filter=incidentType%20eq%20%27Severe%20Storm%27&$inlinecount=allpages&$skip=" + str(skip)+"&$top=" + str(top)
                url = baseUrl.replace(" ","")
                webUrl = requests.get(url + "&$metadata=off&$format=jsona", timeout=10)
                webUrl.raise_for_status() # check for HTTP errors
                result = webUrl.json()
                for entry in result:
                    database.insert_one(entry)
                i += 1
                skip = i * top
                print("Iteration " + str(i) + " done")
            except (ConnectionError, ReadTimeout, TooManyRedirects, RequestException) as e:
                print(f"Error fetching data from API: {str(e)}")
                break
        print("Data inserted successfully.")
    except (urllib.error.URLError, json.JSONDecodeError, PyMongoError) as e:
        print(f"Error: {str(e)}")



mongo_connection_string = "mongodb://dap:dap@127.0.0.1"

try:
    client = MongoClient(mongo_connection_string)

    if 'JobRisk_Backup' in client.list_database_names():
        database = createMongo(client)
        FemaToAPI(database.FemaCollection)
        CensusFileRead(database.CensusCollection)
        costFilePull(database.PropertyCollection)
    else:
        database = client['JobRisk_Backup']
        print(" success")

except Exception as e:
    print(f"Error: {e}")
    # Handle the exception here.

    
