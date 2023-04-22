import requests
import math
import urllib.request
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from http.client import IncompleteRead
import json

def createMongo(client):
    database = client['JobRisk_Backup']
    FemaCollection = database['Storm_Collection']
    PropertyCollection = database['Property_Collection']
    CensusCollection = database['Census_Collection']
    return database



mongo_connection_string="mongodb://dap:dap@127.0.0.1"
client=MongoClient(mongo_connection_string)
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

if 'JobRisk_Backup' not in client.list_database_names():
    database = createMongo(client)
    FemaToAPI(database.FemaCollection)
else:
    database = client['JobRisk_Backup']
