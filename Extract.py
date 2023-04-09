import requests
import math
import urllib.request
from datetime import datetime
from http.client import IncompleteRead
import json
import pandas as pd
from pymongo import MongoClient
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
    return database

def clean_json(x):
    "Create apply function for decoding JSON"
    return json.loads(x)

def storeInMongo(data, table):
    return 0
db=createMongo()
FemaToAPI(db.FemaCollection)
#print(db.FemaCollection.find())