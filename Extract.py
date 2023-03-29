from pymongo import MongoClient
def loadZillow(url):
    return 0
def loadFema(url):
    return 0
def loadCensus(url):
    return 0
def createMongo():
    client= MongoClient("mongodb://%s:%s@127.0.0.1" %('dap','dap'))
    database = client['Property_Cost']
    FemaCollection = database['Storm_Collection']
    ZillowCollection = database['Property_Collection']
    CensusCollection = database['Census_Collection']
    mydict = { "name": "California", "population": "721999000" }
    mydict1 = { "property": "21b Baker street", "Owner": "Mrs Hudson" }
    mydict2 = { "storm": "Flood", "date": "yesterday" }
    x = CensusCollection.insert_one(mydict)
    x = ZillowCollection.insert_one(mydict1)
    x = FemaCollection.insert_one(mydict2)
    return database

def storeInMongo(data, table):
    return 0

db = createMongo()
print(db.list_collection_names())
