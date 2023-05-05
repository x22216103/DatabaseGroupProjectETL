import requests
import math
import urllib.request
import numpy as np
from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
from http.client import IncompleteRead
import json





mongo_connection_string="mongodb://dap:dap@127.0.0.1"


FemaDataFrame = create_dagster_pandas_dataframe_type(
    name="FemaDataFrame",
    columns=[
        PandasColumn.string_column("_id",
            non_nullable=True),
        PandasColumn.integer_column("disasternumber",non_nullable=True, unique=False),
        PandasColumn.string_column("fema_string",
            non_nullable=True),
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
    "femaDeclarationString":"fema_string",
    "_id":"_id",
    "disasterNumber": "disasternumber",
    "incidentBeginDate":"incident_date",
    "state": "state_code",
    "incidentType":"incident_category",
    "declarationTitle": "incident_description",
    "fipsCountyCode": "fips_county",
    "fipsStateCode": "fips_state",
    "designatedArea": "county_name"
}

@op(ins={'start': In(bool)}, out=Out(FemaDataFrame))
def extract_Incident(start) -> FemaDataFrame:
    try:
        conn = MongoClient(mongo_connection_string)
        db = conn["JobRisk_Backup"]
        Fema = pd.DataFrame(db.FemaCollection.find())
        Fema.drop(
            columns=["incidentEndDate", "declarationType","declarationDate","fyDeclared","ihProgramDeclared","iaProgramDeclared","paProgramDeclared","hmProgramDeclared","disasterCloseoutDate","placeCode","declarationRequestNumber","lastIAFilingDate","lastRefresh","hash","id"],
            axis=1,
            inplace=True
        )
        Fema.rename(
            columns=Fema_columns,
            inplace=True
        )
        conn.close()
        return Fema
    except Exception as e:
        print(f"Error: {e}")

@op(ins={'Fema': In(FemaDataFrame)}, out=Out(None))
def stage_extracted_disasters(Fema):
    try:
        Fema.to_csv("staging/fema_disasters.csv", index=False, sep="\t")
    except Exception as e:
        print(f"Error: {e}")
CensusDataFrame = create_dagster_pandas_dataframe_type(
    name="CensusDataFrame",
    columns=[
        PandasColumn.string_column("_id",non_nullable=False, unique=True),
        PandasColumn.string_column("JNYear-2001",
            non_nullable=False),
        PandasColumn.string_column("JNYear-2002", non_nullable=False),
        PandasColumn.string_column("JNYear-2003", non_nullable=False),
        PandasColumn.string_column("JNYear-2004", non_nullable=False),
        PandasColumn.string_column("JNYear-2005", non_nullable=False),
        PandasColumn.string_column("JNYear-2006", non_nullable=False),
        PandasColumn.string_column("JNYear-2007", non_nullable=False),
        PandasColumn.string_column("JNYear-2008",
            non_nullable=False),
        PandasColumn.string_column("JNYear-2009", non_nullable=False),
        PandasColumn.string_column("JNYear-2010", non_nullable=False),
        PandasColumn.string_column("JNYear-2011", non_nullable=False),
        PandasColumn.string_column("JNYear-2012", non_nullable=False),
        PandasColumn.string_column("JNYear-2012", non_nullable=False),
        PandasColumn.string_column("JNYear-2013", non_nullable=False),
        PandasColumn.string_column("JNYear-2014",
            non_nullable=False),
        PandasColumn.string_column("JNYear-2015", non_nullable=False),
        PandasColumn.string_column("JNYear-2016", non_nullable=False),
        PandasColumn.string_column("JNYear-2017", non_nullable=False),
        PandasColumn.string_column("JNYear-2018", non_nullable=False),
        PandasColumn.string_column("JNYear-2019", non_nullable=False),
        PandasColumn.string_column("JNYear-2020", non_nullable=False),
        PandasColumn.string_column("JNYear-2021",
            non_nullable=False),
        PandasColumn.string_column("geofips", non_nullable=False),
        PandasColumn.string_column("GeoName", non_nullable=False),
        PandasColumn.string_column("Region", non_nullable=False),
        PandasColumn.string_column("IndustryClassification", non_nullable=False),
        PandasColumn.string_column("Description", non_nullable=False),
        PandasColumn.string_column("Unit", non_nullable=False)
    ],
)






Census_columns = {
    "2001":"JNYear-2001",
    "2002":"JNYear-2002",
    "2003":"JNYear-2003",
    "2004":"JNYear-2004",
    "2005":"JNYear-2005",
    "2006":"JNYear-2006",
    "2007":"JNYear-2007",
    "2008":"JNYear-2008",
    "2009":"JNYear-2009",
    "2010":"JNYear-2010",
    "2011":"JNYear-2011",
    "2012":"JNYear-2012",
    "2013":"JNYear-2013",
    "2014":"JNYear-2014",
    "2015":"JNYear-2015",
    "2016":"JNYear-2016",
    "2017":"JNYear-2017",
    "2018":"JNYear-2018",
    "2019":"JNYear-2019",
    "2020":"JNYear-2020",
    "2021":"JNYear-2021",
    'GeoFIPS': 'geofips',
    'GeoName': 'GeoName',
    'Region': 'Region',
    'IndustryClassification': 'IndustryClassification',
    'Description': 'Description',
    'Unit': 'Unit'
}

@op(ins={'start': In(bool)}, out=Out(CensusDataFrame))
def extract_census_data(start) -> CensusDataFrame:
    try:
        conn = MongoClient(mongo_connection_string)
        db = conn["JobRisk_Backup"]
        Census = pd.DataFrame(db.CensusCollection.find())
        Census.drop(
            columns=['LineCode','TableName'],
            axis=1,
            inplace=True
        )
        Census.rename(
            columns=Census_columns,
            inplace=True
        )
        conn.close()
        return Census
    except Exception as e:
        print(f"Error: {e}")

@op(ins={'Census': In(CensusDataFrame)}, out=Out(None))
def stage_extracted_census(Census):
    try:
        Census.to_csv("staging/censusData.csv",index=False,sep="\t")
    except Exception as e:
        print(f"Error: {e}")



CostDataFrame = create_dagster_pandas_dataframe_type(
    name="CostDataFrame",
    columns=[
        PandasColumn.integer_column("HouseCost-01", non_nullable=False),
        PandasColumn.integer_column("HouseCost-02", non_nullable=False),
        PandasColumn.integer_column("HouseCost-03", non_nullable=False),
        PandasColumn.integer_column("HouseCost-04", non_nullable=False),
        PandasColumn.integer_column("HouseCost-05", non_nullable=False),
        PandasColumn.integer_column("HouseCost-06", non_nullable=False),
        PandasColumn.integer_column("HouseCost-07", non_nullable=False),
        PandasColumn.integer_column("HouseCost-08", non_nullable=False),
        PandasColumn.integer_column("HouseCost-09", non_nullable=False),
        PandasColumn.integer_column("HouseCost-10", non_nullable=False),
        PandasColumn.integer_column("HouseCost-11", non_nullable=False),
        PandasColumn.integer_column("HouseCost-12", non_nullable=False),
        PandasColumn.integer_column("HouseCost-13", non_nullable=False),
        PandasColumn.integer_column("HouseCost-14", non_nullable=False),
        PandasColumn.integer_column("HouseCost-15", non_nullable=False),
        PandasColumn.integer_column("HouseCost-16", non_nullable=False),
        PandasColumn.integer_column("HouseCost-17", non_nullable=False),
        PandasColumn.integer_column("HouseCost-18", non_nullable=False),
        PandasColumn.integer_column("HouseCost-19", non_nullable=False),
        PandasColumn.integer_column("HouseCost-20", non_nullable=False),
        PandasColumn.integer_column("HouseCost-21", non_nullable=False),
        PandasColumn.integer_column("HouseCost-22", non_nullable=False),
        PandasColumn.integer_column("Population_Rank", non_nullable=False),
        PandasColumn.string_column("Region", non_nullable=True),
        PandasColumn.string_column("State", non_nullable=False),
    ],
)


Cost_columns = {
    "RegionID":"_id",
    'SizeRank': 'Population_Rank',
    'RegionName': 'Region',
    'StateName': 'State',
    "1":"HouseCost-01",
    "2":"HouseCost-02",
    "3":"HouseCost-03",
    "4":"HouseCost-04",
    "5":"HouseCost-05",
    "6":"HouseCost-06",
    "7":"HouseCost-07",
    "8":"HouseCost-08",
    "9":"HouseCost-09",
    "10":"HouseCost-10",
    "11":"HouseCost-11",
    "12":"HouseCost-12",
    "13":"HouseCost-13",
    "14":"HouseCost-14",
    "15":"HouseCost-15",
    "16":"HouseCost-16",
    "17":"HouseCost-17",
    "18":"HouseCost-18",
    "19":"HouseCost-19",
    "20":"HouseCost-20",
    "21":"HouseCost-21",
    "22":"HouseCost-22",
}

@op(ins={'start': In(bool)}, out=Out(CostDataFrame))
def extract_cost_data(start) -> CostDataFrame:
    try:
        conn = MongoClient(mongo_connection_string)
        db = conn["JobRisk_Backup"]
        Cost = pd.DataFrame(db.PropertyCollection.find())
        Cost.drop(
            columns=['_id'],
            axis=1,
            inplace=True
        )
        Cost.rename(
            columns=Cost_columns,
            inplace=True
        )
        Cost.fillna(0, inplace=True)

        # replace infinity with a large finite number
        Cost.replace([np.inf, -np.inf], 1e15, inplace=True)
        cols_to_convert = Cost.filter(regex='^HouseCost-', axis=1).columns
        Cost[cols_to_convert] = Cost[cols_to_convert].astype(int)
    except Exception as e:
        print(f"An error occurred while extracting cost data: {e}")
        Cost = pd.DataFrame()
    finally:
        conn.close()
    return Cost






@op(ins={'Cost': In(CostDataFrame)}, out=Out(None))
def stage_extracted_costs(Cost):
    try:
        Cost.to_csv("staging/CostData.csv",index=False,sep="\t")
    except Exception as e:
        print(f"Error occurred while writing Cost data to file: {e}")



