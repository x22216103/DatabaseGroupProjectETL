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





mongo_connection_string="mongodb://dap:dap@127.0.0.1"


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





CensusDataFrame = create_dagster_pandas_dataframe_type(
    name="CensusDataFrame",
    columns=[
        PandasColumn.string_column("_id",non_nullable=True, unique=True),
        PandasColumn.string_column("",
            non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True)
    ],
)


Census_columns = {
    "_id": "",
    "":"",
    "":"",
    "": "",
    "":"",
    "": "",
    "": "",
    "": "",
    "": ""
}

@op(ins={'start': In(bool)}, out=Out(CensusDataFrame))
def extract_census_data(start) -> CensusDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["JobRisk_Backup"]
    Census = pd.DataFrame(db.CensusCollection.find())
    Census.drop(
        columns=[],
        axis=1,
        inplace=True
    )
    Census.rename(
        columns=Census_columns,
        inplace=True
    )
    conn.close()
    return Census

@op(ins={'Census': In(CensusDataFrame)}, out=Out(None))
def stage_extracted_disasters(Census):
    Census.to_csv("staging/censusData.csv",index=False,sep="\t")



CostDataFrame = create_dagster_pandas_dataframe_type(
    name="CostDataFrame",
    columns=[
        PandasColumn.string_column("_id",non_nullable=True, unique=True),
        PandasColumn.string_column("",
            non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True),
        PandasColumn.string_column("", non_nullable=True)
    ],
)


Cost_columns = {
    "_id": "",
    "":"",
    "":"",
    "state": "",
    "":"",
    "": "",
    "": "",
    "": "",
    "": ""
}

@op(ins={'start': In(bool)}, out=Out(CostDataFrame))
def extract_cost_data(start) -> CostDataFrame:
    conn = MongoClient(mongo_connection_string)
    db = conn["JobRisk_Backup"]
    Cost = pd.DataFrame(db.CostCollection.find())
    Cost.drop(
        columns=[],
        axis=1,
        inplace=True
    )
    Cost.rename(
        columns=Cost_columns,
        inplace=True
    )
    conn.close()
    return Cost

@op(ins={'Cost': In(CostDataFrame)}, out=Out(None))
def stage_extracted_disasters(Cost):
    Cost.to_csv("staging/CostData.csv",index=False,sep="\t")


