from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd

TransformedFemaDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedFemaDataFrame",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True),
        PandasColumn.datetime_column("incident_date",  non_nullable=True),
        PandasColumn.string_column("state_code",  non_nullable=True),
        PandasColumn.string_column("incident_category",  non_nullable=True),
        PandasColumn.string_column("county_name",  non_nullable=True),
        PandasColumn.string_column("fipsgeo",  non_nullable=True)
    ],
)
TransformedCensusDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedCensusDataFrame",
    columns=[    
    PandasColumn.string_column("jobnumbers_01",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_02",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_03",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_04",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_05",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_06",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_07",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_08",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_09",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_10",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_11",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_12",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_13",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_14",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_15",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_16",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_17",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_18",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_19",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_20",  non_nullable=False),
    PandasColumn.string_column("jobnumbers_21",  non_nullable=False),
    PandasColumn.string_column("geofips", non_nullable=True),
    PandasColumn.string_column("slocation", non_nullable=True),
    PandasColumn.string_column("main_industry", non_nullable=False),
    PandasColumn.string_column("wealthmeasurement", non_nullable=False)
    ],
)
TransformedCostDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedCostDataFrame",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True),
        PandasColumn.datetime_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True)
    ],
)


@op(ins={'start':In(None)},out=Out(TransformedFemaDataFrame))
def transform_extracted_disasters(start) -> TransformedFemaDataFrame:
    disasters = pd.read_csv("staging/fema_disasters.csv", sep="\t")
    print(str(disasters["fips_state"]))
    disasters["_id"] = disasters["_id"]
    disasters["incident_category"] = disasters["incident_category"] + ":" + \
       disasters["incident_description"]
    disasters["incident_description"]=disasters["incident_description"]
    disasters["incident_date"] = pd.to_datetime(disasters["incident_date"]).dt.tz_localize(None)
    disasters["fipsgeo"]= str(disasters['fips_state'])+str(disasters['fips_county'])
    disasters['state_code']=disasters['state_code']
    disasters['county_name']= disasters["county_name"]+':'+disasters['state_code']
    disasters.drop(
        columns=["fips_state","fips_county"],
        axis=1,
        inplace=True
    )
    return disasters
@op(ins={'start':In(None)},out=Out(TransformedCensusDataFrame))
def transform_extracted_CensusData(start) -> TransformedCensusDataFrame:
    Census = pd.read_csv("staging/censusData.csv", sep="\t")
    Census["jobnumbers_01"] = Census["JNYear-2001"]
    Census["jobnumbers_02"] = Census["JNYear-2002"]
    Census["jobnumbers_03"] = Census["JNYear-2003"]
    Census["jobnumbers_04"] = Census["JNYear-2004"]
    Census["jobnumbers_05"] = Census["JNYear-2005"]
    Census["jobnumbers_06"] = Census["JNYear-2006"]
    Census["jobnumbers_07"] = Census["JNYear-2007"]
    Census["jobnumbers_08"] = Census["JNYear-2008"]
    Census["jobnumbers_09"] = Census["JNYear-2009"]
    Census["jobnumbers_10"] = Census["JNYear-2010"]
    Census["jobnumbers_11"] = Census["JNYear-2011"]
    Census["jobnumbers_12"] = Census["JNYear-2012"]
    Census["jobnumbers_13"] = Census["JNYear-2013"]
    Census["jobnumbers_14"] = Census["JNYear-2014"]
    Census["jobnumbers_15"] = Census["JNYear-2015"]
    Census["jobnumbers_16"] = Census["JNYear-2016"]
    Census["jobnumbers_17"] = Census["JNYear-2017"]
    Census["jobnumbers_18"] = Census["JNYear-2018"]
    Census["jobnumbers_19"] = Census["JNYear-2019"]
    Census["jobnumbers_20"] = Census["JNYear-2020"]
    Census["jobnumbers_21"] = Census["JNYear-2021"]
    Census["geofips"] = Census["geofips"]
    Census["slocation"]=str(Census["GeoName"])+" : "+str(Census["Region"])
    Census['main_industry']="Classification code: "+Census['IndustryClassification']+" Description of industry"+ Census["Description"]
    Census['wealthmeasurement']= Census["Unit"]
    Census.drop(
        columns=["GeoName","Region","IndustryClassification","Unit","Description","JNYear-2001", "JNYear-2002", "JNYear-2003", "JNYear-2004", "JNYear-2005", "JNYear-2006", "JNYear-2007", "JNYear-2008", "JNYear-2009", "JNYear-2010", "JNYear-2011", "JNYear-2012", "JNYear-2013", "JNYear-2014", "JNYear-2015", "JNYear-2016", "JNYear-2017", "JNYear-2018", "JNYear-2019", "JNYear-2020", "JNYear-2021"],
        axis=1,
        inplace=True
    )
    return Census
@op(ins={'start':In(None)},out=Out(TransformedCostDataFrame))
def transform_extracted_CostData(start) -> TransformedCostDataFrame:
    Cost = pd.read_csv("staging/CostData.csv", sep="\t")
    Cost["CostAVG_2000"] = round(Cost["HouseCost-00"],2)
    Cost["CostAVG_2001"] = round(Cost["HouseCost-01"],2)
    Cost["CostAVG_2002"] = round(Cost["HouseCost-02"],2)
    Cost["CostAVG_2003"] = round(Cost["HouseCost-03"],2)
    Cost["CostAVG_2004"] = round(Cost["HouseCost-04"],2)
    Cost["CostAVG_2005"] = round(Cost["HouseCost-05"],2)
    Cost["CostAVG_2006"] = round(Cost["HouseCost-06"],2)
    Cost["CostAVG_2007"] = round(Cost["HouseCost-07"],2)
    Cost["CostAVG_2008"] = round(Cost["HouseCost-08"],2)
    Cost["CostAVG_2009"] = round(Cost["HouseCost-09"],2)
    Cost["CostAVG_2010"] = round(Cost["HouseCost-10"],2)
    Cost["CostAVG_2011"] = round(Cost["HouseCost-11"],2)
    Cost["CostAVG_2012"] = round(Cost["HouseCost-12"],2)
    Cost["CostAVG_2013"] = round(Cost["HouseCost-13"],2)
    Cost["CostAVG_2014"] = round(Cost["HouseCost-14"],2)
    Cost["CostAVG_2015"] = round(Cost["HouseCost-15"],2)
    Cost["CostAVG_2016"] = round(Cost["HouseCost-16"],2)
    Cost["CostAVG_2017"] = round(Cost["HouseCost-17"],2)
    Cost["CostAVG_2018"] = round(Cost["HouseCost-18"],2)
    Cost["CostAVG_2019"] = round(Cost["HouseCost-19"],2)
    Cost["CostAVG_2020"] = round(Cost["HouseCost-20"],2)
    Cost["CostAVG_2021"] = round(Cost["HouseCost-21"],2)
    Cost["CostAVG_2022"] = round(Cost["HouseCost-22"],2)
    Cost["PopulationRank"] = Cost["Population_Rank"]
    Cost["HLocation"]=Cost["Region"]
    Cost['USState']= Cost["State"]
    return Cost

@op(ins={'disasters': In(TransformedFemaDataFrame)}, out=Out(None))
def stage_transformed_disasters(disasters):
    disasters.to_csv(
        "staging/transformed_disasters.csv",
        sep="\t",
        index=False
    )

@op(ins={'census': In(TransformedCensusDataFrame)}, out=Out(None))
def stage_transformed_census_data(census):
    census.to_csv(
        "staging/transformed_census.csv",
        sep="\t",
        index=False
    )
@op(ins={'cost': In(TransformedCensusDataFrame)}, out=Out(None))
def stage_transformed_costs(cost):
    cost.to_csv(
        "staging/transformed_costs.csv",
        sep="\t",
        index=False
    )