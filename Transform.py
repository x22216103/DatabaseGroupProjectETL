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
    PandasColumn.integer_column("_id", non_nullable=True),
    PandasColumn.integer_column("costavg_2001", non_nullable=True),
    PandasColumn.integer_column("costavg_2002", non_nullable=True),
    PandasColumn.integer_column("costavg_2003", non_nullable=True),
    PandasColumn.integer_column("costavg_2004", non_nullable=True),
    PandasColumn.integer_column("costavg_2005", non_nullable=True),
    PandasColumn.integer_column("costavg_2006", non_nullable=True),
    PandasColumn.integer_column("costavg_2007", non_nullable=True),
    PandasColumn.integer_column("costavg_2008", non_nullable=True),
    PandasColumn.integer_column("costavg_2009", non_nullable=True),
    PandasColumn.integer_column("costavg_2010", non_nullable=True),
    PandasColumn.integer_column("costavg_2011", non_nullable=True),
    PandasColumn.integer_column("costavg_2012", non_nullable=True),
    PandasColumn.integer_column("costavg_2013", non_nullable=True),
    PandasColumn.integer_column("costavg_2014", non_nullable=True),
    PandasColumn.integer_column("costavg_2015", non_nullable=True),
    PandasColumn.integer_column("costavg_2016", non_nullable=True),
    PandasColumn.integer_column("costavg_2017", non_nullable=True),
    PandasColumn.integer_column("costavg_2018", non_nullable=True),
    PandasColumn.integer_column("costavg_2019", non_nullable=True),
    PandasColumn.integer_column("costavg_2020", non_nullable=True),
    PandasColumn.integer_column("costavg_2021", non_nullable=True),
    PandasColumn.integer_column("costavg_2022", non_nullable=True),
    PandasColumn.integer_column("populationrank", non_nullable=True),
    PandasColumn.string_column("hlocation", non_nullable=True),
    PandasColumn.string_column("us_state", non_nullable=True)
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
    Cost["costavg_2001"] = Cost["HouseCost-01"]
    Cost["costavg_2002"] = Cost["HouseCost-02"]
    Cost["costavg_2003"] = Cost["HouseCost-03"]
    Cost["costavg_2004"] = Cost["HouseCost-04"]
    Cost["costavg_2005"] = Cost["HouseCost-05"]
    Cost["costavg_2006"] = Cost["HouseCost-06"]
    Cost["costavg_2007"] = Cost["HouseCost-07"]
    Cost["costavg_2008"] = Cost["HouseCost-08"]
    Cost["costavg_2009"] = Cost["HouseCost-09"]
    Cost["costavg_2010"] = Cost["HouseCost-10"]
    Cost["costavg_2011"] = Cost["HouseCost-11"]
    Cost["costavg_2012"] = Cost["HouseCost-12"]
    Cost["costavg_2013"] = Cost["HouseCost-13"]
    Cost["costavg_2014"] = Cost["HouseCost-14"]
    Cost["costavg_2015"] = Cost["HouseCost-15"]
    Cost["costavg_2016"] = Cost["HouseCost-16"]
    Cost["costavg_2017"] = Cost["HouseCost-17"]
    Cost["costavg_2018"] = Cost["HouseCost-18"]
    Cost["costavg_2019"] = Cost["HouseCost-19"]
    Cost["costavg_2020"] = Cost["HouseCost-20"]
    Cost["costavg_2021"] = Cost["HouseCost-21"]
    Cost["costavg_2022"] = Cost["HouseCost-22"]
    Cost["populationrank"] = Cost["Population_Rank"]
    Cost["hlocation"]=Cost["Region"]
    Cost['us_state']= Cost["State"]
    Cost.drop(
        columns=["RegionType","HouseCost-22","Population_Rank","Region","State","0","HouseCost-01", "HouseCost-02", "HouseCost-03", "HouseCost-04", "HouseCost-05", "HouseCost-06", "HouseCost-07", "HouseCost-08", "HouseCost-09", "HouseCost-10", "HouseCost-11", "HouseCost-12", "HouseCost-13", "HouseCost-14", "HouseCost-15", "HouseCost-16", "HouseCost-17", "HouseCost-18", "HouseCost-19", "HouseCost-20", "HouseCost-21"],
        axis=1,
        inplace=True
    )
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
@op(ins={'cost': In(TransformedCostDataFrame)}, out=Out(None))
def stage_transformed_costs(cost):
    cost.to_csv(
        "staging/transformed_costs.csv",
        sep="\t",
        index=False
    )