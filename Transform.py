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
        PandasColumn.string_column("_id", non_nullable=True),
        PandasColumn.datetime_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True),
        PandasColumn.string_column("",  non_nullable=True)
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
    print(str(Census[""]))
    Census[""] = Census[""]
    Census[""] = Census[""]
    Census[""]=Census[""]
    Census[""] = pd.to_datetime([""]).dt.tz_localize(None)
    Census[""]= str(Census[''])
    Census['']=Census['']
    Census['']= Census[""]
    Census.drop(
        columns=[],
        axis=1,
        inplace=True
    )
    return Census
@op(ins={'start':In(None)},out=Out(TransformedCensusDataFrame))
def transform_extracted_CostData(start) -> TransformedCostDataFrame:
    Cost = pd.read_csv("staging/CostData.csv", sep="\t")
    print(str(Cost[""]))
    Cost[""] = Cost[""]
    Cost[""] = Cost[""]
    Cost[""]=Cost[""]
    Cost[""] = pd.to_datetime(Cost[""]).dt.tz_localize(None)
    Cost[""]= str(Cost[''])
    Cost['']=Cost['']
    Cost['']= Cost[""]
    Cost.drop(
        columns=[],
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
@op(ins={'census': In(TransformedCensusDataFrame)}, out=Out(None))
def stage_transformed_costs(cost):
    cost.to_csv(
        "staging/transformed_costs.csv",
        sep="\t",
        index=False
    )