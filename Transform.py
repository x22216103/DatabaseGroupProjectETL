from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd

TransformedFemaDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedFemaDataFrame",
    columns=[
        PandasColumn.string_column("original_disaster_id", min_value=0),
        PandasColumn.integer_column("disaster_number", non_nullable=True),
        PandasColumn.datetime_column("Incident_date",  non_nullable=True),
        PandasColumn.string_column("state_code",  non_nullable=True),
        PandasColumn.string_column("Incident_Category",  non_nullable=True),
        PandasColumn.string_column("County",  non_nullable=True),
        PandasColumn.integer_column("Fips_location",  non_nullable=True)
    ],
)


@op(ins={'start':In(None)},out=Out(TransformedFemaDataFrame))
def transform_extracted_disasters(start) -> TransformedFemaDataFrame:
    disasters = pd.read_csv("staging/fema_disasters.csv", sep="\t")
    disasters["Storm_details"] = disasters["Incident_Category"] + ":" + \
       disasters["Incident_Description"]
    disasters["Incident_date"] = pd.to_datetime(disasters["Incident_date"])
    disasters["GeoLocationFips"]= disasters['fips_state']+disasters['fips_County']
    disasters['County']= disasters["County"]+':'+disasters['state_code']
    disasters.drop(
        columns=["Incident_Category","Incident_Description","County","state_code","fips_state","fips_County"],
        axis=1,
        inplace=True
    )
    return disasters

@op(ins={'disasters': In(TransformedFemaDataFrame)}, out=Out(None))
def stage_transformed_disasters(disasters):
    disasters.to_csv(
        "staging/transformed_disasters.csv",
        sep="\t",
        index=False
    )