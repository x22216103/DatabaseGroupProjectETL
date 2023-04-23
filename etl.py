import webbrowser
from dagster import job
from Extract import *
from Transform import *
from Load import *
@op(out=Out(bool))
def load_dimensions(stormim,censusim):
    return stormim and censusim


@job
def etl():
    load_dimensions(
        stormim=load_storm_dimension(
        stage_transformed_disasters(
        transform_extracted_disasters(
                stage_extracted_disasters(
                    extract_Incident()
            )))
        ),censusim=load_census_dimension(
        stage_transformed_census_data(
        transform_extracted_CensusData(
        stage_extracted_census(
        extract_census_data()
        )
        )
        )
        )
    )
                
            
        
    