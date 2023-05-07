import webbrowser
from dagster import job
from Extract import *
from Transform import *
from Load import *
@op(out=Out(bool))
def load_dimensions(stormim,costmim,censusmim):
    return stormim and costmim and censusmim


@job
def etl():
    load_dimensions(
        stormim=load_storm_dimension(
        stage_transformed_disasters(
        transform_extracted_disasters(
                stage_extracted_disasters(
                    extract_Incident()
            )))
        )
        ,costmim=load_cost_dimension(
            stage_transformed_costs(
        transform_extracted_CostData(
                stage_extracted_costs(
                    extract_cost_data()
        ))
        )
        ),censusmim=load_census_dimension(
            stage_transformed_census_data(
                transform_extracted_CensusData(
                    stage_extracted_census(
                        extract_census_data()
        )
        )
        )
        )
    )
                
            
        
    