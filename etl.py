import webbrowser
from dagster import job
from Extract import *
from Transform import *
from Load import *
@op(out=Out(bool))
def load_dimensions(stormim,costmim):
    return stormim and costmim


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
                stage_extracted_costs(
                    extract_cost_data()
        )
        )
        ),
    )
                
            
        
    