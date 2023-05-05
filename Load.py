from dagster import op, Out, In, get_dagster_logger
import psycopg2
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd



postgres_connection_string = "postgresql://postgres:postgres@127.0.0.1:5432/JobRisk"


@op(ins={'start': In(None)},out=Out(bool))
def load_storm_dimension(start):
    logger = get_dagster_logger()
    customers = pd.read_csv("staging/transformed_disasters.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("TRUNCATE storm_dimension;")
        rowcount = customers.to_sql(
            name="storm_dimension",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


@op(ins={'start': In(None)},out=Out(bool))
def load_census_dimension(start):
    logger = get_dagster_logger()
    census = pd.read_csv("staging/transformed_census.csv", sep="\t")

    try:
        conn = psycopg2.connect(postgres_connection_string)
        conn.set_session(autocommit=True)
        cursor = conn.cursor()

        # Truncate the table before loading data
        cursor.execute("TRUNCATE census_dimension;")
        
        # Load the data using the COPY command
        with open("staging/transformed_census.csv", "r") as f:
            next(f)  # skip the header row
            cursor.copy_from(f, "census_dimension", sep="\t", null="")
        
        # Get the row count of the inserted records
        cursor.execute("SELECT count(*) FROM census_dimension;")
        rowcount = cursor.fetchone()[0]
        logger.info("%i records loaded" % rowcount)

        # Clean up
        cursor.close()
        conn.close()
        
        return rowcount > 0
    except Exception as e:
        logger.error("Error: %s" % e)
        return False

@op(ins={'start': In(None)},out=Out(bool))
def load_cost_dimension(start):
    logger = get_dagster_logger()
    cost = pd.read_csv("staging/transformed_costs.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        engine.execute("TRUNCATE cost_dimension;")
        rowcount = cost.to_sql(
            name="cost_dimension",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False
