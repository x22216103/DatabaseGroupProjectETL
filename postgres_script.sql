DROP TABLE IF EXISTS public.cost_dimension;
CREATE TABLE cost_dimension(
	_id character varying(500),
	PopulationRank integer,
	HLocation character varying(50),
	USState character varying(50),
    HouseCost_00 FLOAT,
    HouseCost_01 FLOAT,
    HouseCost_02 FLOAT,
    HouseCost_03 FLOAT,
    HouseCost_04 FLOAT,
    HouseCost_05 FLOAT,
    HouseCost_06 FLOAT,
    HouseCost_07 FLOAT,
    HouseCost_08 FLOAT,
    HouseCost_09 FLOAT,
    HouseCost_10 FLOAT,
    HouseCost_11 FLOAT,
    HouseCost_12 FLOAT,
    HouseCost_13 FLOAT,
    HouseCost_14 FLOAT,
    HouseCost_15 FLOAT,
    HouseCost_16 FLOAT,
    HouseCost_17 FLOAT,
    HouseCost_18 FLOAT,
    HouseCost_19 FLOAT,
    HouseCost_20 FLOAT,
    HouseCost_21 FLOAT,
    HouseCost_22 FLOAT,
	CONSTRAINT pk_cost PRIMARY KEY (_id)
);
DROP TABLE IF EXISTS public.census_dimension;
CREATE TABLE public.census_dimension (
	_id character varying(5000),
	GeoFIPS character varying(5000) not null,
	SLocation character varying(5000),
	Main_Industry character varying(5000),
	wealthMeasurement character varying(5000),
    JobNumbers_01 character varying(25),
    JobNumbers_02 character varying(25),
    JobNumbers_03 character varying(25),
    JobNumbers_04 character varying(25),
    JobNumbers_05 character varying(25),
    JobNumbers_06 character varying(25),
    JobNumbers_07 character varying(25),
    JobNumbers_08 character varying(25),
    JobNumbers_09 character varying(25),
    JobNumbers_10 character varying(25),
    JobNumbers_11 character varying(25),
    JobNumbers_12 character varying(25),
    JobNumbers_13 character varying(25),
    JobNumbers_14 character varying(25),
    JobNumbers_15 character varying(25),
    JobNumbers_16 character varying(25),
    JobNumbers_17 character varying(25),
    JobNumbers_18 character varying(25),
    JobNumbers_19 character varying(25),
    JobNumbers_20 character varying(25),
    JobNumbers_21 character varying(25),
    JobNumbers_22 character varying(25),
	CONSTRAINT pk_census PRIMARY KEY (_id)
);
DROP FUNCTION IF EXISTS public.extract_month_name(date);
CREATE OR REPLACE FUNCTION public.extract_month_name(adate date)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    IMMUTABLE PARALLEL UNSAFE
AS $BODY$
SELECT to_char(adate,'Month');
$BODY$;

DROP FUNCTION IF EXISTS public.extract_day_name(date);
CREATE OR REPLACE FUNCTION public.extract_day_name(adate date)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    IMMUTABLE PARALLEL UNSAFE
AS $BODY$
SELECT to_char(adate,'Day');
$BODY$;


DROP TABLE IF EXISTS public.storm_dimension;
CREATE TABLE public.storm_dimension
(
    _id character varying(500),
    original_storm_id character(5),
    Incident_date date NOT NULL,
    Incident_end_date date,
    state_code character varying(2),
    Incident_Category character varying(100) NOT NULL,
    Incident_Description character varying(255) NOT NULL,
    FipsGeo character varying(500) NOT NULL,
    County_name character varying(100) NOT NULL,
    CONSTRAINT pk_storm PRIMARY KEY (_id)
);
ALTER TABLE IF EXISTS public.storm_dimension  OWNER to postgres;
ALTER TABLE IF EXISTS public.census_dimension  OWNER to postgres;
ALTER TABLE IF EXISTS public.cost_dimension  OWNER to postgres;
ALTER FUNCTION public.extract_day_name(date)  OWNER TO postgres;
ALTER FUNCTION public.extract_month_name(date) OWNER TO postgres;