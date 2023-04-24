DROP TABLE IF EXISTS public.cost_dimension;
CREATE TABLE cost_dimension(
	_id character varying(500),
	PopulationRank integer,
	HLocation character varying(50),
	USState character varying(50),
    costavg_2000 FLOAT,
    costavg_2001 FLOAT,
    costavg_2002 FLOAT,
    costavg_2003 FLOAT,
    costavg_2004 FLOAT,
    costavg_2005 FLOAT,
    costavg_2006 FLOAT,
    costavg_2007 FLOAT,
    costavg_2008 FLOAT,
    costavg_2009 FLOAT,
    costavg_2010 FLOAT,
    costavg_2011 FLOAT,
    costavg_2012 FLOAT,
    costavg_2013 FLOAT,
    costavg_2014 FLOAT,
    costavg_2015 FLOAT,
    costavg_2016 FLOAT,
    costavg_2017 FLOAT,
    costavg_2018 FLOAT,
    costavg_2019 FLOAT,
    costavg_2020 FLOAT,
    costavg_2021 FLOAT,
    costavg_2022 FLOAT,
	CONSTRAINT pk_cost PRIMARY KEY (_id)
);
DROP TABLE IF EXISTS public.census_dimension;
CREATE TABLE public.census_dimension (
	_id character varying(5000),
	GeoFIPS character varying(5000) not null,
	SLocation character varying(5000),
	Main_Industry character varying(5000),
	wealthMeasurement character varying(5000),
    JobNumbers_00 character varying(25),
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