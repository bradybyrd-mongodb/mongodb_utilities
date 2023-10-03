CREATE SEQUENCE IF NOT EXISTS policy_coverage_physical_location_id_seq START 1000000;
CREATE TABLE IF NOT EXISTS public.policy_coverage_physical_location
(
    id integer NOT NULL DEFAULT nextval('policy_coverage_physical_location_id_seq'::regclass),
    policy_coverage_id character varying(100) COLLATE pg_catalog."default",
    name character varying(100) COLLATE pg_catalog."default",
    longitude real,
    latitude real,
    CONSTRAINT policy_coverage_location_pkey PRIMARY KEY (id)
);