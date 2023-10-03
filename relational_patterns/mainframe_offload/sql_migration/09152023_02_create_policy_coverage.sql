CREATE SEQUENCE IF NOT EXISTS policy_coverage_id_seq START 1000000;
CREATE TABLE IF NOT EXISTS public.policy_coverage
(
    id integer NOT NULL DEFAULT nextval('policy_coverage_id_seq'::regclass),
    policy_id character varying(100) COLLATE pg_catalog."default",
    product_coverage_id character varying(100) COLLATE pg_catalog."default",
    name character varying(100) COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    startDate timestamp without time zone,
    endDate timestamp without time zone,
    isactive character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT policy_coverage_pkey PRIMARY KEY (id)
) ;