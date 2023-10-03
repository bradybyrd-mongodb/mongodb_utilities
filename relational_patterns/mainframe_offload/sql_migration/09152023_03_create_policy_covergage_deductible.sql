CREATE SEQUENCE IF NOT EXISTS policy_coverage_deductible_id_seq START 1000000;
CREATE TABLE IF NOT EXISTS public.policy_coverage_deductible_conditions
(
    id integer NOT NULL DEFAULT nextval('policy_coverage_deductible_id_seq'::regclass),
    policy_coverage_id character varying(100) COLLATE pg_catalog."default",
    conditionName character varying(100) COLLATE pg_catalog."default",
    factor character varying(100) COLLATE pg_catalog."default",
    factorAmount real,
    CONSTRAINT policy_coverage_deductible_pkey PRIMARY KEY (id)
) ;