CREATE SEQUENCE IF NOT EXISTS policy_coverage_limit_id_seq START 1000000;
CREATE TABLE IF NOT EXISTS public.policy_coverage_limit
(
    id integer NOT NULL DEFAULT nextval('policy_coverage_deductible_id_seq'::regclass),
    policy_coverage_id character varying(100) COLLATE pg_catalog."default",
    limitTypeCode character varying(100) COLLATE pg_catalog."default",
    limitValue real,
    limitBasisCode character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT policy_coverage_limit_pkey PRIMARY KEY (id)
);
