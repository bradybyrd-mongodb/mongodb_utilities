--  DDL :

CREATE SEQUENCE IF NOT EXISTS product_version_id_seq START 1000000;
CREATE TABLE IF NOT EXISTS public.product_version
(
    id integer NOT NULL DEFAULT nextval('product_version_id_seq'::regclass),
    product_version_id character varying(100) COLLATE pg_catalog."default",
    product_id character varying(100) COLLATE pg_catalog."default",
    name character varying(100) COLLATE pg_catalog."default",
    startDate timestamp without time zone,
    endDate timestamp without time zone,
    isactive character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT product_version_pkey PRIMARY KEY (id)
) ;

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

ALTER TABLE IF EXISTS public.policy
    ADD COLUMN version_id character varying(100);

ALTER TABLE IF EXISTS public.product_coverage
    ADD COLUMN product_version_id character varying(100);

-- Policy Coverage Indexes
CREATE INDEX idx_policy_id
    ON public.policy_coverage USING btree
    (policy_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_id
    ON public.policy_coverage USING btree
    (policy_coverage_id ASC NULLS LAST);

CREATE INDEX idx_product_coverage_id
    ON public.policy_coverage USING btree
    (product_coverage_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_deductible_id
    ON public.policy_coverage_deductible_conditions USING btree
    (policy_coverage_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_limit_id
    ON public.policy_coverage_limit USING btreemain
    (policy_coverage_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_location_id
    ON public.policy_coverage_physical_location USING btree
    (policy_coverage_id ASC NULLS LAST);

-- Product Version Indexes
CREATE INDEX idx_product_version_id
    ON public.product_version USING btree
    (product_version_id ASC NULLS LAST);

CREATE INDEX idx_product_id
    ON public.product_version USING btree
    (policy_coverage_id ASC NULLS LAST);

CREATE INDEX idx_product_version
    ON public.product_version USING btree
    (version ASC NULLS LAST);



-- DML:
-- Create Product Versions
SELECT
    NEXTVAL('product_version_id_seq'),
    'PV-5555',
    product_id,
    '1.0',
    current_timestamp,
    NULL,
    'true',
    '1.0',
    premium
INTO TABLE product_version
FROM
    product;

-- Create Policy Coverage
SELECT
    NEXTVAL('policy_coverage_id_seq'),
    'PV-5555',
    product_id,
    '1.0',
    current_timestamp,
    NULL,
    'true',
    '1.0',
    premium

INTO TABLE product_version
FROM
    product




# ------ Fodder ------------- #
CREATE TABLE IF NOT EXISTS public.member
(
    id integer NOT NULL DEFAULT nextval('member_id_seq'::regclass),
    member_id character varying(100) COLLATE pg_catalog."default",
    lastname character varying(100) COLLATE pg_catalog."default",
    firstname character varying(100) COLLATE pg_catalog."default",
    middlename character varying(100) COLLATE pg_catalog."default",
    suffix character varying(100) COLLATE pg_catalog."default",
    modified_at timestamp without time zone,
    ssn character varying(100) COLLATE pg_catalog."default",
    dateofbirth timestamp without time zone,
    gender character varying(100) COLLATE pg_catalog."default",
    ethnicity character varying(100) COLLATE pg_catalog."default",
    maritialstatus character varying(100) COLLATE pg_catalog."default",
    primaryprovider_id character varying(100) COLLATE pg_catalog."default",
    effectivedate timestamp without time zone,
    enddate timestamp without time zone,
    citizenshipstatuscode character varying(100) COLLATE pg_catalog."default",
    situsstate character varying(100) COLLATE pg_catalog."default",
    weight integer,
    planyear timestamp without time zone,
    totalpayments real,
    CONSTRAINT member_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;