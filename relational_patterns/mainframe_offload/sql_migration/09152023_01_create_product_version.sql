CREATE SEQUENCE IF NOT EXISTS product_version_id_seq START 1000000;
CREATE TABLE IF NOT EXISTS public.product_version
(
    id integer NOT NULL DEFAULT nextval('product_version_id_seq'::regclass),
    product_version_id character varying(100) COLLATE pg_catalog."default",
    product_id character varying(100) COLLATE pg_catalog."default",
    version character varying(100) COLLATE pg_catalog."default",
    startDate timestamp without time zone,
    endDate timestamp without time zone,
    isactive character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT product_version_pkey PRIMARY KEY (id)
) ;