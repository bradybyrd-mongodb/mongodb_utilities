ALTER TABLE IF EXISTS public.policy
    ADD COLUMN version_id character varying(100);


ALTER TABLE IF EXISTS public.product_coverage
    ADD COLUMN product_version_id character varying(100);