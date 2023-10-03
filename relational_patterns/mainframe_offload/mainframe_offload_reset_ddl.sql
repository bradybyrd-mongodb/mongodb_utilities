--  DDL:

DROP TABLE IF EXISTS public.product_version;
DROP TABLE IF EXISTS public.policy_coverage;
DROP TABLE IF EXISTS public.policy_coverage_deductible_conditions;
DROP TABLE IF EXISTS public.policy_coverage_limit;
DROP TABLE IF EXISTS public.policy_coverage_physical_location;

ALTER TABLE IF EXISTS public.policy
    DROP COLUMN version_id;

ALTER TABLE IF EXISTS public.product_coverage
    DROP COLUMN product_version_id;

-- Policy Coverage Indexes

DROP INDEX 
    IF EXISTS idx_product_coverage_id;



