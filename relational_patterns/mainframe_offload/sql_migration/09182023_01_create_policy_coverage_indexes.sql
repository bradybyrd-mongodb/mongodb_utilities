-- Policy Coverage Indexes
CREATE INDEX idx_policy_id
    ON public.policy_coverage USING btree
    (policy_id ASC NULLS LAST);

CREATE INDEX idx_product_coverage_id
    ON public.policy_coverage USING btree
    (product_coverage_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_deductible_id
    ON public.policy_coverage_deductible_conditions USING btree
    (policy_coverage_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_limit_id
    ON public.policy_coverage_limit USING btree
    (policy_coverage_id ASC NULLS LAST);

CREATE INDEX idx_policy_coverage_location_id
    ON public.policy_coverage_physical_location USING btree
    (policy_coverage_id ASC NULLS LAST);
