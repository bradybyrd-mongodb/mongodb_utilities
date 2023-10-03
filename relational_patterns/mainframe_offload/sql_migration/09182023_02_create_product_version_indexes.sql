-- Product Version Indexes
CREATE INDEX idx_product_version_id
    ON public.product_version USING btree
    (product_version_id ASC NULLS LAST);

CREATE INDEX idx_product_id
    ON public.product_version USING btree
    (product_id ASC NULLS LAST);

CREATE INDEX idx_product_version
    ON public.product_version USING btree
    (version ASC NULLS LAST);
