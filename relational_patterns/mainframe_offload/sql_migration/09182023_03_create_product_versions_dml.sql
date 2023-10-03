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