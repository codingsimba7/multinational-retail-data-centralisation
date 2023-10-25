-- -- update product price to remove "£" 
UPDATE dim_products SET product_price = REPLACE(product_price, '£', '');
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(255);
UPDATE dim_products SET weight_class = 
CASE 
    WHEN dim_products.weight < 2 THEN 'Light'
    WHEN dim_products.weight >= 2 AND dim_products.weight < 40 THEN 'Mid_Sized'
    WHEN dim_products.weight >= 40 AND dim_products.weight <140 THEN 'Heavy'
    WHEN dim_products.weight >= 140 THEN 'Truck_Required'
END;


ALTER TABLE dim_products RENAME COLUMN removed to still_available;
ALTER TABLE dim_products ALTER COLUMN product_price TYPE FLOAT USING product_price::float;
ALTER TABLE dim_products ALTER COLUMN weight TYPE FLOAT USING weight::float;

DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH("EAN")) INTO max_length FROM dim_products;
    EXECUTE format('ALTER TABLE dim_products ALTER COLUMN "EAN" TYPE varchar(%s)', max_length);
END
$$;



DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(product_code)) INTO max_length FROM dim_products;
    EXECUTE format('ALTER TABLE dim_products ALTER COLUMN product_code TYPE varchar(%s)', max_length);
END
$$;

ALTER TABLE dim_products ALTER COLUMN date_added TYPE DATE USING date_added::date;
ALTER TABLE dim_products ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

UPDATE dim_products SET still_available =
CASE 
    WHEN dim_products.still_available = 'Still_available' THEN TRUE
    WHEN dim_products.still_available = 'Removed' THEN FALSE
END;
-- ALTER TABLE dim_products ALTER COLUMN still_avaliable TYPE BOOLEAN USING still_available::boolean;

DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(weight_class)) INTO max_length FROM dim_products;
    EXECUTE format('ALTER TABLE dim_products ALTER COLUMN weight_class TYPE varchar(%s)', max_length);
END
$$;


