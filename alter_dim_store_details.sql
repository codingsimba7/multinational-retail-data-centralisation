


UPDATE dim_store_details SET latitude = COALESCE(latitude, lat);


-- Alter the table to change longitute and latitude to double precision
ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE numeric(20,10) USING longitude::numeric(20,10);
ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE double precision;
ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE numeric(20,10) USING latitude::numeric(20,10);
ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE double precision;
ALTER TABLE dim_store_details ALTER COLUMN locality TYPE VARCHAR(255);

-- -- leverage PL/pgSQL to dynamically alter the table to the max length of the store_code column
DO $$ 
DECLARE
    max_length INT;
BEGIN
    -- Find the max length
    SELECT MAX(LENGTH(store_code)) INTO max_length FROM dim_store_details;

    -- Execute the ALTER TABLE command dynamically
    EXECUTE format('ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE varchar(%s)', max_length);
END
$$;

ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE numeric(3,0) USING staff_numbers::numeric(3,0);
ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE SMALLINT;
-- ALTER TABLE dim_store_details ALTER COLUMN opening_date TYPE DATE;
-- ALTER TABLE dim_store_details ALTER COLUMN store_type SET DATA TYPE VARCHAR(255);



DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(country_code)) INTO max_length FROM dim_store_details;
EXECUTE format('ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE varchar(%s)', max_length);
END
$$;


ALTER TABLE dim_store_details ALTER COLUMN continent TYPE VARCHAR(255);
