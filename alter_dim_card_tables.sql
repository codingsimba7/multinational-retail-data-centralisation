
DO $$
DECLARE max_length int;
BEGIN
    SELECT max(length(card_number)) INTO max_length FROM dim_card_details;
    EXECUTE format('ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE varchar(%s)', max_length);
END
$$;

DO $$
DECLARE max_length int;
BEGIN
    SELECT max(length(expiry_date)) INTO max_length FROM dim_card_details;
    EXECUTE format('ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE varchar(%s)', max_length);
END
$$;

ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE DATE;


select expiry_date from dim_card_details;