DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(month)) INTO max_length FROM dim_date_times;
    EXECUTE format('ALTER TABLE dim_date_times ALTER COLUMN month TYPE varchar(%s)', max_length);
END
$$;

DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(day)) INTO max_length FROM dim_date_times;
    EXECUTE format('ALTER TABLE dim_date_times ALTER COLUMN day TYPE varchar(%s)', max_length);
END
$$;

DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(year)) INTO max_length FROM dim_date_times;
    EXECUTE format('ALTER TABLE dim_date_times ALTER COLUMN year TYPE varchar(%s)', max_length);
END
$$;

DO $$
DECLARE
    max_length INT;
BEGIN
    SELECT MAX(LENGTH(time_period)) INTO max_length FROM dim_date_times;
    EXECUTE format('ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE varchar(%s)', max_length);
END
$$;

ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

