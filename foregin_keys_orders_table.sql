INSERT INTO dim_card_details (card_number, expiry_date, card_provider, date_payment_confirmed)
SELECT DISTINCT card_number, NULL, NULL, NULL::date
FROM orders_table 
WHERE card_number IS NOT NULL AND card_number NOT IN (SELECT card_number FROM dim_card_details);

INSERT INTO dim_users (user_uuid)
SELECT DISTINCT user_uuid
from orders_table
WHERE user_uuid IS NOT NULL AND user_uuid NOT IN (SELECT user_uuid FROM dim_users);

ALTER TABLE orders_table
ADD CONSTRAINT fk_dm_card_details FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
ADD CONSTRAINT fk_date_details FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD CONSTRAINT fk_products FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
ADD CONSTRAINT fk_store_details FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD CONSTRAINT fk_customer_details FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

