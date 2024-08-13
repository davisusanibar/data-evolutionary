CREATE TABLE IF NOT EXISTS public.payments (
    id SERIAL PRIMARY KEY,
    order_id BIGINT,
    customer_id BIGINT,
    amount NUMERIC(10, 2),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- YYYY-MM-DD HH:MI:SS
);

INSERT INTO payments (order_id, customer_id, amount, create_time) VALUES (1, 370, 100.00, '1996-01-01 00:00:00');

INSERT INTO payments (order_id, customer_id, amount, create_time) VALUES (1, 370, 200.00, '1996-01-01 00:00:00');
