SELECT
	dim_customers.customer_id  AS dim_customers_customer_id,
	dim_customers.customer_lifetime_value  AS dim_customers_customer_lifetime_value,
	CAST(CAST(dim_customers.first_order  AS TIMESTAMP) AS DATE) AS dim_customers_first_order_date,
	FORMAT_TIMESTAMP('%Y-%m', CAST(dim_customers.first_order  AS TIMESTAMP)) AS dim_customers_first_order_month,
	FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_TRUNC(CAST(CAST(dim_customers.first_order  AS TIMESTAMP) AS TIMESTAMP), QUARTER)) AS dim_customers_first_order_quarter,
	dim_customers.first_order  AS dim_customers_first_order_raw,
	FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(CAST(dim_customers.first_order  AS TIMESTAMP), WEEK(MONDAY))) AS dim_customers_first_order_week,
	EXTRACT(YEAR FROM CAST(dim_customers.first_order  AS TIMESTAMP)) AS dim_customers_first_order_year,
	CAST(CAST(dim_customers.most_recent_order  AS TIMESTAMP) AS DATE) AS dim_customers_most_recent_order_date,
	FORMAT_TIMESTAMP('%Y-%m', CAST(dim_customers.most_recent_order  AS TIMESTAMP)) AS dim_customers_most_recent_order_month,
	FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_TRUNC(CAST(CAST(dim_customers.most_recent_order  AS TIMESTAMP) AS TIMESTAMP), QUARTER)) AS dim_customers_most_recent_order_quarter,
	dim_customers.most_recent_order  AS dim_customers_most_recent_order_raw,
	FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(CAST(dim_customers.most_recent_order  AS TIMESTAMP), WEEK(MONDAY))) AS dim_customers_most_recent_order_week,
	EXTRACT(YEAR FROM CAST(dim_customers.most_recent_order  AS TIMESTAMP)) AS dim_customers_most_recent_order_year,
	dim_customers.number_of_orders  AS dim_customers_number_of_orders,
	fct_orders.amount  AS fct_orders_amount,
	fct_orders.bank_transfer_amount  AS fct_orders_bank_transfer_amount,
	fct_orders.coupon_amount  AS fct_orders_coupon_amount,
	fct_orders.credit_card_amount  AS fct_orders_credit_card_amount,
	fct_orders.customer_id  AS fct_orders_customer_id,
	fct_orders.gift_card_amount  AS fct_orders_gift_card_amount,
	CAST(CAST(fct_orders.order_date  AS TIMESTAMP) AS DATE) AS fct_orders_order_date,
	fct_orders.order_id  AS fct_orders_order_id,
	FORMAT_TIMESTAMP('%Y-%m', CAST(fct_orders.order_date  AS TIMESTAMP)) AS fct_orders_order_month,
	FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_TRUNC(CAST(CAST(fct_orders.order_date  AS TIMESTAMP) AS TIMESTAMP), QUARTER)) AS fct_orders_order_quarter,
	fct_orders.order_date  AS fct_orders_order_raw,
	FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(CAST(fct_orders.order_date  AS TIMESTAMP), WEEK(MONDAY))) AS fct_orders_order_week,
	EXTRACT(YEAR FROM CAST(fct_orders.order_date  AS TIMESTAMP)) AS fct_orders_order_year,
	fct_orders.status  AS fct_orders_status
FROM analytics.fct_orders  AS fct_orders
LEFT JOIN analytics.dim_customers  AS dim_customers ON fct_orders.customer_id || 'asd' = dim_customers.customer_id

WHERE 1 = 2
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
ORDER BY 3 DESC
LIMIT 0