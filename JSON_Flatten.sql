# Flatten de JSON complexo
{
  "order_id": 1,
  "customer": {
    "id": 10
  },
  "items": [
    {
      "product_id": 1,
      "price": 10
    }
  ]
}

SELECT
    payload ->> 'order_id' AS order_id,
    payload -> 'customer' ->> 'id' AS customer_id,
    item ->> 'product_id' AS product_id,
    item ->> 'price' AS price
FROM raw_orders,
LATERAL jsonb_array_elements(payload -> 'items') AS item;
