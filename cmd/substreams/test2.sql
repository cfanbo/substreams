select
    (e).types_test.id AS types_test_id,
    (e).types_test.double_field,
    (e).types_test.float_field,
    (e).types_test.int32_field,
    (e).types_test.int64_field,
    (e).types_test.uint32_field,
    (e).types_test.uint64_field,
    (e).types_test.sint32_field,
    (e).types_test.sint64_field,
    (e).types_test.fixed32_field,
    (e).types_test.fixed64_field,
    (e).types_test.sfixed32_field,
    (e).types_test.sfixed64_field,
    (e).types_test.bool_field,
    (e).types_test.string_field,
    (e).types_test.bytes_field,
    (e).types_test.timestamp_field.seconds AS timestamp_seconds,
    (e).types_test.timestamp_field.nanos AS timestamp_nanos,
    -- Accessing customer struct
    (e).customer.customer_id,
    (e).customer.name AS customer_name,
    -- Accessing order struct
    (e).order.order_id,
    (e).order.customer_ref_id,
    -- Items will still be an array of struct - we can unnest that separately if needed
    (e).order.items AS order_items,
    -- Accessing item struct
    (e).item.item_id AS item_id,
    (e).item.name AS item_name,
    (e).item.price
from
    (
        select
            unnest (entities) e
        from
            order
    );

SELECT
    _row_id,
    _rw_timestamp,
    -- Accessing types_test struct
    e.types_test.id AS types_test_id,
    e.types_test.double_field,
    e.types_test.float_field,
    e.types_test.int32_field,
    e.types_test.int64_field,
    e.types_test.uint32_field,
    e.types_test.uint64_field,
    e.types_test.sint32_field,
    e.types_test.sint64_field,
    e.types_test.fixed32_field,
    e.types_test.fixed64_field,
    e.types_test.sfixed32_field,
    e.types_test.sfixed64_field,
    e.types_test.bool_field,
    e.types_test.string_field,
    e.types_test.bytes_field,
    e.types_test.timestamp_field.seconds AS timestamp_seconds,
    e.types_test.timestamp_field.nanos AS timestamp_nanos,
    -- Accessing customer struct
    e.customer.customer_id,
    e.customer.name AS customer_name,
    -- Accessing order struct
    e.order.order_id,
    e.order.customer_ref_id,
    -- Items will still be an array of struct - we can unnest that separately if needed
    e.order.items AS order_items,
    -- Accessing item struct
    e.item.item_id AS item_id,
    e.item.name AS item_name,
    e.item.price
FROM
    order,
    UNNEST (entities) AS e;

SELECT
    (data).block_slot,
    (data).block_date,
    (data).account,
    (data).post_balance (data2).other_data,
FROM
    (
        select
            unnest (data) data,
            unnest (data2) data2
        from
            sol_account_balances
        where
            blockhash not in (
                select
                    invalid_blockashes
            )
    );
