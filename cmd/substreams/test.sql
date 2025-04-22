--CREATE TABLE sol_account_balances (
--    block_slot INT,
--    block_date VARCHAR,
--    account VARCHAR,
--    post_balance INT,
--    PRIMARY KEY (block_slot, block_date)
--)
--WITH
--    (
--        connector = 'kafka',
--        topic = 'test-topic',
--        properties.bootstrap.server = 'localhost:9092'
--    ) FORMAT PLAIN ENCODE PROTOBUF (
--        message = 'sf.solana.account_sol_balance.v1.Output',
--        location = 'https://spkg.io/v1/packages/tl_account_sol_balances_1_0_0/v1.0.0'
--    );
--
CREATE TABLE sol_account_balances (
    *,
    --    blockhash VARCHAR, -- from kafka metadata
)
WITH
    (
        connector = 'kafka',
        topic = 'test-topic',
        properties.bootstrap.server = 'localhost:9092'
    ) FORMAT PLAIN ENCODE PROTOBUF (
        message = 'sf.solana.account_sol_balance.v1.Output',
        schema.location = 'https://spkg.io/v1/packages/tl_account_sol_balances_1_0_0/v1.0.0'
    );

CREATE materialized VIEW sol_account_balances_flat AS
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

CREATE MATERIALIZED VIEW sol_balances_view AS
SELECT
    SUM(post_balance) AS total_balance,
    AVG(post_balance) AS average_balance,
    COUNT(account) AS accounts
FROM
    sol_account_balances_flat
GROUP BY
    block_slot;

-- https://storage.googleapis.com/substreams-registry/order-v0.1.0.spkg
CREATE TABLE order (*)
WITH
    (
        connector = 'kafka',
        topic = 'test-topic',
        properties.bootstrap.server = 'localhost:9092'
    ) FORMAT PLAIN ENCODE PROTOBUF (
        message = 'test.relations.Output',
        schema.location = 'https://storage.googleapis.com/substreams-registry/order-v0.1.0.spkg'
    );

CREATE TABLE s1 (*)
WITH
    (
        connector = 'google_pubsub',
        pubsub.subscription = 'projects/dfuseio-local/subscriptions/test',
        pubsub.emulator_host = 'localhost:8085'
    ) FORMAT PLAIN ENCODE PROTOBUF (
        message = 'test.relations.Output',
        schema.location = 'https://storage.googleapis.com/substreams-registry/order-v0.1.0.spkg'
    );
