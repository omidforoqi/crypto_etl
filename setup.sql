CREATE DATABASE IF NOT EXISTS crypto_db;

USE crypto_db;

CREATE TABLE IF NOT EXISTS bitstamp
(
    coin_name String,
    open Float32,
    close Float32,
    high Float32,
    low Float32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (coin_name, timestamp);

CREATE TABLE IF NOT EXISTS transactions
(
    coin_name String,
    amount Float32,
    timestamp DateTime,
    price Float32,
    type Bool
) ENGINE = MergeTree()
ORDER BY (coin_name, timestamp, amount);