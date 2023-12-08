CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS numbers (
    id serial PRIMARY KEY,
    number BIGINT,
    timestamp BIGINT
);

CREATE TABLE IF NOT EXISTS users (
    user_id serial PRIMARY KEY,
    username VARCHAR(50),
    password TEXT
);