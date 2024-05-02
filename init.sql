CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS calls (
    call_id serial PRIMARY KEY,
    phone_number VARCHAR(50),
    call_text TEXT
);