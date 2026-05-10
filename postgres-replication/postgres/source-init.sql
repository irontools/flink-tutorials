CREATE TABLE users (
    id TEXT PRIMARY KEY,
    created_at BIGINT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    job_title TEXT NOT NULL,
    random_identifier BIGINT NOT NULL,
    active BOOLEAN NOT NULL
);

ALTER TABLE users REPLICA IDENTITY FULL;

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    user_id TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

ALTER TABLE orders REPLICA IDENTITY FULL;
