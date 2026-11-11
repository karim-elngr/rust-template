-- Fresh template schema. Never replace an applied migration in a deployed database.
CREATE TABLE catalog (
    sku text PRIMARY KEY,
    unit_price_minor bigint NOT NULL CHECK (unit_price_minor > 0),
    currency text NOT NULL CHECK (currency = 'USD'),
    active boolean NOT NULL DEFAULT true
);
INSERT INTO catalog (sku, unit_price_minor, currency)
VALUES ('BOOK', 2500, 'USD'), ('COFFEE', 700, 'USD');

CREATE TABLE carts (
    id uuid PRIMARY KEY,
    customer_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    status text NOT NULL CHECK (status IN ('open', 'checked_out')),
    data jsonb NOT NULL
);
CREATE TABLE orders (
    id uuid PRIMARY KEY,
    checkout_id uuid NOT NULL UNIQUE,
    customer_id uuid NOT NULL,
    data jsonb NOT NULL
);
CREATE TABLE payments (
    id uuid PRIMARY KEY,
    order_id uuid NOT NULL UNIQUE REFERENCES orders(id),
    checkout_id uuid NOT NULL UNIQUE,
    data jsonb NOT NULL
);
CREATE TABLE checkouts (
    id uuid PRIMARY KEY,
    cart_id uuid NOT NULL UNIQUE REFERENCES carts(id),
    order_id uuid NOT NULL UNIQUE REFERENCES orders(id),
    payment_id uuid NOT NULL UNIQUE REFERENCES payments(id),
    status text NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    expires_at timestamptz NOT NULL,
    data jsonb NOT NULL
);
-- Checkout creation establishes both sides of these links in one transaction.
ALTER TABLE orders ADD FOREIGN KEY (checkout_id)
    REFERENCES checkouts(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE payments ADD FOREIGN KEY (checkout_id)
    REFERENCES checkouts(id) DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX checkout_expiry ON checkouts(expires_at, id)
    WHERE status IN ('awaiting_authorization', 'awaiting_capture', 'awaiting_order_confirmation');

CREATE TABLE checkout_requests (
    idempotency_key uuid PRIMARY KEY,
    checkout_id uuid NOT NULL UNIQUE REFERENCES checkouts(id),
    canonical_request jsonb NOT NULL,
    receipt jsonb NOT NULL
);
-- Internal requests contain opaque provider tokens. Reporting roles need no access.
CREATE TABLE payment_operations (
    operation_key text PRIMARY KEY,
    payment_id uuid NOT NULL REFERENCES payments(id),
    kind text NOT NULL CHECK (kind IN ('authorize', 'capture', 'void', 'refund')),
    status text NOT NULL CHECK (status IN ('pending', 'completed')),
    request jsonb NOT NULL,
    response jsonb,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_attempt_at timestamptz,
    completed_at timestamptz,
    UNIQUE (payment_id, kind)
);
CREATE TABLE order_summaries (
    order_id uuid PRIMARY KEY REFERENCES orders(id),
    version bigint NOT NULL,
    data jsonb NOT NULL
);

CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    checkout_id uuid NOT NULL REFERENCES checkouts(id),
    event_type text NOT NULL,
    dedup_key text NOT NULL UNIQUE,
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object' AND payload->>'event_id' = id::text
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token uuid,
    lease_until timestamptz,
    last_error text,
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX outbox_pending ON outbox(available_at, created_at, id)
    WHERE published_at IS NULL;
CREATE TABLE inbox (
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, event_id)
);
