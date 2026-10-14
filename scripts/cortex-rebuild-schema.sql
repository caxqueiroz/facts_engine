-- Explicit upgrade for already provisioned projection databases. Run with the
-- migration owner before deploying the rebuild-capable runtime adapter.
CREATE TABLE IF NOT EXISTS facts_projection_gate (
 singleton boolean PRIMARY KEY CHECK(singleton),
 fence bigint NOT NULL CHECK(fence>=0),
 sealed boolean NOT NULL
);
INSERT INTO facts_projection_gate VALUES(true,0,false) ON CONFLICT DO NOTHING;
