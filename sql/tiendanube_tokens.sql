CREATE TABLE IF NOT EXISTS tiendanube_tokens (
  store_id     BIGINT       PRIMARY KEY,
  access_token TEXT         NOT NULL,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
