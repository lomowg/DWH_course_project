CREATE TABLE IF NOT EXISTS dim_date (
  date_key INTEGER PRIMARY KEY,
  full_date DATE NOT NULL,
  year SMALLINT NOT NULL,
  quarter SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  day SMALLINT NOT NULL,
  is_month_start BOOLEAN DEFAULT false,
  is_month_end BOOLEAN DEFAULT false,
  is_weekend BOOLEAN DEFAULT false
);

CREATE TABLE IF NOT EXISTS dim_time (
  time_key INTEGER PRIMARY KEY,
  full_time TIME NOT NULL,
  hour SMALLINT NOT NULL,
  minute SMALLINT NOT NULL,
  second SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_geo (
  geo_key SERIAL PRIMARY KEY,
  country VARCHAR(100) NOT NULL,
  region VARCHAR(100),
  city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_subscriber (
  subscriber_key SERIAL PRIMARY KEY,
  subscriber_id VARCHAR(50) NOT NULL,
  msisdn VARCHAR(20) NOT NULL,
  customer_type VARCHAR(30),
  segment VARCHAR(50),
  status VARCHAR(30),
  activation_date DATE,
  deactivation_date DATE,
  geo_key INTEGER
);

CREATE TABLE IF NOT EXISTS dim_tariff (
  tariff_key SERIAL PRIMARY KEY,
  tariff_code VARCHAR(50) NOT NULL,
  tariff_name VARCHAR(200) NOT NULL,
  tariff_type VARCHAR(50),
  is_active BOOLEAN DEFAULT true,
  valid_from DATE,
  valid_to DATE
);

CREATE TABLE IF NOT EXISTS dim_service (
  service_key SERIAL PRIMARY KEY,
  service_code VARCHAR(50) NOT NULL,
  service_name VARCHAR(200) NOT NULL,
  service_group VARCHAR(100),
  is_recurring BOOLEAN DEFAULT false
);

CREATE TABLE IF NOT EXISTS dim_cell_site (
  cell_key SERIAL PRIMARY KEY,
  cell_id VARCHAR(50) NOT NULL,
  geo_key INTEGER,
  technology VARCHAR(10),
  site_name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_channel (
  channel_key SERIAL PRIMARY KEY,
  channel_code VARCHAR(50) NOT NULL,
  channel_name VARCHAR(200) NOT NULL,
  channel_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_usage (
  usage_key BIGSERIAL PRIMARY KEY,
  date_key INTEGER NOT NULL,
  time_key INTEGER NOT NULL,
  tariff_key INTEGER,
  subscriber_key INTEGER NOT NULL,
  service_key INTEGER NOT NULL,
  cell_key INTEGER,
  call_duration_sec INTEGER DEFAULT 0,
  traffic_mb NUMERIC(18,4) DEFAULT 0,
  units NUMERIC(18,4) DEFAULT 0,
  revenue_amount NUMERIC(18,4) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_billing (
  billing_key BIGSERIAL PRIMARY KEY,
  tariff_key INTEGER,
  date_key INTEGER NOT NULL,
  subscriber_key INTEGER NOT NULL,
  amount NUMERIC(18,4) NOT NULL,
  charge_type VARCHAR(50),
  description VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS fact_payment (
  payment_key BIGSERIAL PRIMARY KEY,
  subscriber_key INTEGER NOT NULL,
  date_key INTEGER NOT NULL,
  channel_key INTEGER,
  amount NUMERIC(18,4) NOT NULL,
  payment_method VARCHAR(50),
  status VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS fact_network_kpi (
  kpi_key BIGSERIAL PRIMARY KEY,
  date_key INTEGER NOT NULL,
  time_key INTEGER NOT NULL,
  cell_key INTEGER NOT NULL,
  traffic_mb NUMERIC(18,4) DEFAULT 0,
  call_attempts BIGINT DEFAULT 0,
  call_successes BIGINT DEFAULT 0,
  call_drops BIGINT DEFAULT 0,
  success_ratio NUMERIC(5,2),
  drop_ratio NUMERIC(5,2)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_subscriber_subscriber_id ON dim_subscriber (subscriber_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_tariff_code ON dim_tariff (tariff_code);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_service_code ON dim_service (service_code);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_cell_cell_id ON dim_cell_site (cell_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_channel_code ON dim_channel (channel_code);

CREATE INDEX IF NOT EXISTS ix_fact_usage_date_subscriber ON fact_usage (date_key, subscriber_key);
CREATE INDEX IF NOT EXISTS ix_fact_billing_date_subscriber ON fact_billing (date_key, subscriber_key);
CREATE INDEX IF NOT EXISTS ix_fact_payment_date_subscriber ON fact_payment (date_key, subscriber_key);
CREATE INDEX IF NOT EXISTS ix_fact_network_kpi_date_cell ON fact_network_kpi (date_key, cell_key);


DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_subscriber_geo') THEN
    ALTER TABLE dim_subscriber
      ADD CONSTRAINT fk_subscriber_geo
      FOREIGN KEY (geo_key) REFERENCES dim_geo (geo_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_cell_geo') THEN
    ALTER TABLE dim_cell_site
      ADD CONSTRAINT fk_cell_geo
      FOREIGN KEY (geo_key) REFERENCES dim_geo (geo_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_usage_date') THEN
    ALTER TABLE fact_usage
      ADD CONSTRAINT fk_usage_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_usage_time') THEN
    ALTER TABLE fact_usage
      ADD CONSTRAINT fk_usage_time FOREIGN KEY (time_key) REFERENCES dim_time (time_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_usage_subscriber') THEN
    ALTER TABLE fact_usage
      ADD CONSTRAINT fk_usage_subscriber FOREIGN KEY (subscriber_key) REFERENCES dim_subscriber (subscriber_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_usage_tariff') THEN
    ALTER TABLE fact_usage
      ADD CONSTRAINT fk_usage_tariff FOREIGN KEY (tariff_key) REFERENCES dim_tariff (tariff_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_usage_service') THEN
    ALTER TABLE fact_usage
      ADD CONSTRAINT fk_usage_service FOREIGN KEY (service_key) REFERENCES dim_service (service_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_usage_cell') THEN
    ALTER TABLE fact_usage
      ADD CONSTRAINT fk_usage_cell FOREIGN KEY (cell_key) REFERENCES dim_cell_site (cell_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_billing_date') THEN
    ALTER TABLE fact_billing
      ADD CONSTRAINT fk_billing_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_billing_subscriber') THEN
    ALTER TABLE fact_billing
      ADD CONSTRAINT fk_billing_subscriber FOREIGN KEY (subscriber_key) REFERENCES dim_subscriber (subscriber_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_billing_tariff') THEN
    ALTER TABLE fact_billing
      ADD CONSTRAINT fk_billing_tariff FOREIGN KEY (tariff_key) REFERENCES dim_tariff (tariff_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_payment_date') THEN
    ALTER TABLE fact_payment
      ADD CONSTRAINT fk_payment_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_payment_subscriber') THEN
    ALTER TABLE fact_payment
      ADD CONSTRAINT fk_payment_subscriber FOREIGN KEY (subscriber_key) REFERENCES dim_subscriber (subscriber_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_payment_channel') THEN
    ALTER TABLE fact_payment
      ADD CONSTRAINT fk_payment_channel FOREIGN KEY (channel_key) REFERENCES dim_channel (channel_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_kpi_date') THEN
    ALTER TABLE fact_network_kpi
      ADD CONSTRAINT fk_kpi_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_kpi_time') THEN
    ALTER TABLE fact_network_kpi
      ADD CONSTRAINT fk_kpi_time FOREIGN KEY (time_key) REFERENCES dim_time (time_key);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_kpi_cell') THEN
    ALTER TABLE fact_network_kpi
      ADD CONSTRAINT fk_kpi_cell FOREIGN KEY (cell_key) REFERENCES dim_cell_site (cell_key);
  END IF;
END $$;
