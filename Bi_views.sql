CREATE OR REPLACE VIEW v_kpi_monthly AS
SELECT
  dd.year,
  dd.month,
  COALESCE(t.tariff_name, 'UNKNOWN') AS tariff_name,
  COALESCE(s.segment, 'UNKNOWN')     AS segment,
  COUNT(DISTINCT fu.subscriber_key)  AS active_subscribers,
  SUM(fu.revenue_amount)             AS total_revenue,
  CASE WHEN COUNT(DISTINCT fu.subscriber_key) > 0
       THEN ROUND(SUM(fu.revenue_amount) / COUNT(DISTINCT fu.subscriber_key), 4)
       ELSE 0 END                    AS arpu
FROM fact_usage fu
JOIN dim_date dd ON dd.date_key = fu.date_key
JOIN dim_subscriber s ON s.subscriber_key = fu.subscriber_key
LEFT JOIN dim_tariff t ON t.tariff_key = fu.tariff_key
GROUP BY dd.year, dd.month, COALESCE(t.tariff_name, 'UNKNOWN'), COALESCE(s.segment, 'UNKNOWN');

CREATE OR REPLACE VIEW v_churn_monthly AS
WITH months AS (
  SELECT DISTINCT make_date(year, month, 1) AS month_start
  FROM dim_date
),
base AS (
  SELECT m.month_start, COUNT(*) AS base_subscribers
  FROM months m
  JOIN dim_subscriber s
    ON s.activation_date <= m.month_start
   AND (s.deactivation_date IS NULL OR s.deactivation_date >= m.month_start)
  GROUP BY m.month_start
),
churned AS (
  SELECT date_trunc('month', deactivation_date)::date AS month_start,
         COUNT(*) AS churned_subscribers
  FROM dim_subscriber
  WHERE deactivation_date IS NOT NULL
  GROUP BY date_trunc('month', deactivation_date)::date
)
SELECT
  EXTRACT(YEAR FROM b.month_start)::int  AS year,
  EXTRACT(MONTH FROM b.month_start)::int AS month,
  b.base_subscribers,
  COALESCE(c.churned_subscribers, 0) AS churned_subscribers,
  CASE WHEN b.base_subscribers > 0
       THEN ROUND(100.0 * COALESCE(c.churned_subscribers,0) / b.base_subscribers, 4)
       ELSE 0 END AS churn_rate_pct
FROM base b
LEFT JOIN churned c ON c.month_start = b.month_start
ORDER BY year, month;

CREATE OR REPLACE VIEW v_network_daily AS
SELECT
  dd.full_date AS date,
  cs.technology,
  g.region,
  SUM(nk.traffic_mb) AS traffic_mb,
  ROUND(AVG(nk.success_ratio), 4) AS avg_success_ratio,
  ROUND(AVG(nk.drop_ratio), 4)    AS avg_drop_ratio
FROM fact_network_kpi nk
JOIN dim_date dd ON dd.date_key = nk.date_key
JOIN dim_cell_site cs ON cs.cell_key = nk.cell_key
LEFT JOIN dim_geo g ON g.geo_key = cs.geo_key
GROUP BY dd.full_date, cs.technology, g.region;
