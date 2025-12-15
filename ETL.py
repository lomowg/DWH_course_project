import os
import datetime
from pathlib import Path
import psycopg2
from psycopg2 import sql


# Базовая директория проекта (папка, где лежит этот скрипт)
BASE_DIR = Path(__file__).resolve().parent

# Папка с CSV-файлами, которые сгенерированы генератором тестовых данных
CSV_DIR = BASE_DIR / "data_out"


def get_conn():
    # Создаём подключение к PostgreSQL.
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "KR"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "root"),
    )


def exec_file(cur, path: Path):
    # Выполняем SQL-скрипт из файла
    with open(path, "r", encoding="utf-8") as f:
        cur.execute(f.read())


def copy_csv(cur, table: str, csv_path: Path, columns: list[str]):
    # Загружаем CSV в таблицу через команду COPY
    # columns — список колонок, в которые идёт загрузка
    with open(csv_path, "r", encoding="utf-8") as f:
        next(f)  # пропускаем заголовок CSV
        cur.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '')").format(
                sql.Identifier(*table.split(".")),
                sql.SQL(",").join(map(sql.Identifier, columns))
            ),
            f
        )


def create_temp_tables(cur):
    # Создаём временные staging-таблицы (tmp_*) в рамках текущей сессии.
    # Они используются как "буфер" для загрузки CSV перед трансформациями и загрузкой в DWH-таблицы.
    cur.execute("""
    CREATE TEMP TABLE tmp_subscribers(
      subscriber_id VARCHAR(50),
      msisdn VARCHAR(20),
      customer_type VARCHAR(30),
      segment VARCHAR(50),
      status VARCHAR(30),
      activation_date DATE,
      deactivation_date DATE,
      country VARCHAR(100),
      region VARCHAR(100),
      city VARCHAR(100)
    );

    CREATE TEMP TABLE tmp_tariffs(
      tariff_code VARCHAR(50),
      tariff_name VARCHAR(200),
      tariff_type VARCHAR(50),
      is_active BOOLEAN,
      valid_from DATE,
      valid_to DATE
    );

    CREATE TEMP TABLE tmp_services(
      service_code VARCHAR(50),
      service_name VARCHAR(200),
      service_group VARCHAR(100),
      is_recurring BOOLEAN
    );

    CREATE TEMP TABLE tmp_channels(
      channel_code VARCHAR(50),
      channel_name VARCHAR(200),
      channel_type VARCHAR(50)
    );

    CREATE TEMP TABLE tmp_cell_sites(
      cell_id VARCHAR(50),
      country VARCHAR(100),
      region VARCHAR(100),
      city VARCHAR(100),
      technology VARCHAR(10),
      site_name VARCHAR(200)
    );

    CREATE TEMP TABLE tmp_usage(
      event_id VARCHAR(64),
      event_ts TIMESTAMP,
      subscriber_id VARCHAR(50),
      tariff_code VARCHAR(50),
      service_code VARCHAR(50),
      cell_id VARCHAR(50),
      call_duration_sec INTEGER,
      traffic_mb NUMERIC(18,4),
      units NUMERIC(18,4),
      revenue_amount NUMERIC(18,4)
    );

    CREATE TEMP TABLE tmp_billing(
      billing_id VARCHAR(64),
      op_ts TIMESTAMP,
      subscriber_id VARCHAR(50),
      tariff_code VARCHAR(50),
      amount NUMERIC(18,4),
      charge_type VARCHAR(50),
      description VARCHAR(500)
    );

    CREATE TEMP TABLE tmp_payments(
      payment_id VARCHAR(64),
      payment_ts TIMESTAMP,
      subscriber_id VARCHAR(50),
      channel_code VARCHAR(50),
      amount NUMERIC(18,4),
      payment_method VARCHAR(50),
      status VARCHAR(30)
    );

    CREATE TEMP TABLE tmp_network_kpi(
      kpi_id VARCHAR(64),
      kpi_ts TIMESTAMP,
      cell_id VARCHAR(50),
      traffic_mb NUMERIC(18,4),
      call_attempts BIGINT,
      call_successes BIGINT,
      call_drops BIGINT
    );
    """)


def truncate_core(cur):
    # Очищаем основные таблицы DWH перед новой загрузкой.
    # CASCADE нужен, чтобы не возникало ошибки внешних ключей (сначала очищаются факты и зависимые таблицы).
    # RESTART IDENTITY сбрасывает автонумерацию суррогатных ключей.
    cur.execute(
        "TRUNCATE TABLE "
        "fact_network_kpi, fact_payment, fact_billing, fact_usage, "
        "dim_channel, dim_cell_site, dim_service, dim_tariff, dim_subscriber, dim_geo, dim_time, dim_date "
        "RESTART IDENTITY CASCADE;"
    )


def fill_dim_date_time(cur):
    # Определяем диапазон дат (min_date..max_date) по всем staging-таблицам.
    # Это нужно для корректного заполнения dim_date и dim_time.
    cur.execute("""
      SELECT MIN(min_d)::date, MAX(max_d)::date
      FROM (
        SELECT MIN(event_ts)::date AS min_d, MAX(event_ts)::date AS max_d FROM tmp_usage
        UNION ALL SELECT MIN(op_ts)::date, MAX(op_ts)::date FROM tmp_billing
        UNION ALL SELECT MIN(payment_ts)::date, MAX(payment_ts)::date FROM tmp_payments
        UNION ALL SELECT MIN(kpi_ts)::date, MAX(kpi_ts)::date FROM tmp_network_kpi
        UNION ALL SELECT MIN(activation_date)::date, MAX(activation_date)::date FROM tmp_subscribers
        UNION ALL SELECT MIN(deactivation_date)::date, MAX(deactivation_date)::date FROM tmp_subscribers
      ) t;
    """)
    min_date, max_date = cur.fetchone()

    if not min_date or not max_date:
        min_date = datetime.date(2024, 1, 1)
        max_date = datetime.date(2026, 12, 31)

    # Заполняем dim_date (календарь) на каждый день в диапазоне.
    # date_key формируется в формате YYYYMMDD.
    cur.execute("""
      WITH RECURSIVE d AS (
        SELECT %s::date AS full_date
        UNION ALL
        SELECT (full_date + INTERVAL '1 day')::date
        FROM d
        WHERE full_date < %s::date
      )
      INSERT INTO dim_date(date_key, full_date, year, quarter, month, day, is_month_start, is_month_end, is_weekend)
      SELECT
        (EXTRACT(YEAR FROM full_date)::int*10000 + EXTRACT(MONTH FROM full_date)::int*100 + EXTRACT(DAY FROM full_date)::int) AS date_key,
        full_date,
        EXTRACT(YEAR FROM full_date)::int AS year,
        EXTRACT(QUARTER FROM full_date)::int AS quarter,
        EXTRACT(MONTH FROM full_date)::int AS month,
        EXTRACT(DAY FROM full_date)::int AS day,
        (DATE_TRUNC('month', full_date)::date = full_date) AS is_month_start,
        ((DATE_TRUNC('month', full_date) + INTERVAL '1 month - 1 day')::date = full_date) AS is_month_end,
        (EXTRACT(ISODOW FROM full_date)::int IN (6,7)) AS is_weekend
      FROM d;
    """, (min_date, max_date))

    # Заполняем dim_time на основе времён, которые реально встречаются в событиях.
    # Для usage/billing/payments берём минуты (date_trunc('minute')),
    # для network_kpi — часы (date_trunc('hour')).
    # time_key формируется как HHMMSS.
    cur.execute("""
      INSERT INTO dim_time(time_key, full_time, hour, minute, second)
      SELECT DISTINCT
        (EXTRACT(HOUR FROM t)::int*10000 + EXTRACT(MINUTE FROM t)::int*100 + EXTRACT(SECOND FROM t)::int) AS time_key,
        t AS full_time,
        EXTRACT(HOUR FROM t)::int AS hour,
        EXTRACT(MINUTE FROM t)::int AS minute,
        EXTRACT(SECOND FROM t)::int AS second
      FROM (
        SELECT date_trunc('minute', event_ts)::time AS t FROM tmp_usage
        UNION ALL SELECT date_trunc('minute', op_ts)::time FROM tmp_billing
        UNION ALL SELECT date_trunc('minute', payment_ts)::time FROM tmp_payments
        UNION ALL SELECT date_trunc('hour', kpi_ts)::time FROM tmp_network_kpi
      ) x;
    """)


def load_dims(cur):
    # Загружаем измерения (dim_*) из staging-таблиц (tmp_*)

    # 1) География: собираем уникальные (country, region, city) из абонентов
    cur.execute("""
      INSERT INTO dim_geo(country, region, city)
      SELECT DISTINCT x.country, x.region, x.city
      FROM (
        SELECT country, region, city FROM tmp_subscribers
        UNION ALL
        SELECT country, region, city FROM tmp_cell_sites
      ) x
      WHERE x.country IS NOT NULL AND x.country <> ''
        AND NOT EXISTS (
          SELECT 1 FROM dim_geo g
          WHERE g.country = x.country
            AND COALESCE(g.region,'') = COALESCE(x.region,'')
            AND COALESCE(g.city,'') = COALESCE(x.city,'')
        );
    """)

    # 2) Абоненты: маппим geo_key по географии; при совпадении subscriber_id обновляем атрибуты
    cur.execute("""
      INSERT INTO dim_subscriber(subscriber_id, msisdn, customer_type, segment, status, activation_date, deactivation_date, geo_key)
      SELECT
        s.subscriber_id, s.msisdn, s.customer_type, s.segment, s.status, s.activation_date, s.deactivation_date,
        g.geo_key
      FROM tmp_subscribers s
      LEFT JOIN dim_geo g
        ON g.country = s.country
       AND COALESCE(g.region,'') = COALESCE(s.region,'')
       AND COALESCE(g.city,'') = COALESCE(s.city,'')
      WHERE s.subscriber_id IS NOT NULL AND s.subscriber_id <> ''
      ON CONFLICT (subscriber_id) DO UPDATE
      SET msisdn = EXCLUDED.msisdn,
          customer_type = EXCLUDED.customer_type,
          segment = EXCLUDED.segment,
          status = EXCLUDED.status,
          activation_date = EXCLUDED.activation_date,
          deactivation_date = EXCLUDED.deactivation_date,
          geo_key = EXCLUDED.geo_key;
    """)

    # 3) Тарифы: обновляем справочник по бизнес-ключу tariff_code
    cur.execute("""
      INSERT INTO dim_tariff(tariff_code, tariff_name, tariff_type, is_active, valid_from, valid_to)
      SELECT tariff_code, tariff_name, tariff_type, is_active, valid_from, valid_to
      FROM tmp_tariffs
      WHERE tariff_code IS NOT NULL AND tariff_code <> ''
      ON CONFLICT (tariff_code) DO UPDATE
      SET tariff_name = EXCLUDED.tariff_name,
          tariff_type = EXCLUDED.tariff_type,
          is_active = EXCLUDED.is_active,
          valid_from = EXCLUDED.valid_from,
          valid_to = EXCLUDED.valid_to;
    """)

    # 4) Услуги: обновляем справочник по service_code
    cur.execute("""
      INSERT INTO dim_service(service_code, service_name, service_group, is_recurring)
      SELECT service_code, service_name, service_group, is_recurring
      FROM tmp_services
      WHERE service_code IS NOT NULL AND service_code <> ''
      ON CONFLICT (service_code) DO UPDATE
      SET service_name = EXCLUDED.service_name,
          service_group = EXCLUDED.service_group,
          is_recurring = EXCLUDED.is_recurring;
    """)

    # 5) Каналы оплаты: обновляем справочник по channel_code
    cur.execute("""
      INSERT INTO dim_channel(channel_code, channel_name, channel_type)
      SELECT channel_code, channel_name, channel_type
      FROM tmp_channels
      WHERE channel_code IS NOT NULL AND channel_code <> ''
      ON CONFLICT (channel_code) DO UPDATE
      SET channel_name = EXCLUDED.channel_name,
          channel_type = EXCLUDED.channel_type;
    """)

    # 6) Соты/сайты: маппим geo_key и обновляем по cell_id
    cur.execute("""
      INSERT INTO dim_cell_site(cell_id, geo_key, technology, site_name)
      SELECT
        c.cell_id,
        g.geo_key,
        c.technology,
        c.site_name
      FROM tmp_cell_sites c
      LEFT JOIN dim_geo g
        ON g.country = c.country
       AND COALESCE(g.region,'') = COALESCE(c.region,'')
       AND COALESCE(g.city,'') = COALESCE(c.city,'')
      WHERE c.cell_id IS NOT NULL AND c.cell_id <> ''
      ON CONFLICT (cell_id) DO UPDATE
      SET geo_key = EXCLUDED.geo_key,
          technology = EXCLUDED.technology,
          site_name = EXCLUDED.site_name;
    """)


def load_facts(cur):
    # Загружаем фактовые таблицы (fact_*) с подстановкой суррогатных ключей из измерений

    # 1) fact_usage: события потребления услуг (CDR/usage)
    cur.execute("""
      INSERT INTO fact_usage(date_key, time_key, tariff_key, subscriber_key, service_key, cell_key,
                             call_duration_sec, traffic_mb, units, revenue_amount)
      SELECT
        dd.date_key,
        dt.time_key,
        t.tariff_key,
        s.subscriber_key,
        sv.service_key,
        cs.cell_key,
        COALESCE(u.call_duration_sec,0),
        COALESCE(u.traffic_mb,0),
        COALESCE(u.units,0),
        COALESCE(u.revenue_amount,0)
      FROM tmp_usage u
      JOIN dim_date dd ON dd.full_date = u.event_ts::date
      JOIN dim_time dt ON dt.full_time = date_trunc('minute', u.event_ts)::time
      JOIN dim_subscriber s ON s.subscriber_id = u.subscriber_id
      LEFT JOIN dim_tariff t ON t.tariff_code = u.tariff_code
      JOIN dim_service sv ON sv.service_code = u.service_code
      LEFT JOIN dim_cell_site cs ON cs.cell_id = u.cell_id;
    """)

    # 2) fact_billing: начисления/скидки/корректировки
    cur.execute("""
      INSERT INTO fact_billing(tariff_key, date_key, subscriber_key, amount, charge_type, description)
      SELECT
        t.tariff_key,
        dd.date_key,
        s.subscriber_key,
        b.amount,
        b.charge_type,
        b.description
      FROM tmp_billing b
      JOIN dim_date dd ON dd.full_date = b.op_ts::date
      JOIN dim_subscriber s ON s.subscriber_id = b.subscriber_id
      LEFT JOIN dim_tariff t ON t.tariff_code = b.tariff_code;
    """)

    # 3) fact_payment: платежи абонентов
    cur.execute("""
      INSERT INTO fact_payment(subscriber_key, date_key, channel_key, amount, payment_method, status)
      SELECT
        s.subscriber_key,
        dd.date_key,
        ch.channel_key,
        p.amount,
        p.payment_method,
        p.status
      FROM tmp_payments p
      JOIN dim_date dd ON dd.full_date = p.payment_ts::date
      JOIN dim_subscriber s ON s.subscriber_id = p.subscriber_id
      LEFT JOIN dim_channel ch ON ch.channel_code = p.channel_code;
    """)

    # 4) fact_network_kpi: сетевые KPI по сотам/времени + вычисление процентных показателей
    cur.execute("""
      INSERT INTO fact_network_kpi(date_key, time_key, cell_key, traffic_mb, call_attempts, call_successes, call_drops, success_ratio, drop_ratio)
      SELECT
        dd.date_key,
        dt.time_key,
        cs.cell_key,
        COALESCE(nk.traffic_mb,0),
        COALESCE(nk.call_attempts,0),
        COALESCE(nk.call_successes,0),
        COALESCE(nk.call_drops,0),
        CASE WHEN COALESCE(nk.call_attempts,0) > 0 THEN ROUND(100.0 * nk.call_successes / nk.call_attempts, 2) ELSE NULL END,
        CASE WHEN COALESCE(nk.call_attempts,0) > 0 THEN ROUND(100.0 * nk.call_drops / nk.call_attempts, 2) ELSE NULL END
      FROM tmp_network_kpi nk
      JOIN dim_date dd ON dd.full_date = nk.kpi_ts::date
      JOIN dim_time dt ON dt.full_time = date_trunc('hour', nk.kpi_ts)::time
      JOIN dim_cell_site cs ON cs.cell_id = nk.cell_id;
    """)


def main():
    # Основной сценарий ETL
    conn = get_conn()
    conn.autocommit = False  # управляем транзакциями
    cur = conn.cursor()
    try:
        # 1) Создаём таблицы DWH (если они ещё не созданы)
        exec_file(cur, BASE_DIR / "Core_tables.sql")
        conn.commit()

        # 2) Очищаем DWH-таблицы перед новой загрузкой
        truncate_core(cur)
        conn.commit()

        # 3) Создаём staging (временные) таблицы
        create_temp_tables(cur)

        # Вспомогательная функция загрузки одного CSV в одну таблицу tmp_*
        def load(table, filename, cols):
            path = CSV_DIR / filename
            if not path.exists():
                raise FileNotFoundError(f"CSV файл не найден: {path}")
            copy_csv(cur, table, path, cols)

        # 4) Загружаем CSV в staging-таблицы (tmp_*)
        load("tmp_tariffs", "tariffs.csv", ["tariff_code","tariff_name","tariff_type","is_active","valid_from","valid_to"])
        load("tmp_services", "services.csv", ["service_code","service_name","service_group","is_recurring"])
        load("tmp_channels", "channels.csv", ["channel_code","channel_name","channel_type"])
        load("tmp_cell_sites", "cell_sites.csv", ["cell_id","country","region","city","technology","site_name"])
        load("tmp_subscribers", "subscribers.csv", ["subscriber_id","msisdn","customer_type","segment","status","activation_date","deactivation_date","country","region","city"])
        load("tmp_usage", "usage.csv", ["event_id","event_ts","subscriber_id","tariff_code","service_code","cell_id","call_duration_sec","traffic_mb","units","revenue_amount"])
        load("tmp_billing", "billing.csv", ["billing_id","op_ts","subscriber_id","tariff_code","amount","charge_type","description"])
        load("tmp_payments", "payments.csv", ["payment_id","payment_ts","subscriber_id","channel_code","amount","payment_method","status"])
        load("tmp_network_kpi", "network_kpi.csv", ["kpi_id","kpi_ts","cell_id","traffic_mb","call_attempts","call_successes","call_drops"])
        conn.commit()

        # 5) Заполняем календарь и время на основе диапазона дат в staging
        fill_dim_date_time(cur)

        # 6) Загружаем измерения (dim_*)
        load_dims(cur)

        # 7) Загружаем факты (fact_*)
        load_facts(cur)
        conn.commit()

        # 8) Создаём представления (витрины) для BI
        exec_file(cur, BASE_DIR / "Bi_views.sql")
        conn.commit()

        # 9) Контрольный вывод: считаем строки в фактах
        cur.execute("SELECT COUNT(*) FROM fact_usage;")
        fu = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_billing;")
        fb = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_payment;")
        fp = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_network_kpi;")
        fn = cur.fetchone()[0]

        print("ETL успешно завершён.")
        print("fact_usage:", fu, "fact_billing:", fb, "fact_payment:", fp, "fact_network_kpi:", fn)

    finally:
        # Закрываем курсор и соединение
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Точка входа при запуске скрипта как самостоятельной программы
    main()
