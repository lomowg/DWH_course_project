import csv
import random
import string
import datetime
from pathlib import Path
from collections import defaultdict



# Фиксируем seed, чтобы при каждом запуске получались одинаковые данные
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Диапазон данных, который хотим покрыть в датасете
DATA_START = datetime.date(2024, 1, 1)
DATA_END = datetime.date(2026, 12, 31)

# Объёмы данных
N_CELLS = 500
N_SUBSCRIBERS = 8000
N_USAGE_EVENTS = 550_000
N_PAYMENTS = 120_000
N_NETWORK_KPI = 180_000

# Папка, куда будут записаны CSV-файлы
OUT_DIR = Path(__file__).resolve().parent / "data_out"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def rand_id(prefix: str, n: int = 10) -> str:
    # Генерирует случайный идентификатор: PREFIX + (n символов A-Z0-9)
    return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def rand_msisdn() -> str:
    # Генерирует псевдо-номер телефона (MSISDN) в формате 79XXXXXXXXX
    return "79" + "".join(random.choices(string.digits, k=9))


def choice_weighted(items):
    # Выбор элемента с учётом весов
    # items = [(value, weight), ...]
    total = sum(w for _, w in items)
    r = random.uniform(0, total)
    upto = 0
    for v, w in items:
        if upto + w >= r:
            return v
        upto += w
    return items[-1][0]


def parse_date(x):
    # Универсальный парсер даты:
    # если пусто - None
    # если уже datetime.date - вернуть как есть
    # если строка ISO (YYYY-MM-DD) - распарсить
    if not x:
        return None
    if isinstance(x, datetime.date):
        return x
    return datetime.date.fromisoformat(x)


def write_csv(name, header, rows):
    # Записывает CSV в OUT_DIR с заданным заголовком и строками
    # header — список названий колонок
    # rows — список списков (строки данных)
    path = OUT_DIR / name
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"Wrote {path} ({len(rows)} rows)")


def month_iter(start_month: datetime.date, end_month: datetime.date):
    # Генератор первых чисел месяцев от start_month до end_month включительно
    cur = datetime.date(start_month.year, start_month.month, 1)
    end = datetime.date(end_month.year, end_month.month, 1)
    while cur <= end:
        yield cur
        if cur.month == 12:
            cur = datetime.date(cur.year + 1, 1, 1)
        else:
            cur = datetime.date(cur.year, cur.month + 1, 1)


# Веса по часам — для реалистичных суточных пиков (пиковая активность вечером)
HOUR_WEIGHTS = [
    (0, 1), (1, 1), (2, 1), (3, 1), (4, 1),
    (5, 2), (6, 3), (7, 4), (8, 5), (9, 5),
    (10, 4), (11, 4), (12, 4), (13, 4), (14, 4),
    (15, 5), (16, 6), (17, 7), (18, 9), (19, 10),
    (20, 10), (21, 9), (22, 6), (23, 3),
]


def weighted_hour():
    # Возвращает час суток с учётом весов (пиковая активность вечером)
    return choice_weighted([(h, w) for h, w in HOUR_WEIGHTS])


# Сезонность по месяцам (пример: декабрь выше из-за праздников, лето выше и т.д.)
MONTH_WEIGHTS = {
    1: 0.85, 2: 0.90, 3: 1.00, 4: 0.95, 5: 1.05, 6: 1.10,
    7: 1.15, 8: 1.20, 9: 1.05, 10: 1.10, 11: 1.15, 12: 1.35
}

# Тренд по годам: 2024 ниже, 2025 база, 2026 выше (рост бизнеса/трафика)
YEAR_TREND = {2024: 0.85, 2025: 1.00, 2026: 1.20}


def time_factor(ts: datetime.datetime) -> float:
    # Мультипликатор времени: сезонность (месяц) * тренд (год)
    return MONTH_WEIGHTS.get(ts.month, 1.0) * YEAR_TREND.get(ts.year, 1.0)


# География
COUNTRY = "Russia"
REGIONS = [
    ("Moscow", ["Moscow"]),
    ("Saint Petersburg", ["Saint Petersburg"]),
    ("Novosibirsk Oblast", ["Novosibirsk"]),
    ("Sverdlovsk Oblast", ["Yekaterinburg"]),
    ("Krasnodar Krai", ["Krasnodar"]),
]

# Сегменты, типы клиентов и статусы абонентов
SEGMENTS = [("Mass", 60), ("Youth", 15), ("Premium", 18), ("Business", 7)]
CUST_TYPES = [("B2C_prepaid", 50), ("B2C_postpaid", 40), ("B2B", 10)]
SUB_STATUS = [("ACTIVE", 90), ("SUSPENDED", 7), ("BLOCKED", 3)]

# Тарифы (справочник)
TARIFFS = [
    ("T01", "Smart Start", "data_oriented"),
    ("T02", "Voice Plus", "voice"),
    ("T03", "Family Pack", "convergent"),
    ("T04", "Unlimited 4G", "data_oriented"),
    ("T05", "Premium Max", "premium"),
    ("T06", "Business Pro", "b2b"),
    ("T07", "Student", "youth"),
    ("T08", "Roaming Lite", "addon"),
]

# Тарифные коэффициенты для расчёта "выручки" по услугам (data/voice/sms)
TARIFF_PRICING = {
    "T01": {"data_mb": (0.006, 0.012), "voice_min": (0.40, 0.90), "sms": (0.20, 0.40)},
    "T02": {"data_mb": (0.008, 0.015), "voice_min": (0.60, 1.40), "sms": (0.20, 0.45)},
    "T03": {"data_mb": (0.006, 0.013), "voice_min": (0.45, 1.10), "sms": (0.18, 0.40)},
    "T04": {"data_mb": (0.003, 0.008), "voice_min": (0.35, 0.80), "sms": (0.15, 0.35)},
    "T05": {"data_mb": (0.010, 0.020), "voice_min": (0.80, 1.80), "sms": (0.25, 0.55)},
    "T06": {"data_mb": (0.009, 0.018), "voice_min": (0.90, 2.00), "sms": (0.25, 0.60)},
    "T07": {"data_mb": (0.004, 0.010), "voice_min": (0.30, 0.70), "sms": (0.15, 0.30)},
    "T08": {"data_mb": (0.012, 0.030), "voice_min": (1.10, 2.50), "sms": (0.35, 0.80)},
}

# Справочник услуг
SERVICES = [
    ("VOICE", "Voice calls", "voice", False),
    ("SMS", "SMS messaging", "sms", False),
    ("DATA", "Mobile Internet", "data", False),
]

# Справочник каналов оплаты
CHANNELS = [
    ("CH_ONLINE", "Online Banking", "online"),
    ("CH_APP", "Mobile App", "online"),
    ("CH_RETAIL", "Retail Store", "offline"),
    ("CH_TERMINAL", "Payment Terminal", "offline"),
    ("CH_PARTNER", "Partner Network", "partner"),
]

# Услуги по сегментам
SEGMENT_SERVICE_MIX = {
    "Mass":     [("DATA", 52), ("VOICE", 36), ("SMS", 12)],
    "Youth":    [("DATA", 78), ("VOICE", 14), ("SMS", 8)],
    "Premium":  [("DATA", 58), ("VOICE", 32), ("SMS", 10)],
    "Business": [("DATA", 42), ("VOICE", 48), ("SMS", 10)],
}

# Интенсивность потребления по сегментам (для объёма событий/трафика/выручки)
SEGMENT_INTENSITY = {"Mass": 1.0, "Youth": 1.2, "Premium": 1.7, "Business": 2.0}

# Вероятностное распределение тарифов по сегментам
SEGMENT_TARIFFS = {
    "Youth":    [("T07", 55), ("T01", 25), ("T04", 15), ("T08", 5)],
    "Mass":     [("T01", 32), ("T03", 30), ("T04", 28), ("T02", 7), ("T08", 3)],
    "Premium":  [("T05", 62), ("T04", 18), ("T03", 15), ("T08", 5)],
    "Business": [("T06", 78), ("T02", 12), ("T05", 5), ("T08", 5)],
}


def gen_tariffs():
    # Генерирует tariffs.csv (справочник тарифов)
    rows = []
    for code, name, ttype in TARIFFS:
        valid_from = datetime.date(2023, 1, 1)
        valid_to = ""
        rows.append([code, name, ttype, True, valid_from.isoformat(), valid_to])
    write_csv(
        "tariffs.csv",
        ["tariff_code","tariff_name","tariff_type","is_active","valid_from","valid_to"],
        rows
    )


def gen_services():
    # Генерирует services.csv (справочник услуг)
    rows = [[c, n, g, rec] for c, n, g, rec in SERVICES]
    write_csv(
        "services.csv",
        ["service_code","service_name","service_group","is_recurring"],
        rows
    )


def gen_channels():
    # Генерирует channels.csv (справочник каналов оплаты)
    rows = [[c, n, t] for c, n, t in CHANNELS]
    write_csv(
        "channels.csv",
        ["channel_code","channel_name","channel_type"],
        rows
    )


def gen_cells(n_cells=N_CELLS):
    # Генерирует cell_sites.csv (справочник сот/базовых станций)
    # Используем разные доли 3G/4G/5G в разных регионах
    tech_by_region = {
        "Moscow": [("5G", 32), ("4G", 58), ("3G", 10)],
        "Saint Petersburg": [("5G", 22), ("4G", 63), ("3G", 15)],
        "Novosibirsk Oblast": [("5G", 10), ("4G", 70), ("3G", 20)],
        "Sverdlovsk Oblast": [("5G", 8), ("4G", 72), ("3G", 20)],
        "Krasnodar Krai": [("5G", 6), ("4G", 74), ("3G", 20)],
    }

    rows = []
    for i in range(1, n_cells + 1):
        region, cities = random.choice(REGIONS)
        city = random.choice(cities)
        tech = choice_weighted(tech_by_region[region])
        cell_id = f"CELL_{i:05d}"
        site_name = f"Site {region[:3].upper()}-{i:05d}"
        rows.append([cell_id, COUNTRY, region, city, tech, site_name])

    write_csv(
        "cell_sites.csv",
        ["cell_id","country","region","city","technology","site_name"],
        rows
    )

    # region_cells: список сот по каждому региону (для реалистичной привязки событий usage)
    # cell_tech: технология по каждой соте (для генерации сетевых KPI)
    region_cells = defaultdict(list)
    cell_tech = {}
    for cell_id, _, region, _, tech, _ in rows:
        region_cells[region].append(cell_id)
        cell_tech[cell_id] = tech

    # Возвращаем:
    # 1) список всех cell_id
    # 2) словарь {region: [cell_id, ...]}
    # 3) словарь {cell_id: tech}
    return [r[0] for r in rows], region_cells, cell_tech


def gen_subscribers(n=N_SUBSCRIBERS):
    # Генерирует subscribers.csv и одновременно формирует "profiles" для генерации событий
    rows = []
    sub_ids = []
    profiles = {}

    start = datetime.date(2023, 7, 1)
    end = DATA_END
    total_days = (end - start).days

    for i in range(1, n + 1):
        subscriber_id = f"SUB_{i:07d}"
        msisdn = rand_msisdn()

        segment = choice_weighted(SEGMENTS)

        # Для бизнес-сегмента выше шанс B2B
        if segment == "Business":
            customer_type = choice_weighted([("B2B", 85), ("B2C_postpaid", 15)])
        else:
            customer_type = choice_weighted(CUST_TYPES)

        status = choice_weighted(SUB_STATUS)

        # Случайная дата активации
        act = start + datetime.timedelta(days=random.randint(0, total_days))

        # Вероятность churn зависит от сегмента (у Youth выше, у Business ниже)
        churn_prob = {"Mass": 0.18, "Youth": 0.24, "Premium": 0.12, "Business": 0.08}[segment]
        deact = ""
        deact_dt = None
        if random.random() < churn_prob:
            d = act + datetime.timedelta(days=random.randint(60, 520))
            if d <= end:
                deact_dt = d
                deact = d.isoformat()

        # География абонента
        region, cities = random.choice(REGIONS)
        city = random.choice(cities)

        # Тариф выбираем с учётом сегмента
        tariff = choice_weighted(SEGMENT_TARIFFS[segment])

        rows.append([
            subscriber_id, msisdn, customer_type, segment, status,
            act.isoformat(), deact,
            COUNTRY, region, city
        ])

        # Сохраняем профиль для генерации фактов (usage/billing/payments)
        sub_ids.append(subscriber_id)
        profiles[subscriber_id] = {
            "segment": segment,
            "customer_type": customer_type,
            "status": status,
            "region": region,
            "city": city,
            "act": act,
            "deact": deact_dt,
            "tariff": tariff
        }

    write_csv(
        "subscribers.csv",
        ["subscriber_id","msisdn","customer_type","segment","status","activation_date","deactivation_date","country","region","city"],
        rows
    )
    return sub_ids, profiles


def is_active(profile, ts: datetime.datetime) -> bool:
    # Проверка: активен ли абонент на дату события
    d = ts.date()
    if d < profile["act"]:
        return False
    if profile["deact"] and d > profile["deact"]:
        return False
    return True


def pick_weighted_date():
    # Выбор даты с учётом распределения по годам и сезонности по месяцам.
    # День ограничен 1..28, чтобы избежать проблем с разным числом дней в месяце.
    year = choice_weighted([(2024, 25), (2025, 33), (2026, 42)])
    month = choice_weighted([(m, int(MONTH_WEIGHTS[m] * 100)) for m in range(1, 13)])
    day = random.randint(1, 28)
    return datetime.date(year, month, day)


def gen_usage(sub_ids, profiles, region_cells, n_events=N_USAGE_EVENTS):
    # Генерирует usage.csv (события потребления: VOICE/SMS/DATA)
    rows = []

    # Взвешиваем абонентов по интенсивности сегмента (Business/Premium дадут больше событий)
    weighted_subs = [(sid, SEGMENT_INTENSITY[profiles[sid]["segment"]]) for sid in sub_ids]

    for _ in range(n_events):
        event_id = rand_id("U_", 14)

        # Формируем дату/время события с сезонностью + суточными пиками
        d = pick_weighted_date()
        h = weighted_hour()
        minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        ts = datetime.datetime(d.year, d.month, d.day, h, minute, 0)

        # Выбираем абонента с учётом интенсивности
        sub = choice_weighted(weighted_subs)
        prof = profiles[sub]

        # Если событие выпадает на неактивного абонента
        if not is_active(prof, ts):
            if random.random() < 0.75:
                continue

        segment = prof["segment"]
        tariff = prof["tariff"]
        service = choice_weighted(SEGMENT_SERVICE_MIX[segment])

        # Привязка к соте
        if random.random() < 0.8 and region_cells[prof["region"]]:
            cell = random.choice(region_cells[prof["region"]])
        else:
            any_region = random.choice(list(region_cells.keys()))
            cell = random.choice(region_cells[any_region])

        # Мультипликатор интенсивности: сегмент * сезонность * тренд
        intensity = SEGMENT_INTENSITY[segment] * time_factor(ts)

        # Тарифные ставки
        pricing = TARIFF_PRICING[tariff]

        # Генерация показателей и выручки зависит от типа услуги
        if service == "VOICE":
            base = random.randint(20, 600)
            if segment in ("Business", "Premium"):
                base = int(base * random.uniform(1.2, 1.9))
            duration = min(base, 1800)
            traffic_mb = 0
            units = 1
            voice_rate = random.uniform(*pricing["voice_min"])
            revenue = round((duration / 60) * voice_rate * intensity, 4)

        elif service == "SMS":
            duration = 0
            traffic_mb = 0
            units = random.choice([1, 1, 2, 2, 3, 5 if segment == "Business" else 2])
            sms_rate = random.uniform(*pricing["sms"])
            revenue = round(units * sms_rate * (0.9 + 0.25 * random.random()), 4)

        else:
            duration = 0

            # База трафика
            base_mb = random.expovariate(1/80) + random.uniform(0, 12)

            # Усиливаем трафик для некоторых сегментов
            if segment == "Youth":
                base_mb *= random.uniform(1.3, 1.9)
            elif segment == "Premium":
                base_mb *= random.uniform(1.4, 2.2)
            elif segment == "Business":
                base_mb *= random.uniform(1.2, 2.0)

            # Пики нагрузки вечером и спад ночью
            if 18 <= h <= 23:
                base_mb *= random.uniform(1.15, 1.6)
            if 0 <= h <= 5:
                base_mb *= random.uniform(0.6, 0.85)

            traffic_mb = round(base_mb * intensity, 4)
            units = traffic_mb
            data_rate = random.uniform(*pricing["data_mb"])

            # Пример промо-эффекта: в апреле 2025 цена ниже (или скидка)
            promo = 0.85 if (ts.year == 2025 and ts.month == 4) else 1.0
            revenue = round(traffic_mb * data_rate * promo, 4)

        rows.append([
            event_id, ts.isoformat(sep=" "), sub, tariff, service, cell,
            duration, traffic_mb, units, revenue
        ])

    write_csv(
        "usage.csv",
        ["event_id","event_ts","subscriber_id","tariff_code","service_code","cell_id","call_duration_sec","traffic_mb","units","revenue_amount"],
        rows
    )


def gen_billing(sub_ids, profiles):
    # Генерирует billing.csv (начисления: monthly_fee + скидки + корректировки)
    rows = []

    # Базовые диапазоны абонплаты по тарифам
    tariff_fee = {
        "T01": (300, 550),
        "T02": (350, 650),
        "T03": (450, 850),
        "T04": (500, 900),
        "T05": (900, 1700),
        "T06": (1200, 2600),
        "T07": (250, 520),
        "T08": (200, 420),
    }

    # Проходим по каждому месяцу и начисляем активным абонентам платежи/скидки/корректировки
    for m in month_iter(datetime.date(2024, 1, 1), datetime.date(2026, 12, 1)):
        for sid in sub_ids:
            prof = profiles[sid]
            if not is_active(prof, datetime.datetime(m.year, m.month, 1, 0, 0, 0)):
                continue

            tariff = prof["tariff"]
            seg = prof["segment"]

            billing_id = rand_id("B_", 14)
            ts = datetime.datetime(m.year, m.month, random.randint(1, 5), random.randint(0, 23), 0, 0)

            # Месячная абонплата зависит от тарифа, сегмента и годового тренда
            base_fee = random.uniform(*tariff_fee[tariff])
            seg_mult = {"Mass": 1.0, "Youth": 0.9, "Premium": 1.4, "Business": 1.7}[seg]
            amt = round(base_fee * seg_mult * YEAR_TREND.get(m.year, 1.0), 4)

            rows.append([billing_id, ts.isoformat(sep=" "), sid, tariff, amt, "monthly_fee", "Monthly subscription fee"])

            # Вероятность скидки по сегментам + сезонное усиление летом
            disc_prob = {"Mass": 0.12, "Youth": 0.18, "Premium": 0.06, "Business": 0.03}[seg]
            if m.month in (6, 7, 8):
                disc_prob *= 1.25
            if random.random() < disc_prob:
                rows.append([
                    rand_id("B_", 14), ts.isoformat(sep=" "), sid, tariff,
                    round(-random.uniform(30, 280), 4),
                    "discount", "Promotional discount"
                ])

            # Небольшая вероятность корректировки
            if random.random() < 0.05:
                rows.append([
                    rand_id("B_", 14), ts.isoformat(sep=" "), sid, tariff,
                    round(random.uniform(-150, 150), 4),
                    "adjustment", "Billing adjustment"
                ])

    write_csv(
        "billing.csv",
        ["billing_id","op_ts","subscriber_id","tariff_code","amount","charge_type","description"],
        rows
    )


def gen_payments(sub_ids, profiles, n_rows=N_PAYMENTS):
    # Генерирует payments.csv (платежи абонентов)
    rows = []

    # Случайное распределение по всей временной шкале 2024-2026
    start_ts = datetime.datetime(2024, 1, 1)
    end_ts = datetime.datetime(2026, 12, 31, 23, 59, 0)
    seconds_range = int((end_ts - start_ts).total_seconds())

    methods = [("card", 52), ("bank_transfer", 18), ("cash", 10), ("e_wallet", 20)]
    statuses = [("SUCCESS", 95), ("FAILED", 5)]
    channel_codes = [c[0] for c in CHANNELS]

    for _ in range(n_rows):
        pid = rand_id("P_", 14)
        ts = start_ts + datetime.timedelta(seconds=random.randint(0, seconds_range))
        ts = ts.replace(second=0, microsecond=0)

        sid = random.choice(sub_ids)
        prof = profiles[sid]
        seg = prof["segment"]
        ctype = prof["customer_type"]

        channel = random.choice(channel_codes)
        method = choice_weighted(methods)
        status = choice_weighted(statuses)

        # Суммы платежей зависят от сегмента (Business платит больше, Youth меньше)
        if seg == "Business":
            base = random.choice([1500, 2000, 3000, 5000, 8000])
        elif seg == "Premium":
            base = random.choice([800, 1200, 1500, 2000, 3000])
        elif seg == "Youth":
            base = random.choice([200, 300, 500, 800, 1000])
        else:
            base = random.choice([300, 500, 800, 1000, 1500])

        # Для prepaid чаще маленькие пополнения
        if "prepaid" in ctype and random.random() < 0.6:
            base = random.choice([100, 200, 300, 500])

        amount = round(base * random.uniform(0.85, 1.20) * YEAR_TREND.get(ts.year, 1.0), 4)

        rows.append([pid, ts.isoformat(sep=" "), sid, channel, amount, method, status])

    write_csv(
        "payments.csv",
        ["payment_id","payment_ts","subscriber_id","channel_code","amount","payment_method","status"],
        rows
    )


def gen_network_kpi(cell_ids, cell_tech, n_rows=N_NETWORK_KPI):
    # Генерирует network_kpi.csv (сетевые KPI по соте и часу)
    rows = []

    start_ts = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end_ts = datetime.datetime(2026, 12, 31, 23, 0, 0)
    hours_range = int((end_ts - start_ts).total_seconds() // 3600)

    # Формируем набор "проблемных" сот и временные окна аварий (outage), чтобы были аномалии на графиках
    outage_cells = set(random.sample(cell_ids, k=max(12, len(cell_ids)//45)))
    outage_windows = []
    for c in outage_cells:
        start_h = random.randint(0, hours_range - 96)
        outage_windows.append((c, start_h, start_h + random.randint(8, 48)))

    # Параметры качества для разных технологий: 3G хуже, 5G лучше
    tech_quality = {
        "3G": {"succ": (0.90, 0.98), "traffic": (180, 800)},
        "4G": {"succ": (0.94, 0.995), "traffic": (350, 1300)},
        "5G": {"succ": (0.96, 0.998), "traffic": (550, 2000)},
    }

    for _ in range(n_rows):
        kid = rand_id("K_", 14)
        hour_idx = random.randint(0, hours_range)
        ts = start_ts + datetime.timedelta(hours=hour_idx)
        ts = ts.replace(minute=0, second=0, microsecond=0)

        cell = random.choice(cell_ids)
        tech = cell_tech[cell]

        # Суточные пики: вечером выше попыток и трафика, ночью ниже
        peak_mult = 1.0
        if 18 <= ts.hour <= 23:
            peak_mult = random.uniform(1.15, 1.55)
        elif 0 <= ts.hour <= 5:
            peak_mult = random.uniform(0.55, 0.85)

        # Попытки вызовов растут с годом (trend) и с пиками (peak_mult)
        attempts = int(random.randint(90, 950) * peak_mult * YEAR_TREND.get(ts.year, 1.0))

        # Базовый процент успешности зависит от технологии
        succ_low, succ_high = tech_quality[tech]["succ"]
        succ_rate = random.uniform(succ_low, succ_high)

        # Если попали в окно аварии — ухудшаем успешность
        in_outage = any((cell == c and s <= hour_idx <= e) for c, s, e in outage_windows)
        if in_outage:
            succ_rate *= random.uniform(0.65, 0.88)

        successes = int(attempts * succ_rate)
        drops = max(0, int((attempts - successes) * random.uniform(0.35, 0.95)))

        # Трафик зависит от технологии + сезонности + тренда + суточного пика
        tr_low, tr_high = tech_quality[tech]["traffic"]
        traffic_mb = random.uniform(tr_low, tr_high) * peak_mult
        traffic_mb *= MONTH_WEIGHTS.get(ts.month, 1.0) * YEAR_TREND.get(ts.year, 1.0)
        traffic_mb = round(traffic_mb, 4)

        rows.append([kid, ts.isoformat(sep=" "), cell, traffic_mb, attempts, successes, drops])

    write_csv(
        "network_kpi.csv",
        ["kpi_id","kpi_ts","cell_id","traffic_mb","call_attempts","call_successes","call_drops"],
        rows
    )


def main():
    # Генерируем справочники
    gen_tariffs()
    gen_services()
    gen_channels()

    # Генерируем соты/ячейки и вспомогательные структуры для последующей генерации фактов
    cell_ids, region_cells, cell_tech = gen_cells(n_cells=N_CELLS)

    # Генерируем абонентов и профили, по которым далее будут генерироваться события
    sub_ids, profiles = gen_subscribers(n=N_SUBSCRIBERS)

    # Генерируем факт usage (CDR/интернет-сессии), начисления, платежи и сетевые KPI
    gen_usage(sub_ids, profiles, region_cells, n_events=N_USAGE_EVENTS)
    gen_billing(sub_ids, profiles)
    gen_payments(sub_ids, profiles, n_rows=N_PAYMENTS)
    gen_network_kpi(cell_ids, cell_tech, n_rows=N_NETWORK_KPI)

    # Итоговое сообщение о расположении созданных файлов
    print("\nДанные созданы. Лежат в", OUT_DIR)


if __name__ == "__main__":
    # Точка входа при запуске скрипта напрямую
    main()
