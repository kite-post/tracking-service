-- Журнал подій відстеження (8.1; 8.3, п. 7). Єдине джерело історії переходів:
-- таблиця лише для додавання, з неї будуються всі проєкції читання.

CREATE TABLE tracking_event
(
    -- id події CloudEvents, з якої зроблено рядок. Для похідних подій мішка
    -- (8.3, п. 2) — виведений з пари «подія мішка + посилка», тож теж стабільний.
    -- На нього посилається анулювання KT90 (8.3, п. 6).
    event_id        uuid        NOT NULL,
    shipment_id     uuid        NOT NULL,
    tracking_number text        NOT NULL,
    event_code      text        NOT NULL CHECK (event_code ~ '^KT[0-9]{2}$'),
    -- Стан після події; NULL — подія стану не змінює (8.2, прочерк у колонці).
    status_after    text CHECK (status_after IN ('DRAFT', 'CREATED', 'CANCELLED', 'ACCEPTED', 'IN_TRANSIT',
                                                 'AT_HUB', 'SORTED', 'ON_HOLD', 'READY_FOR_PICKUP',
                                                 'DELIVERED', 'RETURNED', 'LOST')),
    location_id     uuid,
    -- Первинний час — момент самої події (`time` у конверті), тай-брейк —
    -- момент запису (`kitereceivedat`) (8.3, п. 4).
    occurred_at     timestamptz NOT NULL,
    received_at     timestamptz NOT NULL,
    source          text        NOT NULL,           -- `source` конверта: /kite/<сервіс>
    event_type      text        NOT NULL,           -- `type` конверта: kite.<домен>.<подія>.v<N>
    upu_code        text CHECK (upu_code IN ('EMA', 'EMG', 'EMI')),
    details         jsonb       NOT NULL,           -- `data` події як є
    -- Другий рубіж ідемпотентності після processed_event: та сама подія дає
    -- той самий occurred_at, тож потрапляє в ту саму партицію й упирається
    -- в цей ключ — навіть коли позначку в processed_event уже прибрав ретеншен.
    -- Унікальний ключ партиційованої таблиці мусить містити ключ партиції.
    PRIMARY KEY (event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- Таймлайн відправлення (6.3).
CREATE INDEX tracking_event_shipment_idx ON tracking_event (shipment_id, occurred_at);

-- «Записи не оновлюються і не видаляються» (8.1) — не домовленість, а правило
-- бази. Виправлення — тільки новою подією KT90 (8.3, п. 6). Партиції цілком
-- (DROP / DETACH) це не зачіпає.
CREATE FUNCTION tracking_event_append_only() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION 'tracking_event — журнал лише для додавання: % заборонено', TG_OP
        USING ERRCODE = 'restrict_violation';
END
$$;

CREATE TRIGGER tracking_event_append_only
    BEFORE UPDATE OR DELETE
    ON tracking_event
    FOR EACH ROW
EXECUTE FUNCTION tracking_event_append_only();

-- Партиція місяця, в який потрапляє ts (межі — за UTC): tracking_event_2026_11
-- для [2026-11-01, 2026-12-01). Створює, якщо її ще немає; повертає ім'я.
--
-- DEFAULT-партиції немає свідомо: щойно в ній опиниться рядок за якийсь
-- місяць, створити партицію цього місяця Postgres уже не дасть. Тому партицію
-- створює сервіс — наперед на кілька місяців і на льоту для події з місяця,
-- якого ще немає (офлайн-сканер, спізніла подія).
--
-- Кілька реплік можуть прийти за тією самою партицією одночасно; advisory-лок
-- до кінця транзакції серіалізує їх, а повторна перевірка після лока прибирає
-- гонку CREATE TABLE.
CREATE FUNCTION tracking_event_ensure_partition(ts timestamptz) RETURNS text
    LANGUAGE plpgsql AS
$$
DECLARE
    month_start timestamp := date_trunc('month', ts AT TIME ZONE 'UTC');
    part        text      := format('tracking_event_%s', to_char(month_start, 'YYYY_MM'));
BEGIN
    IF to_regclass(part) IS NOT NULL THEN
        RETURN part;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('tracking_event_partitions'));
    IF to_regclass(part) IS NULL THEN
        EXECUTE format('CREATE TABLE %I PARTITION OF tracking_event FOR VALUES FROM (%L) TO (%L)',
                       part,
                       month_start AT TIME ZONE 'UTC',
                       (month_start + interval '1 month') AT TIME ZONE 'UTC');
    END IF;
    RETURN part;
END
$$;
