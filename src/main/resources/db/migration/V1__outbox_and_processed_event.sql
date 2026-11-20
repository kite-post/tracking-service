-- Транспорт подій, однаковий у кожній базі Kite (6.1). Таблиці сервіса
-- починаються з V2.

-- Вихідні події (5.4, п. 2). Рядок пишеться в одній транзакції зі зміною,
-- публікатор відправляє його в Kafka і проставляє published_at.
-- Це транспорт, а не архів (8.1): опубліковане видаляється за ретеншеном.
CREATE TABLE outbox
(
    id            uuid PRIMARY KEY,                 -- він же id конверта CloudEvents
    -- Порядок публікації. Не id: UUID v7 генерує застосунок, і з двох реплік
    -- із розбіжним годинником пізніша подія може отримати менший id.
    -- Послідовність видає база, тож для двох змін однієї сутності, які
    -- серіалізує блокування рядка, порядок seq збігається з порядком комітів.
    seq           bigint GENERATED ALWAYS AS IDENTITY,
    topic         text        NOT NULL,
    partition_key text        NOT NULL,
    type          text        NOT NULL,
    payload       jsonb       NOT NULL,             -- увесь конверт CloudEvents
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz,
    attempts      integer     NOT NULL DEFAULT 0,
    last_error    text
);

-- Черга публікатора: лише неопубліковані, у порядку seq.
CREATE INDEX outbox_pending_idx ON outbox (seq) WHERE published_at IS NULL;
-- Для прибирання за ретеншеном.
CREATE INDEX outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- Уже оброблені вхідні події (5.4, п. 3; 8.3, п. 3). Ключ — пара
-- «споживач + подія»: два обробники сервіса можуть по-різному реагувати
-- на ту саму подію, і кожен має обробити її рівно раз.
CREATE TABLE processed_event
(
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE INDEX processed_event_processed_at_idx ON processed_event (processed_at);
