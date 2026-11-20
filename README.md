# tracking-service

Журнал подій відстеження, таймлайн, дашборди «мені / від мене», публічне
відстеження (5.1). Власник історії (8.1): споживає події з Kafka і дописує їх
у `tracking_event`, з якого будуються всі проєкції читання. У shipment-service
нічого не пише і стану не змінює. Створено з `template-java-service`.

## Що вже вміє

Каркас споживача — **лише журнал**, без проєкцій і REST:

- слухає `shipment.events` (`@KafkaListener` `tracking-journal`, група
  `tracking-service`) і дописує події в `tracking_event` (міграція `V2`);
- ідемпотентність у два рубежі (8.3, п. 3):
  1. `processed_event(consumer, event_id)` в одній транзакції із записом —
     повтор пропускається ще до мапінгу;
  2. первинний ключ журналу `(event_id, occurred_at)` + `ON CONFLICT DO NOTHING`
     — дубля немає, навіть коли позначку вже прибрав ретеншен (14 діб), а топік
     переграли. `occurred_at` береться з події, тож повтор потрапляє в ту саму
     партицію;
- партиції по місяцях за `occurred_at` (UTC), `tracking_event_2026_11`;
  створює їх функція бази `tracking_event_ensure_partition`: на старті й
  щоночі — поточний місяць і `kite.tracking.partitions.months-ahead` (3)
  наперед, а для події з місяця без партиції — на льоту перед записом.
  DEFAULT-партиції немає свідомо: з рядками в ній Postgres не дасть створити
  партицію цього місяця;
- журнал лише для додавання — `UPDATE` і `DELETE` відхиляє тригер бази (8.1);
- індекс `(shipment_id, occurred_at)` для таймлайну (6.3); `upu_code` для
  KT10 / KT30 / KT50 → EMA / EMG / EMI (8.2).

Що з `shipment.events` іде в журнал (одна подія — щонайбільше один рядок):

| Тип                  | Рядок                                                        |
| -------------------- | ------------------------------------------------------------ |
| `created`            | KT01 / CREATED — лише якщо не чернетка; DRAFT коду не має     |
| `confirmed`          | KT01 / CREATED (T1)                                          |
| `cancelled`          | KT03 / CANCELLED                                             |
| `status-changed`     | `eventCode`, `statusAfter`, `locationId` з події             |
| `redirected`         | KT60 до прибуття; після прибуття KT62 дає `status-changed`   |
| `recipient-changed`  | KT61                                                         |
| `hold-set`, `return-created`, решта | ні — KT32, KT70, KT71 приходять у `status-changed` |

Подія без `time`, `source`, `shipmentId` чи (для `status-changed`)
`eventCode` / `statusAfter` — бита, одразу в `dlt.tracking-service`.

Чого ще немає — свідомо, окремими задачами:

- розгортання `kite.scan.unit-scanned.v1` у події посилок (8.3, п. 2) і решта
  топіків (`scan`, `logistics`, `locker`);
- карантин за розходженням `occurred_at` і `received_at` понад 24 год і
  спізнілі події після кінцевого стану (8.3, п. 5);
- анулювання KT90 і перерахунок (8.3, п. 6);
- проєкції `shipment_view`, `party_shipment_index`, Redis, REST.

Відкрите питання до shipment-service: коли з'явиться `"draft": false`, на
один перехід T1 має йти або `created` зі статусом CREATED, або `confirmed`,
а не обидві — інакше в журналі буде два KT01.

Критерій «повторна доставка не створює дубля» перевіряє `TrackingJournalIT`
на справжніх Postgres і Kafka: та сама подія тричі через топік — один рядок;
повтор після видалення позначки з `processed_event` — теж один.

## Що всередині

```
pom.xml                   Spring Boot 3.5, Java 21, JPA, Flyway, Kafka, Spotless, Failsafe
src/main/java
  web/                    один ендпоїнт /api/v1/info
  journal/                споживач shipment.events, мапінг у рядок журналу, запис, партиції
  common/UuidV7           первинні ключі й id подій (6.1)
  events/                 конверт CloudEvents, реєстр подій, DLT, прибирання
  events/outbox/          запис в outbox, публікатор, метрики відставання
  events/inbox/           ідемпотентна обробка вхідних подій (processed_event)
src/main/resources
  application.yml         база, Kafka, actuator, Prometheus, kite.events.*, kite.tracking.*
  db/migration/V1__…      таблиці outbox і processed_event
  db/migration/V2__…      tracking_event (партиції), тригер append-only, функція партицій
src/test/java             юніт-тести + *IT на Postgres і Kafka у Testcontainers
Dockerfile                шарований образ Spring Boot на temurin-21-jre-alpine
Makefile                  той самий набір цілей, що й у решти репозиторіїв
.github/workflows/ci.yml  п'ять рядків: виклик спільного workflow із .github
```

## Команди

```bash
make build      # зібрати jar
make lint       # формат і імпорти (Spotless + palantir-java-format)
make fmt        # відформатувати
make test       # юніт-тести, без Docker
make test-it    # разом з інтеграційними (*IT): Postgres і Kafka контейнерами
make verify     # усе, що робить CI
make image      # зібрати образ локально
make run        # запустити локально з Postgres і Kafka у контейнерах
make run-stand  # запустити локально проти стенда kite-infra (localhost:5432, :9092)
make up         # підняти образ на 8080 з лімітом 384 МБ у мережі стенда
```

`make run` — це `spring-boot:test-run` з `TrackingDevApplication`: той
самий застосунок плюс `TestcontainersConfiguration`. З IDE — запустити `main`
цього класу.

## CI

Увесь пайплайн — виклик спільного workflow з
[kite-post/.github](https://github.com/kite-post/.github): лінт, тести,
збірка образу, публікація в `ghcr.io/kite-post/<сервіс>` і PR у `kite-infra`,
який піднімає версію у `versions.env` (12.6).

На pull request образ збирається, але не публікується. Теги: `sha-<хеш>` для
`main`, `v<semver>` для релізів; рухомого `latest` немає принципово.

## База

Своя база на сервіс (5.4, п. 1), міграції Flyway сервіс накочує сам на старті
(6.1, 12.3). Домовленості 6.1: ключі — UUID v7 із застосунку (`UuidV7.next()`),
гроші — `bigint` у копійках, час — `timestamptz` в UTC, переліки — `text` з
`CHECK`, у сутностей із конкурентними змінами — колонка `version`.

Hibernate схему не створює (`ddl-auto: validate`), лише звіряє з міграціями.
Локальні значення в `application.yml` дивляться на базу стенда; у контейнері
стенд перекриває їх через `SPRING_DATASOURCE_*`.

Readiness (`/actuator/health/readiness`) залежить від бази — на неї дивляться
`HEALTHCHECK` образу і `depends_on` стенда. Kafka в readiness свідомо не входить:
без брокера сервіс працює далі, а події чекають в outbox.

## Контракти

Моделі API, інтерфейси контролерів і класи подій CloudEvents приходять
залежністю `ua.kite:kite-contracts`, закріпленою за тегом — своїх копій
`openapi.yaml` і JSON Schema сервіс не тримає (5.4, п. 6):

```xml
<kite-contracts.version>0.2.1</kite-contracts.version>
```

```bash
make contracts            # підтягнути пакет за поточним тегом
make contracts V=0.3.0    # перейти на нову версію: Makefile + pom.xml
make contracts-check      # версія закріплена, копій схем немає, залежність резолвиться
```

Нова версія приходить сама: після релізу `kite-contracts` розсилає подію
всім репозиторіям із топіком `kite-contracts`, і workflow `contracts-bump`
відкриває PR з `make contracts V=<версія>`. Версія одна на всі репозиторії.
Руками те саме: `make contracts V=0.3.0`. Якщо тегу немає, `Makefile` і
маніфест лишаються як були.

`contracts-check` проганяє спільний CI. Самі схеми лежать у тому самому jar
(`/kite/contracts/openapi`, `/kite/contracts/events`), тож валідація події не
потребує копії файла в репозиторії — так це і перевіряє `ContractsWiringTest`.

Пакет живе в GitHub Packages, а він віддає Maven лише з токеном. У CI
`settings.xml` розставляє спільний workflow; локально потрібен свій:

```xml
<!-- ~/.m2/settings.xml -->
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>ВАШ-ЛОГІН</username>
      <password>ghp_… (scope read:packages)</password>
    </server>
  </servers>
</settings>
```

Порядок наскрізної зміни (5.6): спершу адитивна зміна в `kite-contracts` з
новим тегом, потім версія тут, і тільки потім продюсер починає слати нове
поле.

## Події

### Публікація: outbox (5.4, п. 2)

Подія пишеться в таблицю `outbox` **у тій самій транзакції**, що й зміна, тож
зміни без події чи події без зміни не буває. Поза транзакцією `append` падає.

```java
@Transactional
public void sort(UUID shipmentId) {
    Shipment shipment = shipments.findById(shipmentId).orElseThrow();
    shipment.markSorted();
    outbox.append(OutgoingEvent.of(KITE_SHIPMENT_STATUS_CHANGED_V1, data)
            .subject(shipment.getTrackingNumber())   // номер, код мішка, рейс
            .occurredAt(scan.occurredAt()));          // без нього — час запису
}
```

Конверт CloudEvents (9.7) складається сам: `id` (UUID v7), `source`
(`/kite/<сервіс>`), `time`, `kitereceivedat`, `kiterequestid` (з MDC
`requestId`), `kitepartitionkey`. Топік і поле ключа розділу беруться з
`events/registry.json` у jar `kite-contracts`: для `status-changed` це
`shipment.events` і `shipmentId`. Якщо поля ключа в `data` немає, `append` падає.

`OutboxPublisher` кожні 200 мс забирає неопубліковане в порядку `seq` і шле в
Kafka (`acks=all`, ідемпотентний продюсер):

- **Один активний публікатор на сервіс** — під `pg_try_advisory_xact_lock`.
  Репліки, які не взяли лок, чекають; так події однієї сутності не обганяють
  одна одну.
- **Збій зупиняє пачку.** Подія, яку Kafka не прийняла, лишається в черзі з
  `attempts` і `last_error`, а наступні не йдуть повз неї. Порядок важливіший.
- **Щонайменше раз.** Подія може піти вдруге (Kafka прийняла, позначка в базі
  не встигла), тому кожен споживач ідемпотентний.
- Опубліковане видаляється через `kite.events.outbox.retention` (7 діб):
  outbox — транспорт, а не архів (8.1).

Метрики: `kite_outbox_pending`, `kite_outbox_oldest_age_seconds` (алерт —
на вік), `kite_outbox_published_total`, `kite_outbox_publish_failures_total`.

### Споживання: processed_event (5.4, п. 3)

Усе тіло обробки — всередині `Inbox.handleOnce`. Позначка в `processed_event`
і зміни обробника комітяться разом: повтор пропускається, а якщо обробник
упав, відкочується і позначка.

```java
@KafkaListener(id = CONSUMER, idIsGroup = false, topics = "shipment.events")
void on(ConsumerRecord<String, String> record) {
    inbox.handleOnce(CONSUMER, record, event -> {
        if (!KITE_SHIPMENT_STATUS_CHANGED_V1.getValue().equals(event.type())) return;
        var data = event.dataAs(ShipmentStatusChangedData.class, mapper);
        // …
    });
}
```

- `CONSUMER` — стабільне ім'я обробника, ключ у `processed_event` разом з id
  події. Змінив ім'я — всі події оброблятимуться наново.
- Група Kafka — ім'я сервіса (`spring.kafka.consumer.group-id`).
- Виняток з обробника — три повтори (0,5 → 1 → 2 с), потім `dlt.<сервіс>`.
  Битий запис (не JSON, без `id` чи `type`) — у DLT одразу. Звідки прийшов
  запис і чому впав, лежить у заголовках `kafka_dlt-*`. Сервіс сам оголошує
  свій DLT-топік: автостворення на стенді вимкнене.
- `processed_event` чиститься через `kite.events.inbox.retention` (14 діб) —
  довше за ретеншен топіків, щоб старий повтор не пройшов як новий.

Робочий приклад — `journal/ShipmentEventsListener`.

### Тести

`OutboxIT`, `OutboxPublisherFailureIT`, `InboxIT` і `TrackingJournalIT` перевіряють на справжніх
Postgres і Kafka: подія доходить у конверті CloudEvents, відкат транзакції не
лишає події, порядок у межах ключа зберігається, збій Kafka не дає наступним
обігнати, другий публікатор чекає на лок, повтор обробляється один раз, битий
запис і обробник, що падає, потрапляють у DLT. Новий IT сервіса —
`@SpringBootTest` + `@Import(TestcontainersConfiguration.class)`.
