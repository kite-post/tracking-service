package ua.kite.tracking.events.outbox;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import ua.kite.tracking.events.EventEnvelope;
import ua.kite.tracking.events.EventsProperties;

/**
 * Публікатор outbox: забирає неопубліковані події в порядку {@code seq} і
 * шле їх у Kafka (5.4, п. 2).
 *
 * <p><b>Один активний публікатор на сервіс.</b> Прохід іде в транзакції під
 * {@code pg_try_advisory_xact_lock}: репліка, яка не взяла лок, нічого не
 * робить. Так події однієї сутності не обганяють одна одну, навіть коли
 * реплік кілька (8.3, п. 4). Лок знімається сам із кінцем транзакції — і коли
 * процес падає.
 *
 * <p><b>Порядок при збоях.</b> Пачка відправляється без очікування між
 * записами, а підтвердження читаються по черзі. Опублікованими позначаються
 * лише події до першої невдалої; решта піде наступним проходом. Продюсер
 * ідемпотентний ({@code enable.idempotence}), тож у межах розділу пізніший
 * запис не ляже раніше за той, що не дійшов.
 *
 * <p><b>Гарантія — щонайменше раз.</b> Якщо Kafka прийняла подію, а позначка
 * в базі не встигла закомітитись, подія піде вдруге. Тому кожен споживач
 * ідемпотентний — {@code Inbox}.
 */
@Component
@ConditionalOnProperty(name = "kite.events.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final EventsProperties.Outbox props;
    private final Counter published;
    private final Counter failures;

    public OutboxPublisher(
            JdbcClient jdbc,
            KafkaTemplate<String, String> kafka,
            PlatformTransactionManager txManager,
            EventsProperties properties,
            MeterRegistry meters) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.props = properties.outbox();
        this.tx = new TransactionTemplate(txManager);
        // Транзакція тримає лок, поки Kafka підтверджує пачку; з запасом.
        this.tx.setTimeout((int) props.sendTimeout().multipliedBy(2).toSeconds());
        this.published = Counter.builder("kite.outbox.published")
                .description("Події, які outbox відправив у Kafka")
                .register(meters);
        this.failures = Counter.builder("kite.outbox.publish.failures")
                .description("Невдалі спроби відправити подію з outbox")
                .register(meters);
    }

    @Scheduled(fixedDelayString = "${kite.events.outbox.poll-interval}")
    public void tick() {
        try {
            // Повна пачка — у черзі, найімовірніше, є ще: розгрібаємо без паузи.
            while (publishBatch() == props.batchSize()) {
                // далі
            }
        } catch (RuntimeException e) {
            log.warn("Прохід outbox не вдався, спробую за {}: {}", props.pollInterval(), e.toString());
        }
    }

    /** Один прохід. Повертає, скільки подій опубліковано; менше за пачку — пауза. */
    int publishBatch() {
        Integer done = tx.execute(status -> {
            if (!tryLock()) {
                return 0;
            }
            List<Row> rows = jdbc.sql("""
                            SELECT id, topic, partition_key, payload::text AS payload
                              FROM outbox
                             WHERE published_at IS NULL
                             ORDER BY seq
                             LIMIT :limit
                            """)
                    .param("limit", props.batchSize())
                    .query(Row.class)
                    .list();
            if (rows.isEmpty()) {
                return 0;
            }
            int sent = send(rows);
            // Неповна пачка через збій — сигнал зупинитись до наступного тіку.
            return sent == rows.size() ? sent : -1;
        });
        return done == null ? 0 : done;
    }

    private boolean tryLock() {
        // Лок на конкретну таблицю outbox: у кожного сервіса своя база, але
        // схема в ключі не дає двом outbox в одній базі блокувати один одного.
        return Boolean.TRUE.equals(jdbc.sql("SELECT pg_try_advisory_xact_lock(hashtext(current_schema() || '.outbox'))")
                .query(Boolean.class)
                .single());
    }

    private int send(List<Row> rows) {
        List<CompletableFuture<SendResult<String, String>>> inFlight = new ArrayList<>(rows.size());
        for (Row row : rows) {
            inFlight.add(kafka.send(toRecord(row)));
        }

        long deadline = System.nanoTime() + props.sendTimeout().toNanos();
        List<UUID> ok = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            try {
                inFlight.get(i).get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                ok.add(row.id());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                markFailed(row, e);
                break;
            } catch (ExecutionException | TimeoutException e) {
                markFailed(row, e instanceof ExecutionException ? e.getCause() : e);
                break;
            }
        }

        if (!ok.isEmpty()) {
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                    .param("ids", ok)
                    .update();
            published.increment(ok.size());
        }
        return ok.size();
    }

    private void markFailed(Row row, Throwable error) {
        failures.increment();
        // Подія лишається в черзі й блокує ті, що за нею: порядок важливіший.
        // Вік найстарішої неопублікованої видно в метриці kite.outbox.oldest.age.
        log.warn("Подію {} не вдалося відправити в {}: {}", row.id(), row.topic(), error.toString());
        jdbc.sql("UPDATE outbox SET attempts = attempts + 1, last_error = :error WHERE id = :id")
                .param("error", String.valueOf(error))
                .param("id", row.id())
                .update();
    }

    private static ProducerRecord<String, String> toRecord(Row row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.partitionKey(), row.payload());
        record.headers()
                .add(new RecordHeader(
                        "content-type", EventEnvelope.KAFKA_CONTENT_TYPE.getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    record Row(UUID id, String topic, String partitionKey, String payload) {}
}
