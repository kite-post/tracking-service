package ua.kite.servicetemplate.events.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import ua.kite.servicetemplate.KafkaProbe;
import ua.kite.servicetemplate.TestcontainersConfiguration;
import ua.kite.servicetemplate.common.UuidV7;

/**
 * Ідемпотентний споживач: повтор пропускається, битий запис іде в DLT одразу,
 * а невдала обробка — після ретраїв і без позначки в processed_event.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, InboxIT.Listener.class})
class InboxIT {

    static final String TOPIC = "inbox-it.events";
    static final String CONSUMER = "inbox-it";
    static final String DLT = "dlt.service-template";
    static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    Listener listener;

    @Test
    void povtorPodiiObrobliaietsiaOdynRaz() throws Exception {
        String key = UUID.randomUUID().toString();
        UUID eventId = UuidV7.next();
        String event = event(eventId, key);

        send(key, event);
        send(key, event);
        // Третій запис з іншим id — маркер, що попередні вже пройшли через споживача.
        UUID marker = UuidV7.next();
        send(key, event(marker, key));

        await().atMost(WAIT).until(() -> listener.handled(marker) == 1);
        assertThat(listener.handled(eventId)).isEqualTo(1);
        assertThat(processed(eventId)).isEqualTo(1);
    }

    @Test
    void bytyiZapysIdeVDltBezRetraiv() {
        String key = UUID.randomUUID().toString();

        send(key, "{ це не json");

        List<ConsumerRecord<String, String>> dead = KafkaProbe.await(DLT, key, 1, WAIT);
        assertThat(dead).hasSize(1);
        assertThat(dead.getFirst().value()).isEqualTo("{ це не json");
        assertThat(header(dead.getFirst(), "kafka_dlt-original-topic")).isEqualTo(TOPIC);
        assertThat(header(dead.getFirst(), "kafka_dlt-exception-message")).contains("CloudEvents");
    }

    @Test
    void obrobnykPadaie_retraiPotimDlt_pozmitkyNemaie() throws Exception {
        String key = UUID.randomUUID().toString();
        UUID eventId = UuidV7.next();
        listener.failFor(eventId);

        send(key, event(eventId, key));

        assertThat(KafkaProbe.await(DLT, key, 1, WAIT)).hasSize(1);
        // Перша спроба + три повтори (EventsConfiguration).
        assertThat(listener.attempts(eventId)).isEqualTo(4);
        // Транзакція відкотилась — подію можна буде переграти з DLT.
        assertThat(processed(eventId)).isZero();
    }

    private void send(String key, String value) {
        kafka.send(new ProducerRecord<>(TOPIC, key, value)).join();
    }

    private String event(UUID id, String key) throws Exception {
        return mapper.writeValueAsString(Map.of(
                "specversion", "1.0",
                "id", id.toString(),
                "source", "/kite/inbox-it",
                "type", "kite.test.happened.v1",
                "time", "2026-11-20T10:00:00Z",
                "kitepartitionkey", key,
                "data", Map.of("key", key)));
    }

    private int processed(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_event WHERE consumer = :consumer AND event_id = :id")
                .param("consumer", CONSUMER)
                .param("id", eventId)
                .query(Integer.class)
                .single();
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    /** Споживач так, як його пише сервіс: усе тіло — всередині Inbox.handleOnce. */
    @TestConfiguration(proxyBeanMethods = false)
    static class Listener {

        private final Map<UUID, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final Map<UUID, AtomicInteger> handled = new ConcurrentHashMap<>();
        private final Map<UUID, Boolean> failing = new ConcurrentHashMap<>();

        @Autowired
        Inbox inbox;

        @Bean
        org.apache.kafka.clients.admin.NewTopic inboxItTopic() {
            return org.springframework.kafka.config.TopicBuilder.name(TOPIC)
                    .partitions(1)
                    .build();
        }

        @KafkaListener(id = CONSUMER, groupId = CONSUMER, topics = TOPIC)
        void on(ConsumerRecord<String, String> record) {
            inbox.handleOnce(CONSUMER, record, event -> {
                attempts.computeIfAbsent(event.id(), id -> new AtomicInteger()).incrementAndGet();
                if (failing.containsKey(event.id())) {
                    throw new IllegalStateException("обробник упав");
                }
                handled.computeIfAbsent(event.id(), id -> new AtomicInteger()).incrementAndGet();
            });
        }

        void failFor(UUID id) {
            failing.put(id, true);
        }

        int attempts(UUID id) {
            return attempts.getOrDefault(id, new AtomicInteger()).get();
        }

        int handled(UUID id) {
            return handled.getOrDefault(id, new AtomicInteger()).get();
        }
    }
}
