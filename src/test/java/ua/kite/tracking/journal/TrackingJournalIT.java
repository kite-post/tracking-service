package ua.kite.tracking.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import ua.kite.tracking.TestcontainersConfiguration;
import ua.kite.tracking.common.UuidV7;

/**
 * Критерій задачі: повторна доставка тієї самої події не створює дубля в
 * журналі. Події йдуть справжнім шляхом — через Kafka-топік
 * {@code shipment.events} у {@link ShipmentEventsListener}.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, TrackingJournalIT.Topics.class})
class TrackingJournalIT {

    static final String TOPIC = "shipment.events";
    static final Duration WAIT = Duration.ofSeconds(30);
    static final DateTimeFormatter PARTITION = DateTimeFormatter.ofPattern("'tracking_event_'yyyy_MM");

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper mapper;

    @Test
    void povtornaDostavka_odynRiadokUZhurnali() throws Exception {
        UUID shipment = UuidV7.next();
        UUID eventId = UuidV7.next();
        String event = statusChanged(eventId, shipment, "2026-11-20T10:00:00Z", "KT21", "AT_HUB");

        send(shipment, event);
        send(shipment, event);
        send(shipment, event);
        // Маркер з тим самим ключем: коли він у журналі, повтори вже пройшли через споживача.
        UUID marker = UuidV7.next();
        send(shipment, statusChanged(marker, shipment, "2026-11-20T11:00:00Z", "KT22", "SORTED"));

        await().atMost(WAIT).until(() -> rows(marker) == 1);
        assertThat(rows(eventId)).isEqualTo(1);
        assertThat(processed(eventId)).isEqualTo(1);
        assertThat(timeline(shipment)).isEqualTo(2);
    }

    @Test
    void povtorPisliaRetenshenuProcessedEvent_dubliaVseOdnoNemaie() throws Exception {
        UUID shipment = UuidV7.next();
        UUID eventId = UuidV7.next();
        String event = statusChanged(eventId, shipment, "2026-11-19T09:00:00Z", "KT10", "ACCEPTED");

        send(shipment, event);
        await().atMost(WAIT).until(() -> rows(eventId) == 1);

        // Як після EventsCleanup: позначки вже немає, а топік переграли.
        jdbc.sql("DELETE FROM processed_event WHERE consumer = :c AND event_id = :id")
                .param("c", ShipmentEventsListener.CONSUMER)
                .param("id", eventId)
                .update();
        send(shipment, event);
        UUID marker = UuidV7.next();
        send(shipment, statusChanged(marker, shipment, "2026-11-19T10:00:00Z", "KT20", "IN_TRANSIT"));

        await().atMost(WAIT).until(() -> rows(marker) == 1);
        assertThat(rows(eventId)).isEqualTo(1);
        // Повтор пройшов через обробник наново — і уперся в ключ журналу.
        assertThat(processed(eventId)).isEqualTo(1);
    }

    @Test
    void riadokLiahaieVPartytsiiuSvohoMisiatsia_vidsutniaStvoriuietsiaNaLotu() throws Exception {
        UUID shipment = UuidV7.next();
        UUID current = UuidV7.next();
        // Офлайн-сканер із давно минулим місяцем: такої партиції наперед ніхто не створював.
        UUID late = UuidV7.next();

        send(shipment, statusChanged(current, shipment, "2026-11-30T23:59:59Z", "KT30", "READY_FOR_PICKUP"));
        send(shipment, statusChanged(late, shipment, "2019-03-05T08:00:00+02:00", "KT50", "DELIVERED"));

        await().atMost(WAIT).until(() -> rows(late) == 1);
        assertThat(partitionOf(current)).isEqualTo("tracking_event_2026_11");
        assertThat(partitionOf(late)).isEqualTo("tracking_event_2019_03");
        assertThat(jdbc.sql("SELECT upu_code FROM tracking_event WHERE event_id = :id")
                        .param("id", late)
                        .query(String.class)
                        .single())
                .isEqualTo("EMI");
    }

    @Test
    void naStartiPartytsiiNaPeredStvoreno() {
        YearMonth now = YearMonth.now(ZoneOffset.UTC);
        for (int i = 0; i <= 3; i++) {
            String name = now.plusMonths(i).format(PARTITION);
            assertThat(jdbc.sql("SELECT to_regclass(:name) IS NOT NULL")
                            .param("name", name)
                            .query(Boolean.class)
                            .single())
                    .as(name)
                    .isTrue();
        }
    }

    @Test
    void chernetkaNePotrapliaieVZhurnal() throws Exception {
        UUID shipment = UuidV7.next();
        UUID draft = UuidV7.next();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("shipmentId", shipment.toString());
        data.put("trackingNumber", "KT123456785UA");
        data.put("type", "PARCEL");
        data.put("status", "DRAFT");
        data.put("draft", true);
        data.put("originLocationId", UuidV7.next().toString());
        data.put("destinationLocationId", UuidV7.next().toString());
        data.put("payer", "SENDER");
        data.put("parcelCount", 1);
        send(shipment, envelope(draft, shipment, "kite.shipment.created.v1", "2026-11-20T09:00:00Z", data));
        UUID marker = UuidV7.next();
        send(shipment, statusChanged(marker, shipment, "2026-11-20T09:30:00Z", "KT10", "ACCEPTED"));

        await().atMost(WAIT).until(() -> rows(marker) == 1);
        assertThat(rows(draft)).isZero();
        // Подію все одно позначено обробленою — повтор її не чіпатиме.
        assertThat(processed(draft)).isEqualTo(1);
    }

    @Test
    void zhurnalLysheDliaDodavannia() throws Exception {
        UUID shipment = UuidV7.next();
        UUID eventId = UuidV7.next();
        send(shipment, statusChanged(eventId, shipment, "2026-11-20T12:00:00Z", "KT21", "AT_HUB"));
        await().atMost(WAIT).until(() -> rows(eventId) == 1);

        assertThatThrownBy(() -> jdbc.sql("UPDATE tracking_event SET event_code = 'KT22' WHERE event_id = :id")
                        .param("id", eventId)
                        .update())
                .hasMessageContaining("лише для додавання");
        assertThatThrownBy(() -> jdbc.sql("DELETE FROM tracking_event WHERE event_id = :id")
                        .param("id", eventId)
                        .update())
                .hasMessageContaining("лише для додавання");
        assertThat(rows(eventId)).isEqualTo(1);
    }

    private void send(UUID shipment, String value) {
        kafka.send(new ProducerRecord<>(TOPIC, shipment.toString(), value)).join();
    }

    private String statusChanged(UUID id, UUID shipment, String time, String code, String statusAfter)
            throws Exception {
        return envelope(
                id,
                shipment,
                "kite.shipment.status-changed.v1",
                time,
                Map.of(
                        "shipmentId",
                        shipment.toString(),
                        "trackingNumber",
                        "KT123456785UA",
                        "statusBefore",
                        "CREATED",
                        "statusAfter",
                        statusAfter,
                        "eventCode",
                        code));
    }

    private String envelope(UUID id, UUID shipment, String type, String time, Map<String, Object> data)
            throws Exception {
        return mapper.writeValueAsString(Map.of(
                "specversion",
                "1.0",
                "id",
                id.toString(),
                "source",
                "/kite/shipment-service",
                "type",
                type,
                "subject",
                "KT123456785UA",
                "time",
                time,
                "kitereceivedat",
                "2026-11-20T12:00:00Z",
                "kitepartitionkey",
                shipment.toString(),
                "data",
                data));
    }

    private int rows(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM tracking_event WHERE event_id = :id")
                .param("id", eventId)
                .query(Integer.class)
                .single();
    }

    private int timeline(UUID shipment) {
        return jdbc.sql("SELECT count(*) FROM tracking_event WHERE shipment_id = :id")
                .param("id", shipment)
                .query(Integer.class)
                .single();
    }

    private int processed(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM processed_event WHERE consumer = :c AND event_id = :id")
                .param("c", ShipmentEventsListener.CONSUMER)
                .param("id", eventId)
                .query(Integer.class)
                .single();
    }

    private String partitionOf(UUID eventId) {
        return jdbc.sql("SELECT tableoid::regclass::text FROM tracking_event WHERE event_id = :id")
                .param("id", eventId)
                .query(String.class)
                .single();
    }

    /** На стенді топік створює kite-infra; у тесті — сам тест. */
    @TestConfiguration(proxyBeanMethods = false)
    static class Topics {

        @Bean
        NewTopic shipmentEventsTopic() {
            return TopicBuilder.name(TOPIC).partitions(3).build();
        }
    }
}
