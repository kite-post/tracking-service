package ua.kite.servicetemplate.events.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import ua.kite.contracts.model.ShipmentStatus;
import ua.kite.contracts.model.ShipmentStatusChangedData;
import ua.kite.servicetemplate.KafkaProbe;
import ua.kite.servicetemplate.TestcontainersConfiguration;
import ua.kite.servicetemplate.events.EventEnvelope;
import ua.kite.servicetemplate.events.TestEvents;

/** Outbox від транзакції до запису в Kafka — на справжніх Postgres і Kafka. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OutboxIT {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Autowired
    Outbox outbox;

    @Autowired
    TransactionTemplate tx;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper mapper;

    @Test
    void zakomichenaPodiiaDokhodytDoKafkaUKonvertiCloudEvents() throws Exception {
        UUID shipmentId = TestEvents.newShipmentId();

        UUID eventId = tx.execute(status -> outbox.append(TestEvents.statusChanged(shipmentId)));

        List<ConsumerRecord<String, String>> records =
                KafkaProbe.await("shipment.events", shipmentId.toString(), 1, WAIT);
        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.getFirst();

        assertThat(new String(record.headers().lastHeader("content-type").value(), StandardCharsets.UTF_8))
                .startsWith("application/cloudevents+json");

        EventEnvelope event = mapper.readValue(record.value(), EventEnvelope.class);
        assertThat(event.specversion()).isEqualTo("1.0");
        assertThat(event.id()).isEqualTo(eventId);
        assertThat(event.id().version()).isEqualTo(7);
        assertThat(event.source()).isEqualTo("/kite/service-template");
        assertThat(event.type()).isEqualTo("kite.shipment.status-changed.v1");
        assertThat(event.subject()).isEqualTo("KT123456785UA");
        assertThat(event.kitepartitionkey()).isEqualTo(shipmentId.toString());
        assertThat(event.time()).isNotNull();
        assertThat(event.kitereceivedat()).isNotNull();

        // data читається класом із kite-contracts.
        ShipmentStatusChangedData data = event.dataAs(ShipmentStatusChangedData.class, mapper);
        assertThat(data.getStatusAfter()).isEqualTo(ShipmentStatus.READY_FOR_PICKUP);

        await().atMost(WAIT)
                .untilAsserted(() -> assertThat(publishedAt(eventId)).isNotNull());
    }

    @Test
    void vidkatTranzaktsiiNeLyshaePodii() {
        UUID shipmentId = TestEvents.newShipmentId();
        List<UUID> ids = new ArrayList<>();

        tx.executeWithoutResult(status -> {
            ids.add(outbox.append(TestEvents.statusChanged(shipmentId)));
            status.setRollbackOnly(); // ніби бізнес-зміна впала після запису події
        });

        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE id = :id")
                        .param("id", ids.getFirst())
                        .query(Integer.class)
                        .single())
                .isZero();
        assertThat(KafkaProbe.await("shipment.events", shipmentId.toString(), 1, Duration.ofSeconds(3)))
                .isEmpty();
    }

    @Test
    void pozaTranzaktsiieiuPodiiaNePyshetsia() {
        assertThatThrownBy(() -> outbox.append(TestEvents.statusChanged(TestEvents.newShipmentId())))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void podiiOdnohoVidpravlenniaIdutUPoriadkuZapysu() throws Exception {
        UUID shipmentId = TestEvents.newShipmentId();
        List<UUID> written = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            written.add(tx.execute(status -> outbox.append(TestEvents.statusChanged(shipmentId))));
        }

        List<ConsumerRecord<String, String>> records =
                KafkaProbe.await("shipment.events", shipmentId.toString(), written.size(), WAIT);

        List<UUID> received = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            received.add(mapper.readValue(record.value(), EventEnvelope.class).id());
        }
        assertThat(received).containsExactlyElementsOf(written);
        assertThat(records)
                .extracting(ConsumerRecord::partition)
                .containsOnly(records.getFirst().partition());
    }

    @Test
    void chasPodiiMozhnaZadatyVidokremlenoVidChasuZapysu() throws Exception {
        UUID shipmentId = TestEvents.newShipmentId();
        OffsetDateTime scannedAt = OffsetDateTime.parse("2026-11-20T06:30:00Z");

        tx.execute(status -> outbox.append(TestEvents.statusChanged(shipmentId).occurredAt(scannedAt)));

        EventEnvelope event = mapper.readValue(
                KafkaProbe.await("shipment.events", shipmentId.toString(), 1, WAIT)
                        .getFirst()
                        .value(),
                EventEnvelope.class);
        assertThat(event.time()).isEqualTo(scannedAt);
        assertThat(event.kitereceivedat()).isAfter(scannedAt);
    }

    private OffsetDateTime publishedAt(UUID id) {
        return jdbc.sql("SELECT published_at FROM outbox WHERE id = :id")
                .param("id", id)
                .query(OffsetDateTime.class)
                .single();
    }
}
