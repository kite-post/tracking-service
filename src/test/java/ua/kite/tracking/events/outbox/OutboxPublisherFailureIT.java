package ua.kite.tracking.events.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

import ua.kite.tracking.TestcontainersConfiguration;
import ua.kite.tracking.events.TestEvents;

/**
 * Публікатор, коли Kafka відмовляє, і коли публікаторів кілька. Kafka тут —
 * мок, а проходи публікатора викликаються вручну: розклад відсунутий на годину.
 *
 * <p>Своя схема бази: контексти інших тестів живуть у кеші Spring паралельно,
 * і їхні публікатори забрали б наші події з outbox у public.
 */
@SpringBootTest(
        properties = {
            "kite.events.outbox.poll-interval=1h",
            "spring.datasource.hikari.schema=publisher_it",
            "spring.flyway.default-schema=publisher_it",
            "spring.flyway.schemas=publisher_it"
        })
@Import(TestcontainersConfiguration.class)
class OutboxPublisherFailureIT {

    @MockitoBean
    KafkaTemplate<String, String> kafka;

    @Autowired
    OutboxPublisher publisher;

    @Autowired
    Outbox outbox;

    @Autowired
    TransactionTemplate tx;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    DataSource dataSource;

    @Autowired
    ObjectMapper mapper;

    @BeforeEach
    void emptyOutbox() {
        jdbc.sql("DELETE FROM outbox").update();
    }

    @Test
    void zbiiZupyniaePachkuINastupniPodiiNeObhaniaiut() throws Exception {
        UUID shipmentId = TestEvents.newShipmentId();
        UUID first = append(shipmentId);
        UUID second = append(shipmentId);
        UUID third = append(shipmentId);

        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(ok())
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("брокер недоступний")))
                .thenReturn(ok());

        publisher.publishBatch();

        assertThat(isPublished(first)).isTrue();
        assertThat(isPublished(second)).isFalse();
        assertThat(isPublished(third)).isFalse(); // хоч Kafka його й прийняла б
        assertThat(attempts(second)).isEqualTo(1);
        assertThat(lastError(second)).contains("брокер недоступний");

        // Kafka ожила: наступний прохід продовжує з тієї самої події.
        clearInvocations(kafka);
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(invocation -> ok());

        publisher.publishBatch();

        assertThat(isPublished(second)).isTrue();
        assertThat(isPublished(third)).isTrue();
        assertThat(sentIds()).containsExactly(second.toString(), third.toString());
    }

    @Test
    void pokyLokUInshoiReplikyTsiaNichohoNeShle() throws Exception {
        append(TestEvents.newShipmentId());
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(invocation -> ok());

        // Інша «репліка» тримає лок публікатора.
        try (Connection other = dataSource.getConnection();
                Statement statement = other.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(hashtext(current_schema() || '.outbox'))");

            assertThat(publisher.publishBatch()).isZero();

            statement.execute("SELECT pg_advisory_unlock(hashtext(current_schema() || '.outbox'))");
        }

        assertThat(publisher.publishBatch()).isEqualTo(1);
    }

    private UUID append(UUID shipmentId) {
        return tx.execute(status -> outbox.append(TestEvents.statusChanged(shipmentId)));
    }

    private boolean isPublished(UUID id) {
        return jdbc.sql("SELECT published_at IS NOT NULL FROM outbox WHERE id = :id")
                .param("id", id)
                .query(Boolean.class)
                .single();
    }

    private int attempts(UUID id) {
        return jdbc.sql("SELECT attempts FROM outbox WHERE id = :id")
                .param("id", id)
                .query(Integer.class)
                .single();
    }

    private String lastError(UUID id) {
        return jdbc.sql("SELECT last_error FROM outbox WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    @SuppressWarnings("unchecked")
    private List<String> sentIds() throws Exception {
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka, atLeastOnce()).send(captor.capture());
        List<String> ids = new ArrayList<>();
        for (ProducerRecord<String, String> record : captor.getAllValues()) {
            ids.add(mapper.readTree(record.value()).path("id").asText());
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, String>> ok() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }
}
