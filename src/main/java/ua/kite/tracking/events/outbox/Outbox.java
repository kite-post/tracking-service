package ua.kite.tracking.events.outbox;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import ua.kite.tracking.common.UuidV7;
import ua.kite.tracking.events.EventEnvelope;
import ua.kite.tracking.events.EventRegistry;
import ua.kite.tracking.events.EventRegistry.Route;

/**
 * Запис події в outbox — у тій самій транзакції, що й зміна (5.4, п. 2).
 * Поза транзакцією метод падає: подія без зміни або зміна без події — саме
 * те, від чого outbox захищає.
 *
 * <pre>{@code
 * @Transactional
 * public void sort(UUID shipmentId) {
 *     Shipment shipment = shipments.findById(shipmentId).orElseThrow();
 *     shipment.markSorted();
 *     outbox.append(OutgoingEvent.of(KITE_SHIPMENT_STATUS_CHANGED_V1, toData(shipment))
 *             .subject(shipment.getTrackingNumber()));
 * }
 * }</pre>
 */
@Component
public class Outbox {

    /** Ключ MDC з X-Request-Id запиту (9.1) — їде в подію як {@code kiterequestid}. */
    public static final String REQUEST_ID_MDC_KEY = "requestId";

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;
    private final EventRegistry registry;
    private final Clock clock;
    private final String source;

    public Outbox(
            JdbcClient jdbc,
            ObjectMapper mapper,
            EventRegistry registry,
            Clock clock,
            @Value("${spring.application.name}") String service) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.registry = registry;
        this.clock = clock;
        this.source = "/kite/" + service;
    }

    /** Кладе подію в outbox і повертає її id — він же {@code id} конверта CloudEvents. */
    @Transactional(propagation = Propagation.MANDATORY)
    public UUID append(OutgoingEvent event) {
        Route route = registry.route(event.type());
        JsonNode data = mapper.valueToTree(event.data());
        String partitionKey = registry.partitionKey(event.type(), data);

        UUID id = UuidV7.next(clock);
        OffsetDateTime now = OffsetDateTime.now(clock);
        EventEnvelope envelope = new EventEnvelope(
                EventEnvelope.SPEC_VERSION,
                id,
                source,
                event.type().getValue(),
                event.subject(),
                event.occurredAt() != null ? event.occurredAt() : now,
                EventEnvelope.CONTENT_TYPE,
                null,
                now,
                MDC.get(REQUEST_ID_MDC_KEY),
                event.causationId(),
                partitionKey,
                data);

        jdbc.sql("""
                        INSERT INTO outbox (id, topic, partition_key, type, payload)
                        VALUES (:id, :topic, :key, :type, CAST(:payload AS jsonb))
                        """)
                .param("id", id)
                .param("topic", route.topic())
                .param("key", partitionKey)
                .param("type", envelope.type())
                .param("payload", toJson(envelope))
                .update();
        return id;
    }

    private String toJson(EventEnvelope envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Подію " + envelope.type() + " не вдалося серіалізувати", e);
        }
    }
}
