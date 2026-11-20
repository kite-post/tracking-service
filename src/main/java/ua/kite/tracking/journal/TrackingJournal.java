package ua.kite.tracking.journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Запис у журнал {@code tracking_event} — лише додавання (8.1). Оновлювати й
 * видаляти рядки не дає тригер у базі; виправлення — новою подією KT90.
 *
 * <p>Викликається всередині транзакції {@code Inbox.handleOnce}, тож рядок і
 * позначка в {@code processed_event} комітяться разом.
 */
@Component
public class TrackingJournal {

    private static final Logger log = LoggerFactory.getLogger(TrackingJournal.class);

    private final JdbcClient jdbc;
    private final TrackingPartitions partitions;

    public TrackingJournal(JdbcClient jdbc, TrackingPartitions partitions) {
        this.jdbc = jdbc;
        this.partitions = partitions;
    }

    /**
     * Дописує подію. Повертає {@code false}, якщо вона вже є в журналі —
     * повтор, який пройшов повз {@code processed_event} (наприклад, позначку
     * вже прибрав ретеншен, а топік переграли).
     */
    public boolean append(JournalEntry e) {
        partitions.ensureFor(e.occurredAt());
        boolean inserted = jdbc.sql("""
                        INSERT INTO tracking_event (event_id, shipment_id, tracking_number, event_code,
                                                    status_after, location_id, occurred_at, received_at,
                                                    source, event_type, upu_code, details)
                        VALUES (:eventId, :shipmentId, :trackingNumber, :eventCode,
                                :statusAfter, :locationId, :occurredAt, :receivedAt,
                                :source, :eventType, :upuCode, CAST(:details AS jsonb))
                        ON CONFLICT (event_id, occurred_at) DO NOTHING
                        """)
                        .param("eventId", e.eventId())
                        .param("shipmentId", e.shipmentId())
                        .param("trackingNumber", e.trackingNumber())
                        .param("eventCode", e.eventCode())
                        .param(
                                "statusAfter",
                                e.statusAfter() == null ? null : e.statusAfter().getValue())
                        .param("locationId", e.locationId())
                        .param("occurredAt", e.occurredAt())
                        .param("receivedAt", e.receivedAt())
                        .param("source", e.source())
                        .param("eventType", e.eventType())
                        .param("upuCode", e.upuCode())
                        .param(
                                "details",
                                e.details() == null ? "{}" : e.details().toString())
                        .update()
                == 1;
        if (!inserted) {
            log.info("Подія {} ({}) уже є в журналі, пропускаю", e.eventId(), e.eventType());
        }
        return inserted;
    }
}
