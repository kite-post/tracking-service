package ua.kite.tracking.journal;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import ua.kite.contracts.model.KiteEventType;
import ua.kite.contracts.model.ShipmentCancelledData;
import ua.kite.contracts.model.ShipmentConfirmedData;
import ua.kite.contracts.model.ShipmentCreatedData;
import ua.kite.contracts.model.ShipmentRecipientChangedData;
import ua.kite.contracts.model.ShipmentRedirectedData;
import ua.kite.contracts.model.ShipmentStatus;
import ua.kite.contracts.model.ShipmentStatusChangedData;
import ua.kite.tracking.events.EventEnvelope;
import ua.kite.tracking.events.MalformedEventException;

/**
 * Перетворює подію з {@code shipment.events} на рядок журналу або вирішує, що
 * в журнал вона не йде.
 *
 * <p>Одна подія — щонайбільше один рядок. Тому коди, які приходять у
 * {@code status-changed}, не дублюються з подій-дій того самого переходу:
 *
 * <ul>
 *   <li>{@code created} — лише якщо накладна створена одразу в CREATED (KT01);
 *       чернетка DRAFT не має коду в каталозі 8.2;
 *   <li>{@code confirmed} — KT01, перехід T1 DRAFT → CREATED;
 *   <li>{@code cancelled} — KT03, CANCELLED;
 *   <li>{@code status-changed} — код і стан з самої події;
 *   <li>{@code redirected} — KT60 лише до прибуття; KT62 після прибуття змінює
 *       стан і приходить окремим {@code status-changed};
 *   <li>{@code recipient-changed} — KT61;
 *   <li>{@code hold-set}, {@code return-created} і решта — ні: KT32, KT70, KT71
 *       журнал отримує зі {@code status-changed}.
 * </ul>
 */
@Component
public class JournalMapper {

    /** Мапінг на UPU EMSEVT — лише базовий набір стандарту (8.2). */
    static final Map<String, String> UPU_CODES = Map.of("KT10", "EMA", "KT30", "EMG", "KT50", "EMI");

    private final ObjectMapper mapper;
    private final Clock clock;

    public JournalMapper(ObjectMapper mapper, Clock clock) {
        this.mapper = mapper;
        this.clock = clock;
    }

    public Optional<JournalEntry> toEntry(EventEnvelope event) {
        KiteEventType type;
        try {
            type = KiteEventType.fromValue(event.type());
        } catch (IllegalArgumentException unknown) {
            // У топіку можуть з'явитися нові типи (8.3, п. 8) — це не помилка.
            return Optional.empty();
        }
        return switch (type) {
            case KITE_SHIPMENT_CREATED_V1 -> {
                var d = event.dataAs(ShipmentCreatedData.class, mapper);
                boolean draft = Boolean.TRUE.equals(d.getDraft()) || d.getStatus() == ShipmentStatus.DRAFT;
                yield draft
                        ? Optional.empty()
                        : entry(
                                event,
                                d.getShipmentId(),
                                d.getTrackingNumber(),
                                "KT01",
                                ShipmentStatus.CREATED,
                                d.getOriginLocationId());
            }
            case KITE_SHIPMENT_CONFIRMED_V1 -> {
                var d = event.dataAs(ShipmentConfirmedData.class, mapper);
                yield entry(event, d.getShipmentId(), d.getTrackingNumber(), "KT01", ShipmentStatus.CREATED, null);
            }
            case KITE_SHIPMENT_CANCELLED_V1 -> {
                var d = event.dataAs(ShipmentCancelledData.class, mapper);
                yield entry(event, d.getShipmentId(), d.getTrackingNumber(), "KT03", ShipmentStatus.CANCELLED, null);
            }
            case KITE_SHIPMENT_STATUS_CHANGED_V1 -> {
                var d = event.dataAs(ShipmentStatusChangedData.class, mapper);
                yield entry(
                        event,
                        d.getShipmentId(),
                        d.getTrackingNumber(),
                        required(event, d.getEventCode(), "eventCode"),
                        required(event, d.getStatusAfter(), "statusAfter"),
                        d.getLocationId());
            }
            case KITE_SHIPMENT_REDIRECTED_V1 -> {
                var d = event.dataAs(ShipmentRedirectedData.class, mapper);
                yield Boolean.TRUE.equals(d.getAfterArrival())
                        ? Optional.empty()
                        : entry(event, d.getShipmentId(), d.getTrackingNumber(), "KT60", null, null);
            }
            case KITE_SHIPMENT_RECIPIENT_CHANGED_V1 -> {
                var d = event.dataAs(ShipmentRecipientChangedData.class, mapper);
                yield entry(event, d.getShipmentId(), d.getTrackingNumber(), "KT61", null, null);
            }
            default -> Optional.empty();
        };
    }

    private Optional<JournalEntry> entry(
            EventEnvelope event,
            UUID shipmentId,
            String trackingNumber,
            String eventCode,
            ShipmentStatus statusAfter,
            UUID locationId) {
        OffsetDateTime occurredAt = required(event, event.time(), "time");
        OffsetDateTime receivedAt = event.kitereceivedat() != null ? event.kitereceivedat() : OffsetDateTime.now(clock);
        return Optional.of(new JournalEntry(
                event.id(),
                required(event, shipmentId, "shipmentId"),
                required(event, trackingNumber, "trackingNumber"),
                eventCode,
                statusAfter,
                locationId,
                occurredAt,
                receivedAt,
                required(event, event.source(), "source"),
                event.type(),
                UPU_CODES.get(eventCode),
                event.data()));
    }

    private static <T> T required(EventEnvelope event, T value, String field) {
        if (value == null) {
            throw new MalformedEventException("Подія " + event.id() + " (" + event.type() + ") без " + field);
        }
        return value;
    }
}
