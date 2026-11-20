package ua.kite.servicetemplate.events.outbox;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

import ua.kite.contracts.model.KiteEventType;

/**
 * Подія, яку сервіс хоче опублікувати. Конверт CloudEvents навколо неї
 * складає {@link Outbox}: id, source, час запису і ключ розділу ставляться
 * самі.
 *
 * <pre>{@code
 * outbox.append(OutgoingEvent.of(KITE_SHIPMENT_STATUS_CHANGED_V1, data)
 *         .subject(shipment.trackingNumber())
 *         .occurredAt(scan.occurredAt()));
 * }</pre>
 *
 * @param type тип із реєстру kite-contracts — з нього беруться топік і ключ
 * @param data тіло події, клас із kite-contracts ({@code ShipmentStatusChangedData} тощо)
 * @param subject сутність, якої стосується подія: номер відправлення, код мішка, рейс
 * @param occurredAt момент самої події; без нього — момент запису
 * @param causationId подія-причина ({@code kitecausationid}, 8.3, п. 2)
 */
public record OutgoingEvent(
        KiteEventType type, Object data, String subject, OffsetDateTime occurredAt, UUID causationId) {

    public OutgoingEvent {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(data, "data");
    }

    public static OutgoingEvent of(KiteEventType type, Object data) {
        return new OutgoingEvent(type, data, null, null, null);
    }

    public OutgoingEvent subject(String subject) {
        return new OutgoingEvent(type, data, subject, occurredAt, causationId);
    }

    public OutgoingEvent occurredAt(OffsetDateTime occurredAt) {
        return new OutgoingEvent(type, data, subject, occurredAt, causationId);
    }

    public OutgoingEvent causedBy(UUID causationId) {
        return new OutgoingEvent(type, data, subject, occurredAt, causationId);
    }
}
