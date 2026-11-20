package ua.kite.tracking.events;

import java.util.Map;
import java.util.UUID;

import ua.kite.contracts.model.KiteEventType;
import ua.kite.tracking.common.UuidV7;
import ua.kite.tracking.events.outbox.OutgoingEvent;

/** Справжня за формою подія зміни статусу — така, яку прочитає і приклад-споживач. */
public final class TestEvents {

    private TestEvents() {}

    public static OutgoingEvent statusChanged(UUID shipmentId) {
        return OutgoingEvent.of(
                        KiteEventType.KITE_SHIPMENT_STATUS_CHANGED_V1,
                        Map.of(
                                "shipmentId", shipmentId.toString(),
                                "trackingNumber", "KT123456785UA",
                                "statusBefore", "IN_TRANSIT",
                                "statusAfter", "READY_FOR_PICKUP",
                                "eventCode", "KT30",
                                "transition", "T7"))
                .subject("KT123456785UA");
    }

    public static UUID newShipmentId() {
        return UuidV7.next();
    }
}
