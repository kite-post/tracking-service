package ua.kite.tracking.journal;

import java.time.OffsetDateTime;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

import ua.kite.contracts.model.ShipmentStatus;

/**
 * Рядок журналу {@code tracking_event} (6.2; 8.3, п. 7).
 *
 * @param eventId id події, з якої зроблено рядок; ключ ідемпотентності журналу
 * @param statusAfter стан після події або {@code null}, якщо подія стану не змінює (8.2)
 * @param locationId точка, де сталася подія, якщо відома
 * @param occurredAt момент самої події — первинний час (8.3, п. 4)
 * @param receivedAt момент запису — тай-брейк порядку
 * @param upuCode код UPU EMSEVT для трьох подій базового набору або {@code null}
 * @param details {@code data} події як є
 */
public record JournalEntry(
        UUID eventId,
        UUID shipmentId,
        String trackingNumber,
        String eventCode,
        ShipmentStatus statusAfter,
        UUID locationId,
        OffsetDateTime occurredAt,
        OffsetDateTime receivedAt,
        String source,
        String eventType,
        String upuCode,
        JsonNode details) {}
