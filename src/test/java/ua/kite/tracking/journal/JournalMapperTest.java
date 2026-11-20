package ua.kite.tracking.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import ua.kite.contracts.model.ShipmentStatus;
import ua.kite.tracking.events.EventEnvelope;
import ua.kite.tracking.events.MalformedEventException;

class JournalMapperTest {

    static final Instant NOW = Instant.parse("2026-11-20T12:00:00Z");
    static final OffsetDateTime OCCURRED = OffsetDateTime.parse("2026-11-20T10:15:00+02:00");
    static final UUID SHIPMENT = UUID.fromString("01936a2e-0000-7000-8000-000000000001");
    static final UUID LOCATION = UUID.fromString("01936a2e-0000-7000-8000-0000000000aa");

    final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    final JournalMapper journalMapper = new JournalMapper(mapper, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void zminaStanu_kodStanITochkaZPodii() {
        var event = event(
                "kite.shipment.status-changed.v1",
                Map.of(
                        "shipmentId", SHIPMENT.toString(),
                        "trackingNumber", "KT123456785UA",
                        "statusBefore", "IN_TRANSIT",
                        "statusAfter", "READY_FOR_PICKUP",
                        "eventCode", "KT30",
                        "locationId", LOCATION.toString()),
                OffsetDateTime.parse("2026-11-20T08:16:00Z"));

        JournalEntry entry = journalMapper.toEntry(event).orElseThrow();

        assertThat(entry.eventId()).isEqualTo(event.id());
        assertThat(entry.shipmentId()).isEqualTo(SHIPMENT);
        assertThat(entry.eventCode()).isEqualTo("KT30");
        assertThat(entry.statusAfter()).isEqualTo(ShipmentStatus.READY_FOR_PICKUP);
        assertThat(entry.locationId()).isEqualTo(LOCATION);
        assertThat(entry.upuCode()).isEqualTo("EMG");
        assertThat(entry.occurredAt()).isEqualTo(OCCURRED);
        assertThat(entry.receivedAt()).isEqualTo(OffsetDateTime.parse("2026-11-20T08:16:00Z"));
        assertThat(entry.source()).isEqualTo("/kite/shipment-service");
        assertThat(entry.details().get("eventCode").asText()).isEqualTo("KT30");
    }

    @Test
    void bezKitereceivedat_chasZapysuZHodynnyka() {
        var event = event(
                "kite.shipment.cancelled.v1",
                Map.of(
                        "shipmentId", SHIPMENT.toString(),
                        "trackingNumber", "KT123456785UA",
                        "statusBefore", "CREATED"),
                null);

        JournalEntry entry = journalMapper.toEntry(event).orElseThrow();

        assertThat(entry.eventCode()).isEqualTo("KT03");
        assertThat(entry.statusAfter()).isEqualTo(ShipmentStatus.CANCELLED);
        assertThat(entry.upuCode()).isNull();
        assertThat(entry.receivedAt().toInstant()).isEqualTo(NOW);
    }

    @Test
    void chernetka_neZhurnaliuietsia_aStvorenaOdrazu_KT01() {
        var draft = created("DRAFT", true);
        var created = created("CREATED", false);

        assertThat(journalMapper.toEntry(draft)).isEmpty();
        JournalEntry entry = journalMapper.toEntry(created).orElseThrow();
        assertThat(entry.eventCode()).isEqualTo("KT01");
        assertThat(entry.statusAfter()).isEqualTo(ShipmentStatus.CREATED);
        assertThat(entry.locationId()).isEqualTo(LOCATION);
    }

    @Test
    void pidtverdzhennia_KT01() {
        var event = event(
                "kite.shipment.confirmed.v1",
                Map.of(
                        "shipmentId", SHIPMENT.toString(),
                        "trackingNumber", "KT123456785UA",
                        "priceTotal", Map.of("amountMinor", 9000, "currency", "UAH")),
                null);

        assertThat(journalMapper.toEntry(event).orElseThrow().eventCode()).isEqualTo("KT01");
    }

    @Test
    void pereadresatsiia_doPrybuttia_KT60_bezZminyStanu_pisliaPrybuttiaNe() {
        var before = redirected(false);
        var after = redirected(true);

        JournalEntry entry = journalMapper.toEntry(before).orElseThrow();
        assertThat(entry.eventCode()).isEqualTo("KT60");
        assertThat(entry.statusAfter()).isNull();
        // KT62 змінює стан і приходить окремим status-changed.
        assertThat(journalMapper.toEntry(after)).isEmpty();
    }

    @Test
    void zminaOtrymuvacha_KT61() {
        var event = event(
                "kite.shipment.recipient-changed.v1",
                Map.of(
                        "shipmentId", SHIPMENT.toString(),
                        "trackingNumber", "KT123456785UA",
                        "newRecipientPhoneHash", "a".repeat(64)),
                null);

        JournalEntry entry = journalMapper.toEntry(event).orElseThrow();
        assertThat(entry.eventCode()).isEqualTo("KT61");
        assertThat(entry.statusAfter()).isNull();
    }

    @Test
    void nevidomyiAboNetsikavyiTyp_propuskaietsia() {
        assertThat(journalMapper.toEntry(event("kite.shipment.hold-set.v1", Map.of(), null)))
                .isEmpty();
        assertThat(journalMapper.toEntry(event("kite.shipment.something-new.v1", Map.of(), null)))
                .isEmpty();
    }

    @Test
    void bezChasuPodii_bytaPodiia() {
        JsonNode data = mapper.valueToTree(Map.of(
                "shipmentId", SHIPMENT.toString(),
                "trackingNumber", "KT123456785UA",
                "statusBefore", "CREATED"));
        var event = new EventEnvelope(
                "1.0",
                UUID.randomUUID(),
                "/kite/shipment-service",
                "kite.shipment.cancelled.v1",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                data);

        assertThatThrownBy(() -> journalMapper.toEntry(event))
                .isInstanceOf(MalformedEventException.class)
                .hasMessageContaining("time");
    }

    @Test
    void zminaStanuBezKodu_bytaPodiia() {
        var event = event(
                "kite.shipment.status-changed.v1",
                Map.of(
                        "shipmentId", SHIPMENT.toString(),
                        "trackingNumber", "KT123456785UA",
                        "statusBefore", "IN_TRANSIT",
                        "statusAfter", "AT_HUB"),
                null);

        assertThatThrownBy(() -> journalMapper.toEntry(event))
                .isInstanceOf(MalformedEventException.class)
                .hasMessageContaining("eventCode");
    }

    private EventEnvelope created(String status, boolean draft) {
        return event(
                "kite.shipment.created.v1",
                Map.of(
                        "shipmentId",
                        SHIPMENT.toString(),
                        "trackingNumber",
                        "KT123456785UA",
                        "type",
                        "PARCEL",
                        "status",
                        status,
                        "draft",
                        draft,
                        "originLocationId",
                        LOCATION.toString(),
                        "destinationLocationId",
                        LOCATION.toString(),
                        "payer",
                        "SENDER",
                        "parcelCount",
                        1),
                null);
    }

    private EventEnvelope redirected(boolean afterArrival) {
        return event(
                "kite.shipment.redirected.v1",
                Map.of(
                        "shipmentId",
                        SHIPMENT.toString(),
                        "trackingNumber",
                        "KT123456785UA",
                        "toLocationId",
                        LOCATION.toString(),
                        "afterArrival",
                        afterArrival),
                null);
    }

    private EventEnvelope event(String type, Map<String, Object> data, OffsetDateTime receivedAt) {
        return new EventEnvelope(
                "1.0",
                UUID.randomUUID(),
                "/kite/shipment-service",
                type,
                "KT123456785UA",
                OCCURRED,
                "application/json",
                null,
                receivedAt,
                null,
                null,
                SHIPMENT.toString(),
                mapper.valueToTree(data));
    }
}
