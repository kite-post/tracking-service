package ua.kite.tracking;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import ua.kite.contracts.model.KiteEventType;
import ua.kite.contracts.model.ShipmentStatus;
import ua.kite.contracts.model.ShipmentStatusChangedEvent;

/**
 * Перевіряє, що контракти справді приходять залежністю: і класи, і схеми. Якщо
 * хтось скопіює схему до себе або відв'яже залежність, цей тест упаде першим.
 */
class ContractsWiringTest {

    private static final String EVENT = """
            {
              "specversion": "1.0",
              "id": "0193c2f4-2f4a-7a7e-9c3d-2f1b8a6e4d11",
              "source": "/kite/shipment-service",
              "type": "kite.shipment.status-changed.v1",
              "subject": "KT123456785UA",
              "time": "2026-11-16T09:14:03Z",
              "kitereceivedat": "2026-11-16T09:14:04.812Z",
              "data": {
                "shipmentId": "0193c2f4-1111-7a7e-9c3d-2f1b8a6e4d00",
                "trackingNumber": "KT123456785UA",
                "statusBefore": "IN_TRANSIT",
                "statusAfter": "READY_FOR_PICKUP",
                "eventCode": "KT30",
                "transition": "T7"
              }
            }
            """;

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void podiiaRozbyraietsiaKlasamyKontraktiv() throws Exception {
        ShipmentStatusChangedEvent event = mapper.readValue(EVENT, ShipmentStatusChangedEvent.class);

        assertThat(event.getType().getValue()).isEqualTo(KiteEventType.KITE_SHIPMENT_STATUS_CHANGED_V1.getValue());
        assertThat(event.getData().getStatusAfter()).isEqualTo(ShipmentStatus.READY_FOR_PICKUP);
        assertThat(event.getData().getTrackingNumber()).isEqualTo("KT123456785UA");
        assertThat(event.getKitereceivedat()).isNotNull();
    }

    @Test
    void skhemyPryikhodiatRazomIzZalezhnistiu() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/kite/contracts/events/registry.json")) {
            assertThat(in).as("реєстр подій має приходити в jar kite-contracts").isNotNull();
            JsonNode registry = mapper.readTree(in);
            assertThat(registry.get("events")).isNotEmpty();
        }
    }
}
