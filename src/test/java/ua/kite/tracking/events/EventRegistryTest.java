package ua.kite.tracking.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import ua.kite.contracts.model.KiteEventType;

class EventRegistryTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final EventRegistry registry = new EventRegistry(mapper);

    @ParameterizedTest
    @EnumSource(KiteEventType.class)
    void kozhnyiTypMaeTopikIKliuch(KiteEventType type) {
        EventRegistry.Route route = registry.route(type);

        assertThat(route.topic()).endsWith(".events");
        assertThat(route.partitionKeyField()).isNotBlank();
    }

    @Test
    void kliuchBeretsiaZDataZaReiestrom() {
        var data = mapper.valueToTree(Map.of("shipmentId", "s-1", "trackingNumber", "KT123456785UA"));

        assertThat(registry.route(KiteEventType.KITE_SHIPMENT_STATUS_CHANGED_V1).topic())
                .isEqualTo("shipment.events");
        assertThat(registry.partitionKey(KiteEventType.KITE_SHIPMENT_STATUS_CHANGED_V1, data))
                .isEqualTo("s-1");
    }

    @Test
    void povernenniaKliuchuietsiaVykhidnymVidpravlenniam() {
        // 9.7: return-created — ключ originalShipmentId, щоб повернення йшло
        // в той самий розділ, що й історія відправлення.
        var data = mapper.valueToTree(Map.of("shipmentId", "return-1", "originalShipmentId", "s-1"));

        assertThat(registry.partitionKey(KiteEventType.KITE_SHIPMENT_RETURN_CREATED_V1, data))
                .isEqualTo("s-1");
    }

    @Test
    void bezPoliaKliuchaPodiiaNePyshetsia() {
        var data = mapper.valueToTree(Map.of("trackingNumber", "KT123456785UA"));

        assertThatThrownBy(() -> registry.partitionKey(KiteEventType.KITE_SHIPMENT_STATUS_CHANGED_V1, data))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shipmentId");
    }
}
