package ua.kite.servicetemplate.events;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import ua.kite.contracts.model.KiteEventType;

/**
 * Реєстр подій із jar {@code kite-contracts}: для кожного типу — топік і поле
 * {@code data}, яке є ключем розділу Kafka (9.7). Сервіс не тримає власної
 * таблиці «тип → топік»: нова версія контрактів приносить її сама.
 */
@Component
public class EventRegistry {

    static final String REGISTRY = "/kite/contracts/events/registry.json";

    /** Куди і з яким ключем іде подія певного типу. */
    public record Route(String topic, String partitionKeyField) {}

    private final Map<String, Route> routes;

    public EventRegistry(ObjectMapper mapper) {
        this.routes = load(mapper);
    }

    public Route route(KiteEventType type) {
        Route route = routes.get(type.getValue());
        if (route == null) {
            throw new IllegalArgumentException(
                    "Тип " + type.getValue() + " є в KiteEventType, але немає в " + REGISTRY);
        }
        return route;
    }

    /** Значення ключа розділу з тіла події: {@code shipmentId}, {@code unitCode}, {@code tripId}… */
    public String partitionKey(KiteEventType type, JsonNode data) {
        String field = route(type).partitionKeyField();
        JsonNode value = data == null ? null : data.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException(
                    "Подія " + type.getValue() + " має нести поле " + field + " — це її ключ розділу (9.7)");
        }
        return value.asText();
    }

    private static Map<String, Route> load(ObjectMapper mapper) {
        try (InputStream in = EventRegistry.class.getResourceAsStream(REGISTRY)) {
            if (in == null) {
                throw new IllegalStateException(REGISTRY + " немає на classpath — чи підключений kite-contracts?");
            }
            Map<String, Route> result = new HashMap<>();
            for (JsonNode event : mapper.readTree(in).path("events")) {
                result.put(
                        event.path("type").asText(),
                        new Route(
                                event.path("topic").asText(),
                                event.path("partitionKey").asText()));
            }
            return Map.copyOf(result);
        } catch (IOException e) {
            throw new UncheckedIOException("Не вдалося прочитати " + REGISTRY, e);
        }
    }
}
