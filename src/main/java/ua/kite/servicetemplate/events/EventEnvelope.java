package ua.kite.servicetemplate.events;

import java.time.OffsetDateTime;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Конверт доменної події Kite: CloudEvents 1.0 у JSON, structured mode
 * (9.7; схема — {@code kite/contracts/events/cloudevents.schema.json}).
 *
 * <p>Один тип на обидва напрямки: {@code data} лежить деревом JSON, а в класи
 * {@code kite-contracts} перетворюється на межі — {@link #dataAs}. Невідомі
 * поля ігноруються: схеми розвиваються лише додаванням полів (8.3, п. 8).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventEnvelope(
        String specversion,
        UUID id,
        String source,
        String type,
        String subject,
        /* occurred_at — момент самої події, первинний час журналу (8.3, п. 4). */
        OffsetDateTime time,
        String datacontenttype,
        String dataschema,
        /* received_at — момент запису; тай-брейк порядку (8.3, пп. 4-5). */
        OffsetDateTime kitereceivedat,
        String kiterequestid,
        UUID kitecausationid,
        String kitepartitionkey,
        JsonNode data) {

    public static final String SPEC_VERSION = "1.0";
    public static final String CONTENT_TYPE = "application/json";
    /** Заголовок Kafka-запису в structured mode CloudEvents. */
    public static final String KAFKA_CONTENT_TYPE = "application/cloudevents+json; charset=UTF-8";

    /** Тіло події як клас із kite-contracts, напр. {@code ShipmentStatusChangedData}. */
    public <T> T dataAs(Class<T> type, ObjectMapper mapper) {
        try {
            return mapper.treeToValue(data, type);
        } catch (Exception e) {
            throw new MalformedEventException("data події " + id + " не читається як " + type.getSimpleName(), e);
        }
    }
}
