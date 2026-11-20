package ua.kite.servicetemplate.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import ua.kite.contracts.model.KiteEventType;
import ua.kite.contracts.model.ShipmentStatusChangedData;
import ua.kite.servicetemplate.events.EventEnvelope;
import ua.kite.servicetemplate.events.inbox.Inbox;

/**
 * ПРИКЛАД споживача — заміни своїми обробниками або видали пакет
 * {@code example} цілком.
 *
 * <p>Як влаштований справжній споживач:
 *
 * <ul>
 *   <li>топік — з таблиці 9.7, група — ім'я сервіса ({@code spring.kafka.consumer.group-id});
 *   <li>усе тіло обробки — всередині {@link Inbox#handleOnce}: повтор пропускається,
 *       а зміни в базі і позначка про обробку комітяться разом;
 *   <li>типи, які сервісу не цікаві, просто пропускаються — у топіку їх кілька;
 *   <li>виняток з обробника — три повтори, потім DLT сервіса ({@code dlt.<сервіс>}).
 * </ul>
 */
@Component
class ShipmentEventsListener {

    /** Ключ у processed_event: не змінювати, інакше всі події оброблятимуться наново. */
    static final String CONSUMER = "example-shipment-status";

    private static final Logger log = LoggerFactory.getLogger(ShipmentEventsListener.class);

    private final Inbox inbox;
    private final ObjectMapper mapper;

    ShipmentEventsListener(Inbox inbox, ObjectMapper mapper) {
        this.inbox = inbox;
        this.mapper = mapper;
    }

    // Топік — KiteTopic.SHIPMENT_EVENTS; в анотації потрібна константа.
    @KafkaListener(id = CONSUMER, idIsGroup = false, topics = "shipment.events")
    void onShipmentEvent(ConsumerRecord<String, String> record) {
        inbox.handleOnce(CONSUMER, record, this::handle);
    }

    private void handle(EventEnvelope event) {
        if (!KiteEventType.KITE_SHIPMENT_STATUS_CHANGED_V1.getValue().equals(event.type())) {
            return;
        }
        ShipmentStatusChangedData data = event.dataAs(ShipmentStatusChangedData.class, mapper);
        // Тут — справжня робота сервіса: оновити свою проєкцію, створити
        // нотифікацію, написати подію у відповідь через Outbox.append(...).
        log.info("Відправлення {}: {} → {}", data.getTrackingNumber(), data.getStatusBefore(), data.getStatusAfter());
    }
}
