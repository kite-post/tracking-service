package ua.kite.servicetemplate.events.inbox;

import java.util.UUID;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import ua.kite.servicetemplate.events.EventEnvelope;
import ua.kite.servicetemplate.events.MalformedEventException;

/**
 * Ідемпотентна обробка вхідних подій (5.4, п. 3; 8.3, п. 3). Outbox гарантує
 * доставку щонайменше раз, тож повтори — норма, а не аварія.
 *
 * <p>Позначка в {@code processed_event} і зміни обробника йдуть в одній
 * транзакції: якщо обробник упав, відкочується і позначка, і повтор від
 * Kafka обробить подію наново.
 *
 * <pre>{@code
 * @KafkaListener(topics = "shipment.events")
 * void on(ConsumerRecord<String, String> record) {
 *     inbox.handleOnce("shipment-status", record, event -> { ... });
 * }
 * }</pre>
 */
@Component
public class Inbox {

    private static final Logger log = LoggerFactory.getLogger(Inbox.class);

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;

    public Inbox(JdbcClient jdbc, ObjectMapper mapper, PlatformTransactionManager txManager) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.tx = new TransactionTemplate(txManager);
    }

    /**
     * Розбирає запис і викликає обробник, якщо цей споживач ще не обробляв
     * подію з таким id. Повертає {@code false} для повтору.
     *
     * @param consumer ім'я обробника в межах сервіса; стабільне — це ключ у processed_event
     */
    public boolean handleOnce(String consumer, ConsumerRecord<String, String> record, Consumer<EventEnvelope> handler) {
        EventEnvelope event = parse(record);
        Boolean handled = tx.execute(status -> {
            if (!markProcessed(consumer, event.id())) {
                log.debug("{}: подія {} ({}) уже оброблена, пропускаю", consumer, event.id(), event.type());
                return false;
            }
            handler.accept(event);
            return true;
        });
        return Boolean.TRUE.equals(handled);
    }

    private boolean markProcessed(String consumer, UUID eventId) {
        return jdbc.sql("""
                        INSERT INTO processed_event (consumer, event_id)
                        VALUES (:consumer, :eventId)
                        ON CONFLICT DO NOTHING
                        """)
                        .param("consumer", consumer)
                        .param("eventId", eventId)
                        .update()
                == 1;
    }

    private EventEnvelope parse(ConsumerRecord<String, String> record) {
        EventEnvelope event;
        try {
            event = mapper.readValue(record.value(), EventEnvelope.class);
        } catch (Exception e) {
            throw new MalformedEventException(
                    "Запис " + record.topic() + "-" + record.partition() + "@" + record.offset()
                            + " не є конвертом CloudEvents",
                    e);
        }
        if (event == null || event.id() == null || event.type() == null) {
            throw new MalformedEventException(
                    "Запис " + record.topic() + "-" + record.partition() + "@" + record.offset() + " без id або type");
        }
        return event;
    }
}
