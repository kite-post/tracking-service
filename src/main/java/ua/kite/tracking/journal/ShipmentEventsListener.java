package ua.kite.tracking.journal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import ua.kite.tracking.events.inbox.Inbox;

/**
 * Дописує події відправлення з {@code shipment.events} у журнал (8.1).
 * Ключ розділу — {@code shipmentId}, тож події одного відправлення приходять
 * по порядку (8.3, п. 4).
 *
 * <p>Ідемпотентність — у два рубежі: {@code processed_event} пропускає
 * повтор ще до мапінгу, а первинний ключ журналу {@code (event_id, occurred_at)}
 * не пустить дубль, навіть коли позначку вже прибрав ретеншен.
 */
@Component
class ShipmentEventsListener {

    /** Ключ у processed_event: не змінювати, інакше всі події оброблятимуться наново. */
    static final String CONSUMER = "tracking-journal";

    private final Inbox inbox;
    private final JournalMapper journalMapper;
    private final TrackingJournal journal;

    ShipmentEventsListener(Inbox inbox, JournalMapper journalMapper, TrackingJournal journal) {
        this.inbox = inbox;
        this.journalMapper = journalMapper;
        this.journal = journal;
    }

    // Топік — KiteTopic.SHIPMENT_EVENTS; в анотації потрібна константа.
    @KafkaListener(id = CONSUMER, idIsGroup = false, topics = "shipment.events")
    void onShipmentEvent(ConsumerRecord<String, String> record) {
        inbox.handleOnce(CONSUMER, record, event -> journalMapper.toEntry(event).ifPresent(journal::append));
    }
}
