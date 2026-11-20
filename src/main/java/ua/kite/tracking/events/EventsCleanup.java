package ua.kite.tracking.events;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Прибирає транспорт подій: опубліковане з outbox (це не архів, 8.1) і старі
 * записи processed_event. Порціями, щоб не тримати довгих блокувань. Кілька
 * реплік можуть прибирати одночасно — це безпечно.
 */
@Component
class EventsCleanup {

    private static final Logger log = LoggerFactory.getLogger(EventsCleanup.class);
    private static final int CHUNK = 5_000;

    private final JdbcClient jdbc;
    private final EventsProperties props;

    EventsCleanup(JdbcClient jdbc, EventsProperties props) {
        this.jdbc = jdbc;
        this.props = props;
    }

    @Scheduled(cron = "${kite.events.cleanup-cron:0 17 * * * *}")
    void cleanup() {
        try {
            int outbox = deleteOlderThan("""
                    DELETE FROM outbox WHERE id IN (
                        SELECT id FROM outbox
                         WHERE published_at < now() - make_interval(secs => :age)
                         LIMIT :chunk)
                    """, props.outbox().retention());
            int inbox = deleteOlderThan("""
                    DELETE FROM processed_event WHERE (consumer, event_id) IN (
                        SELECT consumer, event_id FROM processed_event
                         WHERE processed_at < now() - make_interval(secs => :age)
                         LIMIT :chunk)
                    """, props.inbox().retention());
            if (outbox + inbox > 0) {
                log.info("Прибрано: outbox {}, processed_event {}", outbox, inbox);
            }
        } catch (RuntimeException e) {
            log.warn("Прибирання транспорту подій не вдалося: {}", e.toString());
        }
    }

    private int deleteOlderThan(String sql, Duration age) {
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.sql(sql)
                    .param("age", age.toSeconds())
                    .param("chunk", CHUNK)
                    .update();
            total += deleted;
        } while (deleted == CHUNK);
        return total;
    }
}
