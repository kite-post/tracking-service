package ua.kite.tracking.journal;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Місячні партиції {@code tracking_event} (8.3, п. 7). Саме створення — у
 * функції бази {@code tracking_event_ensure_partition} (міграція V2); тут —
 * коли її викликати:
 *
 * <ul>
 *   <li>на старті й щоночі — поточний місяць і {@code months-ahead} наперед;
 *   <li>перед записом — місяць самої події, якщо його ще немає (офлайн-сканер,
 *       спізніла подія).
 * </ul>
 *
 * <p>Партиція створюється в окремій транзакції: вона потрібна незалежно від
 * того, чи закомітиться обробка події, а довге блокування батьківської
 * таблиці в транзакції обробника гальмувало б решту споживачів.
 */
@Component
public class TrackingPartitions {

    private static final Logger log = LoggerFactory.getLogger(TrackingPartitions.class);

    private final Set<YearMonth> known = ConcurrentHashMap.newKeySet();
    private final JdbcClient jdbc;
    private final TransactionTemplate ownTx;
    private final Clock clock;
    private final int monthsAhead;

    public TrackingPartitions(
            JdbcClient jdbc,
            PlatformTransactionManager txManager,
            Clock clock,
            @Value("${kite.tracking.partitions.months-ahead:3}") int monthsAhead) {
        this.jdbc = jdbc;
        this.ownTx = new TransactionTemplate(txManager);
        this.ownTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.clock = clock;
        this.monthsAhead = monthsAhead;
    }

    /** Гарантує, що партиція для моменту {@code ts} існує. */
    public void ensureFor(OffsetDateTime ts) {
        YearMonth month = YearMonth.from(ts.withOffsetSameInstant(ZoneOffset.UTC));
        if (known.contains(month)) {
            return;
        }
        String partition = ownTx.execute(status -> jdbc.sql("SELECT tracking_event_ensure_partition(:ts)")
                .param("ts", ts)
                .query(String.class)
                .single());
        known.add(month);
        log.debug("Партиція {} на місці", partition);
    }

    @EventListener(ApplicationReadyEvent.class)
    @Scheduled(cron = "${kite.tracking.partitions.cron:0 7 3 * * *}")
    void createAhead() {
        YearMonth current = YearMonth.now(clock.withZone(ZoneOffset.UTC));
        try {
            for (int i = 0; i <= monthsAhead; i++) {
                ensureFor(current.plusMonths(i).atDay(1).atStartOfDay().atOffset(ZoneOffset.UTC));
            }
        } catch (RuntimeException e) {
            // Не валимо сервіс: партицію події все одно створить ensureFor перед записом.
            log.warn("Не вдалося створити партиції tracking_event наперед: {}", e.toString());
        }
    }
}
