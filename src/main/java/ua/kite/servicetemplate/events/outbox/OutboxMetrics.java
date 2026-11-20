package ua.kite.servicetemplate.events.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Відставання outbox у Prometheus. Це метрики, а не healthcheck навмисно:
 * коли лежить Kafka, сервіс має лишатися ready і складати події в outbox —
 * для цього outbox і існує. Тривогу піднімає алерт на
 * {@code kite_outbox_oldest_age_seconds}.
 */
@Component
class OutboxMetrics implements MeterBinder {

    private final JdbcClient jdbc;

    OutboxMetrics(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("kite.outbox.pending", this::pending)
                .description("Неопубліковані події в outbox")
                .register(registry);
        Gauge.builder("kite.outbox.oldest.age", this::oldestAgeSeconds)
                .description("Вік найстарішої неопублікованої події")
                .baseUnit("seconds")
                .register(registry);
    }

    private double pending() {
        return query("SELECT count(*) FROM outbox WHERE published_at IS NULL");
    }

    private double oldestAgeSeconds() {
        return query("""
                SELECT coalesce(extract(epoch FROM now() - min(created_at)), 0)
                  FROM outbox
                 WHERE published_at IS NULL
                """);
    }

    private double query(String sql) {
        try {
            return jdbc.sql(sql).query(Double.class).single();
        } catch (RuntimeException e) {
            // База недоступна — краще порожня точка на графіку, ніж збій скрейпу.
            return Double.NaN;
        }
    }
}
