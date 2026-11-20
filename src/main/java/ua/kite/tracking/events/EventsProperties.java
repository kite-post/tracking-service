package ua.kite.tracking.events;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Налаштування транспорту подій ({@code kite.events.*} в application.yml).
 *
 * @param outbox публікатор outbox
 * @param inbox таблиця processed_event
 * @param deadLetterTopic куди йдуть записи, які не вдалося обробити
 * @param deadLetterReplicas фактор реплікації DLT-топіка, коли сервіс створює його сам
 */
@ConfigurationProperties("kite.events")
public record EventsProperties(Outbox outbox, Inbox inbox, String deadLetterTopic, int deadLetterReplicas) {

    /**
     * @param enabled чи працює публікатор у цьому процесі
     * @param pollInterval пауза між проходами, коли черга порожня
     * @param batchSize скільки подій бере один прохід
     * @param sendTimeout скільки чекати підтвердження Kafka для пачки
     * @param retention скільки тримати вже опубліковані рядки
     */
    public record Outbox(
            boolean enabled, Duration pollInterval, int batchSize, Duration sendTimeout, Duration retention) {}

    /**
     * @param retention скільки пам'ятати оброблені події. Має бути довшим за
     *     ретеншен топіків Kafka — інакше повтор старої події пройде як новий.
     */
    public record Inbox(Duration retention) {}
}
