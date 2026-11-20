package ua.kite.servicetemplate.events;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Обв'язка транспорту подій: планувальник для публікатора й прибирання,
 * DLT сервіса і обробник помилок споживачів.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(EventsProperties.class)
class EventsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(EventsConfiguration.class);

    /** Годинник для часу подій; у тестах підміняється фіксованим. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Один DLT на сервіс: {@code dlt.<сервіс>}. Звідки прийшов запис і чому
     * впав, лежить у заголовках {@code kafka_dlt-*}. Автостворення топіків на
     * стенді вимкнене (12.3), тож сервіс оголошує свій DLT сам; якщо брокер
     * недоступний на старті, сервіс усе одно піднімається.
     */
    @Bean
    NewTopic deadLetterTopic(EventsProperties props) {
        return TopicBuilder.name(props.deadLetterTopic())
                .partitions(6)
                .replicas(props.deadLetterReplicas())
                .build();
    }

    /**
     * Помилка обробника — три повтори з наростанням (0,5 → 1 → 2 с), потім
     * DLT, і споживач іде далі. Битий запис ({@link MalformedEventException})
     * повторювати марно — одразу в DLT. Транзакція обробника відкочується
     * разом із записом у processed_event, тож повтор справді повторює.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template, EventsProperties props) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, error) -> {
            log.error(
                    "Запис {}-{}@{} іде в {}: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    props.deadLetterTopic(),
                    error.toString());
            // Розділ -1: Kafka обере його за ключем запису.
            return new TopicPartition(props.deadLetterTopic(), -1);
        });

        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxAttempts(3);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(MalformedEventException.class, DeserializationException.class);
        return handler;
    }
}
