package ua.kite.servicetemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Читає топік із самого початку й повертає записи з потрібним ключем.
 * Тести ділять топіки між собою, тож кожен шукає лише свій ключ.
 */
public final class KafkaProbe {

    private KafkaProbe() {}

    public static List<ConsumerRecord<String, String>> await(String topic, String key, int expected, Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                TestcontainersConfiguration.KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false);

        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            while (found.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (key.equals(record.key())) {
                        found.add(record);
                    }
                }
            }
        }
        return found;
    }
}
