package ua.kite.tracking;

import java.util.List;

import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Postgres і Kafka для інтеграційних тестів і `make run`. Адреси контейнерів
 * потрапляють у datasource і spring.kafka через {@code *ConnectionDetails} —
 * нічого не прописуємо в properties.
 *
 * <p>Один набір контейнерів на весь прогін. Вони навмисно не біни Spring:
 * тести піднімають кілька контекстів, і контейнер-бін зупинився б разом із
 * першим закритим контекстом, поки інші ще працюють. Прибирає їх Ryuk, коли
 * завершується JVM.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    // Версії — ті самі, що на стенді (kite-infra/versions.env). Сервісу з
    // PostGIS (network) тут потрібен imresamu/postgis:16-3.5 як сумісний образ.
    public static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));
    public static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
    }

    @Bean
    JdbcConnectionDetails postgresConnection() {
        return new JdbcConnectionDetails() {
            @Override
            public String getUsername() {
                return POSTGRES.getUsername();
            }

            @Override
            public String getPassword() {
                return POSTGRES.getPassword();
            }

            @Override
            public String getJdbcUrl() {
                return POSTGRES.getJdbcUrl();
            }
        };
    }

    @Bean
    KafkaConnectionDetails kafkaConnection() {
        return () -> List.of(KAFKA.getBootstrapServers());
    }
}
