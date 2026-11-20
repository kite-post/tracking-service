package ua.kite.tracking;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Інтеграційний тест: сервіс піднімається на справжньому порту з Postgres і
 * Kafka в контейнерах і відповідає. Клас з суфіксом IT підхоплює Failsafe на
 * фазі verify, тож `make test` швидкий, а `make test-it` і CI проганяють усе.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability(tracing = false) // у тестах Boot вимикає експорт метрик
@Import(TestcontainersConfiguration.class)
class InfoEndpointIT {

    @Autowired
    TestRestTemplate rest;

    @Autowired
    JdbcClient jdbc;

    @Test
    void infoVidpovidae() {
        ResponseEntity<String> response = rest.getForEntity("/api/v1/info", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("tracking-service");
    }

    @Test
    void healthKazheUp() {
        ResponseEntity<String> response = rest.getForEntity("/actuator/health", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    void readinessZalezhytVidBazy() {
        // Сюди дивиться HEALTHCHECK образу і depends_on стенда (12.3).
        ResponseEntity<String> response = rest.getForEntity("/actuator/health/readiness", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    void metrykyOutboxIdutUPrometheus() {
        ResponseEntity<String> response = rest.getForEntity("/actuator/prometheus", String.class);

        assertThat(response.getBody()).contains("kite_outbox_pending").contains("kite_outbox_oldest_age_seconds");
    }

    @Test
    void mihratsiiNakochenoNaStarti() {
        List<String> versions = jdbc.sql(
                        "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank")
                .query(String.class)
                .list();

        assertThat(versions).startsWith("1");
    }
}
