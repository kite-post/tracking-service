package ua.kite.servicetemplate.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class UuidV7Test {

    @Test
    void versiiaIVariantZaRfc() {
        UUID id = UuidV7.next();

        assertThat(id.version()).isEqualTo(7);
        assertThat(id.variant()).isEqualTo(2);
    }

    @Test
    void pershi48BitTseChasUMilisekundakh() {
        Instant at = Instant.parse("2026-11-20T10:15:30.123Z");

        UUID id = UuidV7.next(Clock.fixed(at, ZoneOffset.UTC));

        assertThat(id.getMostSignificantBits() >>> 16).isEqualTo(at.toEpochMilli());
    }

    @Test
    void piznishyiChasDaeBilshyiId() {
        UUID earlier = UuidV7.next(Clock.fixed(Instant.parse("2026-11-20T10:00:00Z"), ZoneOffset.UTC));
        UUID later = UuidV7.next(Clock.fixed(Instant.parse("2026-11-20T10:00:01Z"), ZoneOffset.UTC));

        // Порівнюємо як беззнакові рядки: саме так їх сортує Postgres.
        assertThat(later.toString()).isGreaterThan(earlier.toString());
    }
}
