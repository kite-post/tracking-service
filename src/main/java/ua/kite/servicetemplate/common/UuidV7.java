package ua.kite.servicetemplate.common;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;

/**
 * UUID версії 7 (RFC 9562): 48 біт часу в мілісекундах, далі випадкові біти.
 * Впорядковані за часом, тож B-дерево індексу росте з одного краю. Первинні
 * ключі в Kite генерує застосунок, а не база (6.1); ідентифікатори подій — теж.
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {}

    public static UUID next() {
        return next(Clock.systemUTC());
    }

    public static UUID next(Clock clock) {
        long millis = clock.millis();
        long randA = RANDOM.nextLong();
        long randB = RANDOM.nextLong();

        // 48 біт часу | 4 біти версії (0111) | 12 випадкових біт
        long msb = (millis << 16) | 0x7000L | (randA & 0x0FFFL);
        // 2 біти варіанта (10) | 62 випадкових біти
        long lsb = (randB & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
        return new UUID(msb, lsb);
    }
}
