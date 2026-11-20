package ua.kite.servicetemplate.events;

/**
 * Подію неможливо прочитати: битий JSON, немає id чи type, data не тієї форми.
 * Повтор тут не допоможе, тож обробник помилок шле такий запис одразу в DLT,
 * без ретраїв ({@link EventsConfiguration}).
 */
public class MalformedEventException extends RuntimeException {

    public MalformedEventException(String message) {
        super(message);
    }

    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
