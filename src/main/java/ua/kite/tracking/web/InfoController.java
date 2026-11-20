package ua.kite.tracking.web;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Єдиний ендпоїнт шаблона: показує, під яким іменем і якою версією піднявся
 * сервіс. Справжні ендпоїнти сервіса приходять на його місце.
 */
@RestController
@RequestMapping("/api/v1")
public class InfoController {

    private final String service;
    private final String version;

    public InfoController(
            @Value("${spring.application.name}") String service, @Value("${kite.version:dev}") String version) {
        this.service = service;
        this.version = version;
    }

    @GetMapping("/info")
    public Map<String, String> info() {
        return Map.of("service", service, "version", version);
    }
}
