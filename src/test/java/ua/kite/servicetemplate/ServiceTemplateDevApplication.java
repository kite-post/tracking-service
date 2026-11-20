package ua.kite.servicetemplate;

import org.springframework.boot.SpringApplication;

/**
 * `make run`: той самий застосунок, але Postgres і Kafka піднімаються
 * контейнерами з {@link TestcontainersConfiguration}. Стенд не потрібен,
 * порти з ним не конфліктують. Запуск з IDE — просто main цього класу.
 */
public class ServiceTemplateDevApplication {

    public static void main(String[] args) {
        SpringApplication.from(ServiceTemplateApplication::main)
                .with(TestcontainersConfiguration.class)
                .run(args);
    }
}
