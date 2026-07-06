package br.com.kafka.util.infra.config.validator;

import br.com.kafka.util.infra.config.properties.KafkaConfigProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConfigurationValidator {

    private final KafkaConfigProperties properties;

    @PostConstruct
    public void validate() {

        if (properties.producer() == null) {
            return;
        }

        properties.producer().forEach((key, config) -> {

            if (config.topic() == null || config.topic().isBlank()) {
                throw new BeanCreationException(
                        "Topic não configurado para producer: " + key
                );
            }
        });
    }
}
