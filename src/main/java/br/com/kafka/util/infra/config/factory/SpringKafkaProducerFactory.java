package br.com.kafka.util.infra.config.factory;

import br.com.kafka.util.infra.config.KafkaConfigProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class SpringKafkaProducerFactory {

    private final KafkaConfigProperties properties;
    private final Map<String, KafkaTemplate<String, Object>> templateCache = new ConcurrentHashMap<>();

    public KafkaTemplate<String, Object> getOrCreateProducer(String producerKey) {
        return templateCache.computeIfAbsent(producerKey, key -> {
            Map<String, Object> config = new HashMap<>();

            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            config.put(ProducerConfig.ACKS_CONFIG, "all");
            config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

            var specificConfig = properties.producer().get(key);
            if (specificConfig != null) {
                specificConfig.jassConfig().ifPresent(jass -> {
                    config.put("sasl.jass.config", jass);
                    config.put("security.protocol", "SASL_SSL");
                    config.put("sasl.mechanism", "PLAIN");
                });
                specificConfig.maxRequestSize().ifPresent(size -> config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, size));
            }

            ProducerFactory<String, Object> pf = new DefaultKafkaProducerFactory<>(config);
            return new KafkaTemplate<>(pf);
        });
    }
}
