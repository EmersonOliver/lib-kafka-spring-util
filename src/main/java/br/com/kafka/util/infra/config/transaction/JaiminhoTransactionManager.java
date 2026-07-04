package br.com.kafka.util.infra.config.transaction;

import br.com.kafka.util.infra.config.KafkaConfigProperties;
import br.com.kafka.util.interfaces.send.SendToKafka;
import br.com.kafka.util.interfaces.send.SpringSend;
import br.com.kafka.util.interfaces.send.SpringSendMsgs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class JaiminhoTransactionManager {


    private final KafkaConfigProperties properties;
    private final ObjectMapper objectMapper;

    // Adicione este método dentro da classe JaiminhoTransactionManager que criamos antes:
    public SendToKafka getSimpleProducer(String producerKey) {
        Map<String, Object> config = new HashMap<>();
        config.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
        config.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        var specificConfig = properties.producer().get(producerKey);
        String realTopicName = producerKey;

        if (specificConfig != null) {
            realTopicName = specificConfig.topic();
            specificConfig.jassConfig().ifPresent(jass -> {
                config.put("sasl.jass.config", jass);
                config.put("security.protocol", "SASL_SSL");
                config.put("sasl.mechanism", "PLAIN");
            });
        }

        return new SpringSendMsgs(config, realTopicName, objectMapper);
    }

    public SpringSend getTransactionProducer(String producerKey) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        var specificConfig = properties.producer().get(producerKey);
        String realTopicName = producerKey;

        if (specificConfig != null) {
            realTopicName = specificConfig.topic();
            // Injeta configurações extras de JAAS/Segurança se houver por canal
            specificConfig.jassConfig().ifPresent(jass -> {
                config.put("sasl.jass.config", jass);
                config.put("security.protocol", "SASL_SSL");
                config.put("sasl.mechanism", "PLAIN");
            });
        }

        // Retorna uma instância limpa pronta para controlar a transação
        return new SpringSend(config, realTopicName, objectMapper);
    }

}
