package br.com.kafka.util.infra.config.transaction;

import br.com.kafka.util.core.domain.ErrorMessage;
import br.com.kafka.util.infra.config.KafkaConfigProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

import java.util.HashMap;
import java.util.Map;

@Slf4j

public class JaiminhoSpringErrorRecoverer implements ConsumerRecordRecoverer {


    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaConfigProperties properties;
    private final ObjectMapper objectMapper;

    public JaiminhoSpringErrorRecoverer(KafkaConfigProperties properties, ObjectMapper objectMapper) {

        this.properties = properties;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = new KafkaTemplate<>(createErrorProducerFactory());
    }

    private ProducerFactory<String, Object> createErrorProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Customizações específicas que você usou no Quarkus:
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // pr.remove("enable.idempotence")
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // max.in.flight.request.per.connection
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        String targetTopic = properties.errorProducerTopic();

        if (targetTopic == null || targetTopic.isEmpty()) {
            log.warn("Mensagem falhou mas 'kafka.error.producer.topic' não foi configurado. Erro original: {}", exception.getMessage());
            return;
        }

        try {
            log.info("Encaminhando mensagem com erro para o tópico configurado: {}", targetTopic);

            Object originalPayload = record.value();

            String msgError = exception != null ? exception.getMessage() + (exception.getCause() != null ? ", " + exception.getCause() : "") : "Erro inesperado";

            ErrorMessage errorMessageEnvelope = new ErrorMessage(originalPayload, properties.applicationName(), msgError);

            String key = record.key() != null ? record.key().toString() : null;
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(targetTopic, key, errorMessageEnvelope);

            record.headers().forEach(header -> producerRecord.headers().add(header));

            kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Erro ao enviar mensagem para a fila de erro do Jaiminho-Spring [{}]", targetTopic, ex);
                } else {
                    log.info("Mensagem enviada com sucesso para a fila de erros [{}] no offset: {}", targetTopic, result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("Erro fatal ao tentar processar o recoverer de erro da lib", e);
        }
    }
}
