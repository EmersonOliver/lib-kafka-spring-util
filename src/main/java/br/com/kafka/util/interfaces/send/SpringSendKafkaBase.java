package br.com.kafka.util.interfaces.send;

import br.com.kafka.util.core.domain.MessageWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


@Slf4j
public abstract class SpringSendKafkaBase {

    protected Producer<String, Object> producer;
    protected final String topic;
    protected final Map<String, Object> baseConfigs;
    protected String transactionID;
    protected final ObjectMapper objectMapper;

    public SpringSendKafkaBase(Map<String, Object> baseConfigs, String topic, ObjectMapper objectMapper) {
        this.baseConfigs = new HashMap<>(baseConfigs);
        this.topic = topic;
        this.objectMapper = objectMapper;
        this.producer = null;
    }
    private String generatedTransactionId() {
        String uniqueId = UUID.randomUUID().toString();
        this.transactionID = "Jaiminho-" + topic + "-" + uniqueId;
        return this.transactionID;
    }
    void initializerProducer() {
        baseConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, generatedTransactionId());
        DefaultKafkaProducerFactory<String, Object> pf = new DefaultKafkaProducerFactory<>(baseConfigs);
        this.producer = pf.createProducer();
//        this.producer = new KafkaProducer<>(this.baseConfigs);
//        this.producer.initTransactions();

    }
    void beginTransaction() {
        if (producer != null) {
            producer.beginTransaction();
        }
    }

    void commitInternal() {
        try {
            if (producer != null) {
                producer.commitTransaction();
            }
        } catch (ProducerFencedException | InvalidProducerEpochException e) {
            log.error("Producer fenced during commit: {}", e.getMessage());
            closeProducer();
            throw e;
        } catch (KafkaException e) {
            log.error("Error during commit Transaction: {}", e.getMessage());
            closeProducer();
            throw e;
        }
    }

    void abortInternal() {
        try {
            if (producer != null) {
                producer.abortTransaction();
            }
        } catch (ProducerFencedException | InvalidProducerEpochException e) {
            log.error("Producer fenced during abort: {}", e.getMessage());
            closeProducer();
        } catch (KafkaException e) {
            log.error("Error during abortTransaction: {}", e.getMessage());
            closeProducer();
        }
    }

    void closeProducer() {
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                log.debug("Erro ao fechar o producer internamente");
            }
        }
        producer = null;
    }

    public static String getHash(String jsonAsString) {
        return DigestUtils.sha256Hex(jsonAsString);
    }

    protected void sendMsgs(List<MessageWrapper> msgs) throws Exception {
        for (MessageWrapper messageWrapper : msgs) {
            String jsonAsString = objectMapper.writeValueAsString(messageWrapper.getPayload());
            String hash = messageWrapper.getKey() != null ? messageWrapper.getKey() : getHash(jsonAsString);

            String targetTopic = !StringUtils.hasText(messageWrapper.getTopicName()) ? topic : messageWrapper.getTopicName();

            ProducerRecord<String, Object> record = new ProducerRecord<>(targetTopic, hash, jsonAsString);

            Map<String, String> headers = messageWrapper.getHeader();
            if (headers != null && !headers.isEmpty()) {
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    record.headers().add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
                }
            }
            producer.send(record);
        }
    }
}
