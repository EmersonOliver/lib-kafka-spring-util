package br.com.kafka.util.infra.config.factory;

import br.com.kafka.util.core.domain.MessageProcessor;
import br.com.kafka.util.core.domain.MessageWrapper;
import br.com.kafka.util.infra.config.KafkaConfigProperties;
import br.com.kafka.util.infra.config.controls.JaiminhoSpringControl;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
public class SpringKafkaConsumerFactory {

    private final KafkaConfigProperties properties;
    private final DefaultErrorHandler errorHandler;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, ConcurrentMessageListenerContainer<String, Object>> activeConsumers = new ConcurrentHashMap<>();

    public void createAndStartConsumer(String consumerKey, MessageProcessor processor, Class<?> targetPayloadType, boolean threadSafe) {
        activeConsumers.computeIfAbsent(consumerKey, key -> {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            var specificConfig = properties.consumer().get(key);
            String topicName = key;

            if (specificConfig != null) {
                topicName = specificConfig.topic();
                config.put(ConsumerConfig.GROUP_ID_CONFIG, specificConfig.groupId());
                config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, specificConfig.offsetResetConfig().orElse("earliest"));

                // AJUSTE DINÂMICO DE LOTE:
                // Se for threadSafe, podemos ler mais mensagens por Poll para processar em paralelo nas Virtual Threads.
                // Se NÃO for threadSafe, mantemos 1 por 1 para garantir a ordem estrita de processamento sequencial.
                if (threadSafe) {
                    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
                } else {
                    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
                }

                if (properties.bootstrapServer() != null && !properties.bootstrapServer().contains("localhost")) {
                    config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
                    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
                }
            }

            DefaultKafkaConsumerFactory<String, Object> cf = new DefaultKafkaConsumerFactory<>(config);
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(cf);
            factory.setCommonErrorHandler(errorHandler);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

            // =========================================================================
            // CONFIGURAÇÃO DE VIRTUAL THREADS BASEADA NA FLAG THREADSAFE
            // =========================================================================
            if (threadSafe) {
                // Cria um executor assíncrono do Spring configurado para usar Virtual Threads (Java 21+)
                SimpleAsyncTaskExecutor vtExecutor = new SimpleAsyncTaskExecutor("jaiminho-vt-" + consumerKey + "-");
                vtExecutor.setVirtualThreads(true);

                // Aloca o executor no container. O Spring passará a despachar o processamento
                // das mensagens capturadas usando threads virtuais paralelas.
                factory.getContainerProperties().setListenerTaskExecutor(vtExecutor);
            }
            // Se threadSafe for false, não setamos o executor. O Spring usará o comportamento
            // padrão sequencial bloqueante na thread principal do poll, garantindo a ordem.
            // =========================================================================

            ConcurrentMessageListenerContainer<String, Object> container = factory.createContainer(topicName);

            container.setupMessageListener((AcknowledgingMessageListener<String, Object>) (record, acknowledgment) -> {
                try {
                    MessageWrapper wrapper = new MessageWrapper();
                    wrapper.setTopicName(record.topic());
                    wrapper.setKey(record.key() != null ? record.key().toString() : null);
                    wrapper.setPartition(record.partition());
                    wrapper.setOffset(record.offset());
                    wrapper.setCreateTime(new Timestamp(record.timestamp()));

                    Object payload = objectMapper.readValue(record.value().toString(), targetPayloadType);
                    wrapper.setPayload(payload);

                    JaiminhoSpringControl control = new JaiminhoSpringControl(acknowledgment, wrapper, container);

                    processor.process(wrapper, control);

                } catch (Exception e) {
                    log.error("Erro processando mensagem no Jaiminho-Spring", e);
                    throw new RuntimeException(e);
                }
            });

            container.start();
            log.info("Consumer Jaiminho-Spring iniciado com sucesso (threadSafe={}) para a chave: {} no tópico: {}",
                    threadSafe, key, topicName);

            return container;
        });
    }

    public void stopConsumer(String consumerKey) {
        var container = activeConsumers.remove(consumerKey);
        if (container != null) {
            container.stop();
        }
    }

}
