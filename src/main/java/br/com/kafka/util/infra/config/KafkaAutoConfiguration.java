package br.com.kafka.util.infra.config;

import br.com.kafka.util.infra.config.factory.SpringKafkaConsumerFactory;
import br.com.kafka.util.infra.config.factory.SpringKafkaProducerFactory;
import br.com.kafka.util.infra.config.injectors.ConsumerKeyInjectionProcessor;
import br.com.kafka.util.infra.config.injectors.ProducerKeyInjectionProcessor;
import br.com.kafka.util.infra.config.injectors.TopicKeyAnnotationProcessor;
import br.com.kafka.util.infra.config.transaction.JaiminhoSpringErrorRecoverer;
import br.com.kafka.util.infra.config.transaction.JaiminhoTransactionManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableConfigurationProperties(KafkaConfigProperties.class)
@RequiredArgsConstructor
public class KafkaAutoConfiguration {

    private final KafkaConfigProperties properties;

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    // 1. O ConsumerFactory base (que você já tem)
    @Bean
    @ConditionalOnExpression("#{ '${kafka.consumer:}' != '' }")
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka", name = "consumer")
    public static TopicKeyAnnotationProcessor topicKeyAnnotationProcessor(
            KafkaConfigProperties properties,
            ObjectMapper objectMapper) {
        return new TopicKeyAnnotationProcessor(properties, objectMapper);
    }

    @Bean
    @ConditionalOnExpression("#{ '${kafka.consumer:}' != '' }")
    public SpringKafkaConsumerFactory springKafkaConsumerFactory(
            DefaultErrorHandler errorHandler,
            ObjectMapper objectMapper) {
        // Passa as propriedades da lib, o gerenciador de erros (DLQ) e o Jackson
        return new SpringKafkaConsumerFactory(properties, errorHandler, objectMapper);
    }

    @Bean
    @ConditionalOnExpression("#{ '${kafka.consumer:}' != '' }")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> cf,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setCommonErrorHandler(errorHandler);

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("jaiminho-vt-");
        executor.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(executor);
        factory.setBatchListener(true);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    @ConditionalOnExpression("#{ '${kafka.consumer:}' != '' }")
    public static ConsumerKeyInjectionProcessor topicKeyBeanPostProcessor(
            ObjectProvider<SpringKafkaConsumerFactory> springKafkaConsumerFactoryProvider) {
        return new ConsumerKeyInjectionProcessor(springKafkaConsumerFactoryProvider);
    }


    @Bean
    @ConditionalOnProperty(prefix = "kafka", name = "consumer")
    public DefaultErrorHandler errorHandler(KafkaConfigProperties properties, ObjectMapper objectMapper) {
        FixedBackOff backOff = new FixedBackOff(2000L, 5L);
        JaiminhoSpringErrorRecoverer jaiminhoRecoverer = new JaiminhoSpringErrorRecoverer(properties, objectMapper);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(jaiminhoRecoverer, backOff);
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka", name = "producer")
    public SpringKafkaProducerFactory springKafkaProducerFactory(KafkaConfigProperties properties) {
        return new SpringKafkaProducerFactory(properties);
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka", name = "producer")
    public static ProducerKeyInjectionProcessor producerKeyInjectionProcessor(JaiminhoTransactionManager transactionManager) {
        return new ProducerKeyInjectionProcessor(transactionManager);
    }

    @Bean
    @ConditionalOnExpression("#{ '${kafka.producer:}' != '' }")
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    @ConditionalOnExpression("#{ '${kafka.producer:}' != '' }")
    public DeadLetterPublishingRecoverer publisher(KafkaTemplate<String, Object> bytesTemplate) {
        return new DeadLetterPublishingRecoverer(bytesTemplate,
                (record, exception) -> new org.apache.kafka.common.TopicPartition(record.topic() + ".DLQ", record.partition()));
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka", name = "producer")
    public JaiminhoTransactionManager jaiminhoTransactionManager(KafkaConfigProperties properties, ObjectMapper objectMapper) {
        return new JaiminhoTransactionManager(properties, objectMapper);
    }
}
