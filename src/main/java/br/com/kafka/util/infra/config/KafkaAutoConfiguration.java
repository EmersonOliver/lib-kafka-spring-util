package br.com.kafka.util.infra.config;

import br.com.kafka.util.infra.config.condition.KafkaConsumerCondition;
import br.com.kafka.util.infra.config.condition.KafkaProducerCondition;
import br.com.kafka.util.infra.config.factory.SpringKafkaConsumerFactory;
import br.com.kafka.util.infra.config.factory.SpringKafkaProducerFactory;
import br.com.kafka.util.infra.config.injectors.ConsumerKeyInjectionProcessor;
import br.com.kafka.util.infra.config.injectors.KafkaConsumerLifecycle;
import br.com.kafka.util.infra.config.injectors.ProducerKeyInjectionProcessor;
//import br.com.kafka.util.infra.config.injectors.TopicKeyAnnotationProcessor;
import br.com.kafka.util.infra.config.properties.KafkaConfigProperties;
import br.com.kafka.util.infra.config.transaction.LibKafkaSpringErrorRecoverer;
import br.com.kafka.util.infra.config.transaction.LibKafkaTransactionManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
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

    @Bean
    @Conditional(KafkaConsumerCondition.class)
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServer());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }

//    @Bean
//    @Conditional(KafkaConsumerCondition.class)
//    public static TopicKeyAnnotationProcessor topicKeyAnnotationProcessor(
//            KafkaConfigProperties properties,
//            ObjectMapper objectMapper) {
//        return new TopicKeyAnnotationProcessor(properties, objectMapper);
//    }

    @Bean
    @Conditional(KafkaConsumerCondition.class)
    public SpringKafkaConsumerFactory springKafkaConsumerFactory(
            DefaultErrorHandler errorHandler,
            ObjectMapper objectMapper) {
        return new SpringKafkaConsumerFactory(properties, errorHandler, objectMapper);
    }

    @Bean
    @Conditional(KafkaConsumerCondition.class)
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
    @Conditional(KafkaConsumerCondition.class)
    public static ConsumerKeyInjectionProcessor topicKeyBeanPostProcessor(
            ObjectProvider<SpringKafkaConsumerFactory> springKafkaConsumerFactoryProvider) {
        return new ConsumerKeyInjectionProcessor(springKafkaConsumerFactoryProvider);
    }


    @Bean
    @Conditional(KafkaConsumerCondition.class)
    public DefaultErrorHandler errorHandler(KafkaConfigProperties properties, ObjectMapper objectMapper) {
        FixedBackOff backOff = new FixedBackOff(2000L, 5L);
        LibKafkaSpringErrorRecoverer jaiminhoRecoverer = new LibKafkaSpringErrorRecoverer(properties, objectMapper);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(jaiminhoRecoverer, backOff);
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }

    @Bean
    @Conditional(KafkaProducerCondition.class)
    public SpringKafkaProducerFactory springKafkaProducerFactory(KafkaConfigProperties properties) {
        return new SpringKafkaProducerFactory(properties);
    }

    @Bean
    @Conditional(KafkaConsumerCondition.class)
    public KafkaConsumerLifecycle kafkaConsumerLifecycle(
            SpringKafkaConsumerFactory consumerFactory) {

        return new KafkaConsumerLifecycle(consumerFactory);
    }

    @Bean
    @Conditional(KafkaProducerCondition.class)
    public static ProducerKeyInjectionProcessor producerKeyInjectionProcessor(
            LibKafkaTransactionManager transactionManager, KafkaConfigProperties properties) {
        return new ProducerKeyInjectionProcessor(transactionManager, properties);
    }

    @Bean
    @Conditional(KafkaProducerCondition.class)
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    @Conditional(KafkaProducerCondition.class)
    public DeadLetterPublishingRecoverer publisher(KafkaTemplate<String, Object> bytesTemplate) {
        return new DeadLetterPublishingRecoverer(bytesTemplate,
                (record, exception) -> new org.apache.kafka.common.TopicPartition(record.topic() + ".DLQ", record.partition()));
    }

    @Bean
    @Conditional(KafkaProducerCondition.class)
    public LibKafkaTransactionManager libKafkaTransactionManager(KafkaConfigProperties properties, ObjectMapper objectMapper) {
        return new LibKafkaTransactionManager(properties, objectMapper);
    }
}
