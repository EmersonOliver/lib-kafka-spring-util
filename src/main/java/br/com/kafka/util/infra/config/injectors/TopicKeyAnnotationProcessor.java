//package br.com.kafka.util.infra.config.injectors;
//
//import br.com.kafka.util.infra.annotations.TopicKey;
//import br.com.kafka.util.infra.config.properties.KafkaConfigProperties;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.jspecify.annotations.Nullable;
//import org.springframework.beans.BeansException;
//import org.springframework.beans.factory.config.BeanPostProcessor;
//import org.springframework.context.SmartLifecycle;
//import org.springframework.util.ReflectionUtils;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//@Slf4j
//@RequiredArgsConstructor
//public class TopicKeyAnnotationProcessor implements BeanPostProcessor, SmartLifecycle {
//
//    private final KafkaConfigProperties kafkaProperties;
//    private final ObjectMapper objectMapper;
//    private final List<KafkaConsumerContainer> activeContainers = new ArrayList<>();
//    private boolean isRunning = false;
//
//    @Override
//    public @Nullable Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
//        Class<?> targetClass = bean.getClass();
//
//        ReflectionUtils.doWithMethods(targetClass, method -> {
//            if (method.isAnnotationPresent(TopicKey.class)) {
//                TopicKey annotation = method.getAnnotation(TopicKey.class);
//
//                Properties props = new Properties();
//                props.put("bootstrap.servers", kafkaProperties.bootstrapServer());
//                props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//                props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//                props.put("enable.auto.commit", "false");
//                props.put("group.id", "jaiminho-group-" + annotation.value());
//
//                KafkaConsumerContainer container = new KafkaConsumerContainer(props, bean, method, annotation, objectMapper);
//                activeContainers.add(container);
//            }
//        });
//
//        return bean;
//    }
//
//    @Override
//    public void start() {
//        log.info("Iniciando Jaiminho Consumers via Virtual Threads...");
//        for (KafkaConsumerContainer container : activeContainers) {
//            Thread.startVirtualThread(container);
//        }
//        this.isRunning = true;
//    }
//
//    @Override
//    public void stop() {
//        log.info("Parando Jaiminho Consumers...");
//        activeContainers.forEach(KafkaConsumerContainer::stop);
//        this.isRunning = false;
//    }
//
//    @Override
//    public boolean isRunning() {
//        return this.isRunning;
//    }
//
//    @Override
//    public int getPhase() {
//        return Integer.MAX_VALUE;
//    }
//}
