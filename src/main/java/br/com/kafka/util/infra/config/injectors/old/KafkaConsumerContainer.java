//package br.com.kafka.util.infra.config.injectors;
//
//import br.com.kafka.util.core.domain.MessageWrapper;
//import br.com.kafka.util.infra.annotations.TopicKey;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//
//import java.lang.reflect.Method;
//import java.time.Duration;
//import java.util.Collections;
//import java.util.Properties;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//@Slf4j
//public class KafkaConsumerContainer implements Runnable {
//
//    private final KafkaConsumer<String, String> consumer;
//    private final Object beanTarget;
//    private final Method methodTarget;
//    private final TopicKey annotation;
//    private final ObjectMapper objectMapper;
//    private final ExecutorService vtExecutor;
//    private volatile boolean running = true;
//
//    public KafkaConsumerContainer(Properties properties, Object beanTarget, Method methodTarget, TopicKey annotation, ObjectMapper objectMapper) {
//        this.consumer = new KafkaConsumer<>(properties);
//        this.beanTarget = beanTarget;
//        this.methodTarget = methodTarget;
//        this.annotation = annotation;
//        this.objectMapper = objectMapper;
//        this.vtExecutor = annotation.parallelProcessing() ? Executors.newVirtualThreadPerTaskExecutor() : null;
//    }
//
//    @Override
//    public void run() {
//        try {
//            consumer.subscribe(Collections.singletonList(annotation.value()));
//            log.info("Jaiminho Consumer iniciado para o tópico: {} (parallelProcessing={})", annotation.value(), annotation.parallelProcessing());
//
//            while (running) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//
//                if (records.isEmpty()) continue;
//
//                if (annotation.parallelProcessing()) {
//                    for (ConsumerRecord<String, String> record : records) {
//                        vtExecutor.submit(() -> invokeProcessor(record));
//                    }
//                    consumer.commitSync(); // Commit do lote após despachar para as VTs
//                } else {
//                    for (ConsumerRecord<String, String> record : records) {
//                        invokeProcessor(record);
//                    }
//                    consumer.commitSync();
//                }
//            }
//        } catch (Exception e) {
//            log.error("Erro crítico no loop do consumer do tópico {}", annotation.value(), e);
//        } finally {
//            try {
//                consumer.close();
//            } catch (Exception ignored) {
//            }
//            if (vtExecutor != null) vtExecutor.shutdown();
//        }
//    }
//
//    private void invokeProcessor(ConsumerRecord<String, String> record) {
//        try {
//            Object payload = objectMapper.readValue(record.value(), annotation.payload());
//
//            MessageWrapper wrapper = new MessageWrapper();
//            wrapper.setPayload(payload);
//
//            methodTarget.invoke(beanTarget, wrapper);
//
//        } catch (Exception e) {
//            log.error("Erro ao processar mensagem do tópico {}", annotation.value(), e);
//        }
//    }
//
//    public void stop() {
//        this.running = false;
//    }
//}
