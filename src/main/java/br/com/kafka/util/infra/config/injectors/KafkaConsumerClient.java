package br.com.kafka.util.infra.config.injectors;

import br.com.kafka.util.core.domain.MessageWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerClient {

    private final ExecutorService vtExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ObjectMapper objectMapper;

    public void consumeBatchAsStream(List<ConsumerRecord<String, String>> records,
                                     Acknowledgment ack, MessageStreamProcessor processor) {
        try {
            Stream<MessageWrapper> messageStream = records.stream().map(record -> {
                try {
                    MessageWrapper wrapper = new MessageWrapper();
                    wrapper.setPayload(record.value());
                    wrapper.setOffset(record.offset());
                    wrapper.setKey(record.key());
                    wrapper.setTopicName(record.topic());
                    return wrapper;
                } catch (Exception e) {
                    log.error("Erro ao fazer o parse da mensagem no Stream", e);
                    return null;
                }
            });

            messageStream.forEach(msg -> {
                if (msg != null) {
                    vtExecutor.submit(() -> {
                        try {
                            processor.process(msg);
                        } catch (Exception e) {
                            log.error("Erro ao processar mensagem na Virtual Thread", e);
                            throw new RuntimeException(e);
                        }
                    });
                }
            });

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Falha catastrófica no processamento do lote via Stream", e);
            throw e;
        }
    }

    @FunctionalInterface
    public interface MessageStreamProcessor {
        void process(MessageWrapper message) throws Exception;
    }
}
