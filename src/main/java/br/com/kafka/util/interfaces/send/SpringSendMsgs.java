package br.com.kafka.util.interfaces.send;

import br.com.kafka.util.core.domain.MessageWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.List;
import java.util.Map;

@Slf4j
public class SpringSendMsgs extends SpringSendKafkaBase implements SendToKafka {

    public SpringSendMsgs(Map<String, Object> baseConfigs, String topic, ObjectMapper objectMapper) {
        super(baseConfigs, topic, objectMapper);
    }

    @Override
    public boolean sendMessageToTopic(List<MessageWrapper> msgs) {
        int retryCount = 1;
        int maxRetries = 6;
        long backoffTime = 1000;

        while (retryCount < maxRetries) {
            try {
                if (producer == null) {
                    initializerProducer();
                }
                log.info("Iniciando transacao Jaiminho-Spring {} para o topico {}", transactionID, topic);
                this.beginTransaction();
                this.sendMsgs(msgs);
                this.commitInternal();

                log.info("Mensagem enviada com sucesso, on attempt {} fim da transactionID= {}", retryCount, transactionID);
                return true;
            } catch (ProducerFencedException | InvalidProducerEpochException e) {
                log.error("Producer fenced, cannot recover: {} on attempt {}, transactionID={}", topic, retryCount, transactionID, e);
                destroyCurrentProducer();
            } catch (KafkaException e) {
                log.error("Kafka exception during send: {} on attempt {}. transactionID= {}", topic, retryCount, transactionID, e);
                destroyCurrentProducer();
            } catch (Exception e) {
                log.error("Erro ao enviar mensagem para o topico {} on attempt {}. transactionId= {}", topic, retryCount, transactionID, e);
                destroyCurrentProducer();
            }

            retryCount++;
            try {
                this.abortInternal();
                this.closeProducer();
                Thread.sleep(backoffTime * retryCount);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }

    private void destroyCurrentProducer() {
        try {
            if (this.producer != null) {
                this.producer.close();
            }
        } catch (Exception ignored) {
            // Ignora falhas ao fechar um producer já quebrado
        } finally {
            this.producer = null;
        }
    }
}
