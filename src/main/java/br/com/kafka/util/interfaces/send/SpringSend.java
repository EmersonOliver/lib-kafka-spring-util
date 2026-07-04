package br.com.kafka.util.interfaces.send;

import br.com.kafka.util.core.domain.MessageWrapper;
import br.com.kafka.util.core.gateway.MsgsTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.List;
import java.util.Map;

@Slf4j
public class SpringSend extends SpringSendKafkaBase implements MsgsTransaction {

    private boolean userEnd = false;

    public SpringSend(Map<String, Object> baseConfigs, String topic, ObjectMapper objectMapper) {
        super(baseConfigs, topic, objectMapper);
    }
    @Override
    public void commit() {
        this.commitInternal();
        this.closeProducer(); // Garante o fechamento após o fim do ciclo
        this.userEnd = true;
    }

    @Override
    public void abort() {
        this.abortInternal();
        this.closeProducer();
        this.userEnd = true;
    }

    @Override
    public boolean sendMsg(List<MessageWrapper> msgs) throws Exception {
        log.info("Iniciando transacao MANUAL Jaiminho-Spring {} para o topico {}", transactionID, topic);
        int retryCount = 0;
        int maxRetries = 5;
        long backoffTime = 1000;
        this.userEnd = false;

        while ((retryCount < maxRetries) && !userEnd) {
            try {
                if (producer == null) {
                    initializerProducer();
                }
                this.beginTransaction();
                this.sendMsgs(msgs);
                log.warn("Mensagem enviada com sucesso no Spring, Transacao NAO!!! comitada {}, {}", retryCount, transactionID);
                return true;
            } catch (ProducerFencedException | InvalidProducerEpochException e) {
                log.error("Producer fenced, cannot recover: {} on attempt {}, transactionID={}", topic, retryCount, transactionID);
                this.closeProducer();
            } catch (KafkaException e) {
                log.error("Kafka exception during send: {} on attempt {}. transactionID= {}", topic, retryCount, transactionID, e);
                this.abortInternal();
                this.closeProducer();
            } catch (Exception e) {
                log.error("Erro ao enviar mensagem para o topico {} on attempt {}. transactionId= {}", topic, retryCount, transactionID, e);
                this.abortInternal();
                this.closeProducer();
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
        throw new Exception("Erro ao enviar mensagem para o topico. Rollback sera realizado pela aplicacao e nao pela lib: " + topic + " " + transactionID);
    }
}
