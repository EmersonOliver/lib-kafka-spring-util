package br.com.kafka.util.infra.config.controls;

import br.com.kafka.util.core.domain.MessageWrapper;
import br.com.kafka.util.core.gateway.KafkaControl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
@RequiredArgsConstructor
public class LibKafkaSpringControl implements KafkaControl {

    private final Acknowledgment acknowledgment;
    private final MessageWrapper message;
    private final MessageListenerContainer container;
    private boolean processed = false;

    @Override
    public void commit() {
        if (!processed) {
            try {
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
                this.processed = true;
                log.info("Mensagem commitada com sucesso no offset: {}", message.getOffset());
            } catch (Exception e) {
                log.error("Erro ao executar commit na lib Spring", e);
                resetConsumer();
            }
        }
    }

    @Override
    public void rollback(Exception ex) {
        if (!processed) {
            log.warn("Rollback acionado pela aplicação: {}. Forçando reprocessamento do offset: {}", ex.getMessage(), message.getOffset());
            this.processed = true;
            resetConsumer();
        }
    }

    private void resetConsumer() {
        try {
            container.stop();
            container.start();
        } catch (Exception e) {
            log.error("Falha ao reiniciar o container de consumo do Spring", e);
        }
    }
}
