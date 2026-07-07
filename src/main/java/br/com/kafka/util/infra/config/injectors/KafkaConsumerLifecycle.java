package br.com.kafka.util.infra.config.injectors;

import br.com.kafka.util.infra.config.factory.SpringKafkaConsumerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerLifecycle implements SmartLifecycle {

    private final SpringKafkaConsumerFactory consumerFactory;

    private boolean running;

    @Override
    public void start() {
        consumerFactory.getActiveConsumers()
                .values()
                .forEach(container -> {

                    container.start();

                    log.info(
                            "Consumer iniciado: {}",
                            container.getListenerId()
                    );
                });

        running = true;
    }

    @Override
    public void stop() {
        consumerFactory.getActiveConsumers()
                .values()
                .forEach(ConcurrentMessageListenerContainer::stop);

        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
