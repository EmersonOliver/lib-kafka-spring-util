package br.com.kafka.util.core.domain;

import br.com.kafka.util.core.gateway.KafkaControl;

public interface MessageProcessor {
    void process(MessageWrapper msg, KafkaControl control);

}
