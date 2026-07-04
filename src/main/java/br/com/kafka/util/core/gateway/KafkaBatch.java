package br.com.kafka.util.core.gateway;

import br.com.kafka.util.core.domain.MessageWrapper;

import java.util.stream.Stream;

public interface KafkaBatch {

    void commit();
    void aborteReader(Exception ex);
    Stream<MessageWrapper> fetchAllMsgsFromTopic();
}
