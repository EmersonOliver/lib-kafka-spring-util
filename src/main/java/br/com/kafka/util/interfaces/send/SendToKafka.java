package br.com.kafka.util.interfaces.send;

import br.com.kafka.util.core.domain.MessageWrapper;

import java.util.List;

public interface SendToKafka {

    boolean sendMessageToTopic(List<MessageWrapper> msgs);

}
