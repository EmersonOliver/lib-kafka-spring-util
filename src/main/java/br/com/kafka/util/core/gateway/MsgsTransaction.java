package br.com.kafka.util.core.gateway;

import br.com.kafka.util.core.domain.MessageWrapper;

import java.util.List;

public interface MsgsTransaction {

    void commit();
    void abort();
    boolean sendMsg(List<MessageWrapper> msg) throws Exception;
}
