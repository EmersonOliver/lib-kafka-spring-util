package br.com.kafka.util.core.gateway;

public interface KafkaControl {
    void commit();
    void rollback(Exception ex);
}
