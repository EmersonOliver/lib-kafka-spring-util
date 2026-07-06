package br.com.kafka.util.infra.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;
import java.util.Optional;

@ConfigurationProperties(prefix = "kafka")
public record KafkaConfigProperties(
        String bootstrapServer,
        Map<String, ConsumerConfig> consumer,
        Map<String, ProducerConfig> producer,
        Map<String, AutomaticResender> automaticResender,
        Map<String, TracerConfig> tracerConfig,
        String errorProducerTopic,
        String applicationName,
        BatchProperties batch
) {

    public record BatchProperties(
            Map<String, ConsumerConfig> consumer,
            Map<String, ProducerConfig> producer,
            String errorProducerTopic) {
    }

    public record ConsumerConfig(
            String topic,
            String groupId,
            Optional<String> jassConfig,
            Optional<String> offsetResetConfig,
            Optional<String> maxRequestSize
    ) {
    }

    public record ProducerConfig(
            String topic,
            Optional<String> jassConfig,
            Optional<String> maxRequestSize
    ) {
    }

    public record AutomaticResender(
            String sourceTopic,
            String groupId,
            String targetTopic,
            String cronExpression,
            String maxRetry,
            String offsetResetConfig
    ) {
    }

    public record TracerConfig(
            String topic,
            String tracerEnable,
            String tracerExporter,
            String tracerSamplerRatio,
            String tracerEndpoint
    ) {
    }
}
