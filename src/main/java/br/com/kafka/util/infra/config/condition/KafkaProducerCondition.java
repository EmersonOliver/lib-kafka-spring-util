package br.com.kafka.util.infra.config.condition;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Map;

public class KafkaProducerCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        Map<String, Object> producers =
                Binder.get(context.getEnvironment())
                        .bind(
                                "kafka.producer",
                                Bindable.mapOf(String.class, Object.class)
                        )
                        .orElse(Map.of());

        return !producers.isEmpty();
    }
}
