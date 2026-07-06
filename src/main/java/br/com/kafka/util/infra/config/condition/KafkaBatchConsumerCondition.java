package br.com.kafka.util.infra.config.condition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Map;

public class KafkaBatchConsumerCondition implements Condition {

    @Override
    public boolean matches(
            ConditionContext context,
            AnnotatedTypeMetadata metadata) {

        Map<String, Object> consumers =
                Binder.get(context.getEnvironment())
                        .bind(
                                "kafka.batch.consumer",
                                Bindable.mapOf(String.class, Object.class)
                        )
                        .orElse(Map.of());

        return !consumers.isEmpty();
    }
}
