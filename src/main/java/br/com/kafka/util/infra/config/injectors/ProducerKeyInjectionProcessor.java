package br.com.kafka.util.infra.config.injectors;

import br.com.kafka.util.infra.annotations.ProducerKey;
import br.com.kafka.util.infra.config.properties.KafkaConfigProperties;
import br.com.kafka.util.infra.config.transaction.LibKafkaTransactionManager;
import br.com.kafka.util.interfaces.send.SendToKafka;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

@RequiredArgsConstructor
public class ProducerKeyInjectionProcessor implements BeanPostProcessor {

    private final LibKafkaTransactionManager libKafkaTransactionManager;
    private final KafkaConfigProperties properties;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);

        ReflectionUtils.doWithFields(targetClass, field -> {
            if (field.isAnnotationPresent(ProducerKey.class)) {

                if (!SendToKafka.class.isAssignableFrom(field.getType())) {
                    throw new IllegalStateException("A anotação @ProducerKey só pode ser usada em campos do tipo " + SendToKafka.class.getName());
                }

                ProducerKey annotation = field.getAnnotation(ProducerKey.class);
                String producerKey = annotation.value();
                if (properties.producer() == null
                        || !properties.producer().containsKey(producerKey)) {

                    throw new BeanCreationException(
                            "Producer '%s' não configurado"
                                    .formatted(producerKey)
                    );
                }
                if (!libKafkaTransactionManager.hasProducer(producerKey)) {

                    throw new BeanCreationException(
                            """
                            Producer '%s' encontrado na anotação @ProducerKey,
                            porém não existe configuração:
                
                            kafka:
                              producer:
                                %s:
                                  topic: ...
                            """.formatted(producerKey, producerKey)
                    );
                }

                SendToKafka simpleProducer = libKafkaTransactionManager.getSimpleProducer(producerKey);
                injectField(bean, field, simpleProducer);

                if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
                    try {
                        Object targetObject = ((Advised) bean).getTargetSource().getTarget();
                        if (targetObject != null) {
                            Field targetField = ReflectionUtils.findField(targetObject.getClass(), field.getName());
                            if (targetField != null) {
                                injectField(targetObject, targetField, simpleProducer);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Erro ao injetar @ProducerKey no objeto alvo do proxy do Spring", e);
                    }
                }
            }
        });

        return bean;
    }

    private void injectField(Object target, Field field, Object value) {
        try {
            ReflectionUtils.makeAccessible(field);
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Falha ao injetar campo via reflexão", e);
        }
    }
}
