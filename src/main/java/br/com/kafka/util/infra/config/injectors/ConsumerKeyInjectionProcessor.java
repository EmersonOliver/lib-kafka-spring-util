package br.com.kafka.util.infra.config.injectors;

import br.com.kafka.util.core.domain.MessageProcessor;
import br.com.kafka.util.infra.annotations.TopicKey;
import br.com.kafka.util.infra.config.factory.SpringKafkaConsumerFactory;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@RequiredArgsConstructor
public class ConsumerKeyInjectionProcessor implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(ConsumerKeyInjectionProcessor.class);
    private final ObjectProvider<SpringKafkaConsumerFactory> springKafkaConsumerFactoryProvider;

    @Override
    public @Nullable Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);

        // 1. Verifica se a CLASSE possui a anotação @TopicKey
        if (targetClass.isAnnotationPresent(TopicKey.class)) {
            TopicKey annotation = targetClass.getAnnotation(TopicKey.class);
            String consumerKey = annotation.value();

            // 2. Validação: Como a anotação está na classe, o Bean PRECISA implementar MessageProcessor
            if (!(bean instanceof MessageProcessor)) {
                throw new IllegalStateException("A classe " + targetClass.getName() +
                        " está anotada com @TopicKey, mas não implementa " + MessageProcessor.class.getName());
            }

            // 3. Descobre dinamicamente o tipo do Payload (O Generics da interface MessageProcessor<T>)
            Class<?> targetPayloadType = discoverPayloadType(targetClass);

            log.info("Descoberto Consumidor Jaiminho na classe: {} para a chave: {} com payload: {}",
                    targetClass.getSimpleName(), consumerKey, targetPayloadType.getSimpleName());

            // 4. Recupera a fábrica de forma segura através do ObjectProvider
            SpringKafkaConsumerFactory springKafkaConsumerFactory = springKafkaConsumerFactoryProvider.getIfAvailable();
            if (springKafkaConsumerFactory == null) {
                throw new IllegalStateException("SpringKafkaConsumerFactory não encontrada no contexto do Spring.");
            }

            // 5. Inicia o loop de consumo em background vinculando o próprio bean
            springKafkaConsumerFactory.createAndStartConsumer(consumerKey, (MessageProcessor) bean, targetPayloadType);
        }

        return bean;
    }

    /**
     * Utilitário para descobrir o tipo do DTO (Payload) que a classe espera processar,
     * inspecionando a interface parametrizada MessageProcessor<SeuDTO>.
     */
    private Class<?> discoverPayloadType(Class<?> clazz) {
        for (Type iface : clazz.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) iface;
                if (pt.getRawType().equals(MessageProcessor.class)) {
                    return (Class<?>) pt.getActualTypeArguments()[0];
                }
            }
        }
        // Fallback genérico caso a sua interface MessageProcessor não utilize Generics compilado
        return Object.class;
    }
}
