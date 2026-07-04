package br.com.kafka.util.infra.annotations;


import java.lang.annotation.*;

@Target({ElementType.FIELD, ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TopicKey {
    String value();
    Class<?> payload();
    boolean threadSafe() default false;
}
