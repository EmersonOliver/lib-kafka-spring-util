# lib-kafka-spring-util

Biblioteca utilitária para integrações com Kafka em aplicações Spring Boot, com foco em produtividade, configuração automática e processamento de mensagens com suporte a transações, retry e dead-letter queue.

## Visão geral

A `lib-kafka-spring-util` encapsula parte da complexidade da integração com Kafka e oferece uma camada de abstração para:

- registro automático de consumidores via anotações;
- injeção de produtores por chave;
- serialização de payloads em JSON;
- processamento em lote com confirmação manual;
- tratamento de erro com retry e DLQ;
- suporte a transações de producer;
- uso de virtual threads em listeners quando necessário.

A biblioteca foi projetada para ser consumida por aplicações Spring Boot que precisam reduzir boilerplate na comunicação com tópicos Kafka.

## Características principais

- Auto-configuration para Kafka em Spring Boot;
- `@TopicKey` para registrar consumidores de forma declarativa;
- `@ProducerKey` para injetar produtores por nome;
- `MessageProcessor` para processamento padronizado de mensagens;
- `MessageWrapper` para transportar metadados da mensagem;
- `SendToKafka` como contrato para envio de mensagens;
- suporte a transação e rollback em envios;
- integração com `DefaultErrorHandler` e `DeadLetterPublishingRecoverer`;
- configuração centralizada via `kafka.*` properties.

## Requisitos

- Java 21
- Spring Boot 4.x
- Spring Kafka
- Kafka broker acessível
- Maven ou Gradle

## Instalação

Adicionar a dependência no `pom.xml`:

```xml
<dependency>
    <groupId>br.com.kafka.util</groupId>
    <artifactId>lib-kafka-spring-util</artifactId>
    <version>1.0.0</version>
</dependency>
```

Se o projeto estiver sendo publicado no GitHub Packages, configure o repositório Maven antes de importar:

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/EmersonOliver/lib-kafka-spring-util</url>
    </repository>
</repositories>
```

## Configuração

Exemplo de configuração no `application.yml`:

```yaml
kafka:
  bootstrap-server: localhost:9092
  application-name: my-kafka-app

  consumer:
    customer-events:
      topic: customer-events
      group-id: customer-service-group
      offset-reset-config: earliest
      max-request-size: 1048576

  producer:
    customer-producer:
      topic: customer-events
      max-request-size: 1048576
      jass-config: ""
```

Estrutura da propriedade observada pelo projeto:

- `kafka.bootstrap-server`: broker Kafka;
- `kafka.consumer.<key>.topic`: tópico do consumidor;
- `kafka.consumer.<key>.group-id`: identificador do grupo de consumo;
- `kafka.consumer.<key>.offset-reset-config`: política de offset;
- `kafka.producer.<key>.topic`: tópico destino do producer;
- `kafka.producer.<key>.jass-config`: configuração SASL/SSL, quando necessário.

## Uso com consumidores

Para registrar um consumidor, implemente a interface `MessageProcessor` e marque a classe com `@TopicKey`.

```java
package com.example.consumer;

import br.com.kafka.util.core.domain.MessageProcessor;
import br.com.kafka.util.core.domain.MessageWrapper;
import br.com.kafka.util.core.gateway.KafkaControl;
import br.com.kafka.util.infra.annotations.TopicKey;
import org.springframework.stereotype.Component;

@Component
@TopicKey(value = "customer-events")
public class CustomerEventConsumer implements MessageProcessor {

    @Override
    public void process(MessageWrapper msg, KafkaControl control) {
        CustomerEvent event = (CustomerEvent) msg.getPayload();

        System.out.println("Recebido evento: " + event);

        // confirmação manual do offset
        control.commit();
    }
}
```

A biblioteca usa o tipo informado pela implementação da interface `MessageProcessor` para converter a mensagem em um payload específico durante o consumo.

### Processamento em paralelo

Você pode habilitar processamento paralelo com `parallelProcessing = true`:

```java
@Component
@TopicKey(value = "customer-events", parallelProcessing = true)
public class CustomerEventConsumer implements MessageProcessor {
    @Override
    public void process(MessageWrapper msg, KafkaControl control) {
        // processamento assíncrono via virtual thread
    }
}
```

## Uso com produtores

Para injetar um producer, use `@ProducerKey` em um campo do tipo `SendToKafka`.

```java
package com.example.service;

import br.com.kafka.util.core.domain.MessageWrapper;
import br.com.kafka.util.infra.annotations.ProducerKey;
import br.com.kafka.util.interfaces.send.SendToKafka;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CustomerProducerService {

    @ProducerKey("customer-producer")
    private SendToKafka sendToKafka;

    public void publishCustomerCreated(CustomerEvent event) {
        MessageWrapper message = new MessageWrapper();
        message.setTopicName("customer-events");
        message.setPayload(event);
        message.setKey(event.getCustomerId());

        sendToKafka.sendMessageToTopic(List.of(message));
    }
}
```

A interface `SendToKafka` expõe a operação principal:

```java
boolean sendMessageToTopic(List<MessageWrapper> msgs);
```

## Transações e controle de erro

A biblioteca implementa suporte a transação no envio e oferece mecanismos de recuperação para falhas de processamento.

### Fluxo de envio transacional

```java
SpringSend producer = new SpringSend(baseConfigs, "customer-events", objectMapper);

List<MessageWrapper> messages = List.of(message);

producer.sendMsg(messages);
producer.commit();
```

Em caso de falha, o fluxo pode ser abortado e a transação pode ser encerrada explicitamente.

### Tratamento de erro e DLQ

A auto-configuração já inclui:

- `DefaultErrorHandler`;
- `FixedBackOff` com retry;
- `DeadLetterPublishingRecoverer`;
- publicação em tópico `.DLQ` automaticamente.

Isso facilita cenários em que mensagens inválidas ou falhas de negócio precisam ser reprocessadas ou armazenadas para análise posterior.

## Estruturas principais

### `MessageWrapper`

Representa a mensagem Kafka com metadados útiles:

- `topicName`
- `createTime`
- `header`
- `payload`
- `offset`
- `partition`
- `key`

### `KafkaControl`

Fornece controle ao consumidor para confirmar ou reverter o processamento atual:

```java
public interface KafkaControl {
    void commit();
    void rollback(Exception ex);
}
```

## Observações de arquitetura

A biblioteca foi pensada para ser uma base reutilizável em microsserviços com Kafka, reduzindo a quantidade de código repetitivo e padronizando:

- configuração de clientes;
- serialização de mensagens;
- processamento de eventos em batch;
- gerenciamento de transações;
- automação de recuperação de falhas.

## Como executar localmente

```bash
./mvnw clean install
```

Ou:

```bash
mvn clean install
```

## Boas práticas recomendadas

- manter um `key` único por mensagem importante;
- usar tópicos específicos por domínio;
- centralizar as configurações de Kafka em `application.yml`;
- configurar DLQ para mensagens que não puderem ser processadas;
- validar payloads antes de publicar e consumir;
- preferir `@TopicKey` para registrar consumidores do domínio em vez de criar listeners manuais repetitivos.

## Contribuição

Contribuições são bem-vindas. Para contribuir:

1. faça um fork do projeto;
2. crie uma branch para a funcionalidade ou correção;
3. implemente a mudança;
4. abra um pull request com descrição clara.

## Status do projeto

Este repositório é uma biblioteca utilitária para integração com Kafka em aplicações Java/Spring Boot. O projeto pode ser estendido conforme a necessidade do ecossistema da aplicação.
