package br.com.kafka.util.core.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ErrorMessage {

    private Object mensagem;
    private String service;
    private String stack;
}
