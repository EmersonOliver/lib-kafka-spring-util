package br.com.kafka.util.core.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Map;

@Data
@NoArgsConstructor
public class MessageWrapper {
    private String topicName;
    private Timestamp createTime;
    private Map<String, String> header;
    private Object payload;
    private long offset;
    private int partition;
    private String key;
}
