package com.tamalesp.ingestionservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tamalesp.ingestionservice.model.LogEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class IngestionService {


    private final ReactiveKafkaProducerTemplate<String, LogEntry> kafkaProducerTemplate;

    public IngestionService(ReactiveKafkaProducerTemplate<String, LogEntry> kafkaProducerTemplate) {
        this.kafkaProducerTemplate = kafkaProducerTemplate;
    }

    public Mono<Void> processAndPublish(List<LogEntry> logEntries, String tenantId) {
        log.info("Processing {} logs for tenant {}", logEntries.size(), tenantId);
        String topicName = "logs." + tenantId;
        String correlationId = UUID.randomUUID().toString();

        return Flux.fromIterable(logEntries)
                .flatMap( logEntry -> {
                    try {
                        // Serialize logEntry to a json String
                        String logJson = new ObjectMapper().writeValueAsString(logEntry);

                        // Create the kafka Message with headers
                        Message<String> kafkaMessage = MessageBuilder
                                .withPayload(logJson)
                                .setHeader(KafkaHeaders.KEY, tenantId)
                                .setHeader(KafkaHeaders.TOPIC, topicName)
                                .setHeader("correlationId", correlationId)
                                .build();

                        // Sending message and handling the result
                        return kafkaProducerTemplate.send(topicName, kafkaMessage)
                                .doOnSuccess(senderResult ->
                                        log.debug("Published log to topic {} on partition {} with offset {}",
                                                senderResult.recordMetadata().topic(),
                                                senderResult.recordMetadata().partition(),
                                                senderResult.recordMetadata().offset()
                                                )
                                        )
                                .doOnError(e -> log.error("Failed to publish log for tenant {}", tenantId, e));
                     }catch (JsonProcessingException e) {
                        log.error("Failed to serialize log entry for tenant {}", tenantId, e);
                        return Mono.error(e);
                    }
                })
                .then(); //Wait
    }
}
