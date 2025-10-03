package com.tamalesp.ingestionservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tamalesp.ingestionservice.model.LogEntry;
import com.tamalesp.ingestionservice.repository.LogRepository;
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

    private final InsertionManager insertionManager;
    private final ReactiveKafkaProducerTemplate<String, LogEntry> kafkaProducerTemplate;
    //private final SchemaSwitchingService schemaSwitchingService;
    //private final LogRepository logRepository;

    public IngestionService(
            ReactiveKafkaProducerTemplate<String, LogEntry> kafkaProducerTemplate,
            SchemaSwitchingService schemaSwitchingService,
            LogRepository logRepository,
            InsertionManager insertionManager) {
        this.kafkaProducerTemplate = kafkaProducerTemplate;
        //this.schemaSwitchingService = schemaSwitchingService;
        this.insertionManager = insertionManager;
        //this.logRepository = logRepository;
    }

    public Mono<Void> processAndPublish(List<LogEntry> logEntries, String tenantId) {
        log.info("Processing {} logs for tenant {}", logEntries.size(), tenantId);

        // 1. --- DATABASE SAVE (Ensures logs are persisted before publishing) ---
       // Mono<Void> saveToDbMono = logRepository.saveAllInTenantSchema(Flux.fromIterable(logEntries));
        Mono<Void> saveToDbMono = insertionManager.insertLog(tenantId, logEntries).then();



        String topicName = "logs." + tenantId;
        String correlationId = UUID.randomUUID().toString();

        // Define the publishing flow
        Mono<Void> publishToKafkaMono = Flux.fromIterable(logEntries)
                .flatMap(logEntry -> {
                    // Create the kafka Message with headers
                    Message<LogEntry> kafkaMessage = MessageBuilder
                            .withPayload(logEntry)
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
                })
                .then(); // Ensure the whole Flux completes before returning

        // CHAIN THE OPERATIOSN
        // Save to DB first, then publish to kafka
        return saveToDbMono
                .doOnSuccess(v -> log.info("Successfully persisted {} logs to DB for tenant {}", logEntries.size(), tenantId))
                .onErrorResume(e -> {
                    log.error("FATAL: Failed to persist logs to DB for tenant {}. Aborting Kafka send.", tenantId, e);
                    return Mono.error(e);
                })
                .then(publishToKafkaMono);

    }
}
