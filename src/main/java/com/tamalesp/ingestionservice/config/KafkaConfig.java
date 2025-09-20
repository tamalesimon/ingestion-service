package com.tamalesp.ingestionservice.config;

import com.tamalesp.ingestionservice.model.LogEntry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public ReactiveKafkaProducerTemplate<String, LogEntry> kafkaProducerTemplate() {
        Map<String, Object> props = new HashMap<>();
        String bootstrapServers = "localhost:9092";
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomLogEntrySerializer.class); // custom serializer for LogEntry

        // Add more production-grade properties
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        SenderOptions<String, LogEntry> senderOptions = SenderOptions.create(props);
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }
}
